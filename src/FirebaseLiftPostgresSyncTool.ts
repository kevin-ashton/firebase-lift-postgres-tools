import { Pool } from 'pg';
import { SyncTask, SyncTaskValidator } from './models';
import * as fbAdmin from 'firebase-admin';
import * as Queue from 'promise-queue';
import * as stable from 'json-stable-stringify';
import { fetchAndProcessFirestoreCollection, fetchAndProcessRtdbRecordPath } from './collectionProcessor';

type ErrorHanlder = (p: { message: string; error?: any; meta?: any }) => void;
type DebuggerFn = (p: { task: any }) => Promise<void>;

interface FullMirrorValidationResultError {
  status: 'failed';
  errorMsg: string;
  finalFullMirrorValidationRunResult: FullMirrorValidationRunResult;
}

interface FullMirrorValidationRunResult {
  runId: number;
  startMS: number;
  finishMS?: number;
  totalRunTimeSeconds?: number;
  status: 'finished' | 'running';
  initialRowCounts: {
    [pgTitle: string]: { [collectionsOrRecordPath: string]: number };
  };
  totalInitialRowsFromMirrorTables: number;
  totalRowsProcessed: number;
  totalDocsOrNodesProcessed: number;
  totalProcessingErrors: number;
  totalValidationErrors: number;
  validationResults: Record<ItemState, number>;
}

type ItemState =
  | 'ITEMS_WERE_IN_EXPECTED_STATE'
  | 'ITEMS_DID_NOT_MATCH'
  | 'ITEM_WAS_MISSING_IN_MIRROR'
  | 'ITEM_WAS_NOT_DELETED_IN_MIRROR';

export type PreMirrorTransformFn = (p: { item: any; collectionOrRecordPath: string }) => any;

export type PostMirrorHookFn = (p: {
  action: 'create/update' | 'delete';
  item: any;
  collectionOrRecordPath: string;
}) => Promise<void>;

interface ValidationResult {
  itemState: ItemState;
  idOrKey: string;
  collectionOrRecordPath: string;
  description?: string;
  poolTitle?: string;
  fbItem?: any;
  pgItem?: any;
}

type ValidationErrorLogger = (pp: ValidationResult) => void;
type ProcessingErrorLogger = (pp: { error: any }) => void;

type FullMirrorValidationResult = FullMirrorValidationResultError | FullMirrorValidationRunResult;

export type CollectionOrRecordPathMeta = { collectionOrRecordPath: string; source: 'rtdb' | 'firestore' };

export class FirebaseLiftPostgresSyncTool {
  private _externalErrorHandler: ErrorHanlder = null as any;
  private mirrorPgs: { title: string; pool: Pool }[] = [];
  private auditPgs: { title: string; pool: Pool }[] = [];

  private rtdb: fbAdmin.database.Database = null as any;
  private firestore: fbAdmin.firestore.Firestore = null as any;
  private collectionOrRecordPathsMeta: CollectionOrRecordPathMeta[] = [];

  private syncQueue: Queue = null as any;
  private syncValidatorQueue: Queue = null as any;

  private syncTaskDebuggerFn: null | DebuggerFn = null;
  private syncTaskRunningIdOrKeys: Record<string, number> = {}; // Used to track if the same item is already being processed
  private totalSyncTasksPendingRetry = 0;
  private totalSyncTasksSkipped = 0;

  private totalSyncTasksProcessed = 0;
  private totalSyncValidationTasksInUnpexectedState = 0;
  private totalErrors = 0;

  private totalSyncValidatorTasksProcessed = 0;
  private syncValidatorTaskDebuggerFn: null | DebuggerFn = null;
  private syncValidatorTaskRunningIdOrKeys: Record<string, number> = {}; // Used to track if the same item is already being processed
  private totalSyncValidatorTasksPendingRetry = 0;
  private totalSyncValidatorsTasksSkipped = 0;

  private preMirrorTransform: PreMirrorTransformFn = (i) => i.item; // Usesd to transform the item before sending it to the pgMirror
  private postMirrorHook: PostMirrorHookFn = async () => {};

  constructor(config: {
    mirrorsPgs: { title: string; pool: Pool }[];
    auditPgs: { title: string; pool: Pool }[];
    collectionOrRecordPathsMeta: CollectionOrRecordPathMeta[];
    rtdb: fbAdmin.database.Database;
    firestore: fbAdmin.firestore.Firestore;
    syncQueueConcurrency: number;
    syncValidatorQueueConcurrency: number;
    errorHandler: (p: { message: string; error?: any }) => void;
    preMirrorTransform?: PreMirrorTransformFn;
    postMirrorHook?: PostMirrorHookFn;
  }) {
    console.log('Init FirebaseLiftPostgresSyncTool');

    // Make sure we have valid collectionOrRecordPaths
    // Only supports realtime database nodes at the root for the time being as result
    for (let i = 0; i < config.collectionOrRecordPathsMeta.length; i++) {
      const t = config.collectionOrRecordPathsMeta[i].collectionOrRecordPath;
      if (t !== t.replace(/[^A-Za-z]/gim, ' ')) {
        throw new Error(`Invalid collectionOrRecordPath. Only letters are allowed`);
      }
    }

    this.mirrorPgs = config.mirrorsPgs;
    this.auditPgs = config.auditPgs;
    this.syncQueue = new Queue(config.syncQueueConcurrency, Infinity);
    this.syncValidatorQueue = new Queue(config.syncValidatorQueueConcurrency, Infinity);
    this.collectionOrRecordPathsMeta = config.collectionOrRecordPathsMeta;
    this.rtdb = config.rtdb;
    this.firestore = config.firestore;
    this._externalErrorHandler = config.errorHandler;

    if (config.preMirrorTransform) {
      this.preMirrorTransform = config.preMirrorTransform;
    }

    if (config.postMirrorHook) {
      this.postMirrorHook = config.postMirrorHook;
    }

    this.ensureMirrorTablesExists();
    this.ensureAuditTablesExists();
  }

  public _registerSyncTaskDebugFn(fn: DebuggerFn) {
    this.syncTaskDebuggerFn = fn;
  }

  public _registerSyncValidatorTaskDebugFn(fn: DebuggerFn) {
    this.syncValidatorTaskDebuggerFn = fn;
  }

  private errorHandler: ErrorHanlder = (p) => {
    this.totalErrors += 1;
    this._externalErrorHandler(p);
  };

  private async ensureMirrorTablesExists() {
    console.log('Running ensureMirrorTablesExists');
    if (this.mirrorPgs.length > 0) {
      for (let i = 0; i < this.mirrorPgs.length; i++) {
        const pg = this.mirrorPgs[i];
        const baseTableNames = this.collectionOrRecordPathsMeta.map((e) => e.collectionOrRecordPath);
        for (let k = 0; k < baseTableNames.length; k++) {
          const baseTableName = baseTableNames[k];
          try {
            await pg.pool.query(`select * from mirror_${baseTableName} limit 1`);
          } catch (e) {
            try {
              console.log(`Creating mirror table. BaseTableName: ${baseTableName}. DB Title: ${pg.title}`);
              pg.pool.query(`
              CREATE TABLE mirror_${baseTableName}
              (
                id text PRIMARY KEY,
                item jsonb not null,
                updated_at timestamp not null,
                last_sync_task_date_ms bigint,
                validation_number bigint
              );`);
            } catch (ee) {
              const msg = `Trouble ensuring an mirror table has been created.`;
              console.error(msg);
              this.errorHandler({
                message: msg,
                error: ee,
                meta: { baseTableName, dbTitle: pg.title, errorMsg: ee.message }
              });
              throw new Error(msg);
            }
          }
        }
      }
    }
  }

  private async ensureAuditTablesExists() {
    console.log('Running ensureAuditTablesExists');
    if (this.auditPgs.length > 0) {
      for (let i = 0; i < this.auditPgs.length; i++) {
        const pg = this.auditPgs[i];
        const baseTableNames = this.collectionOrRecordPathsMeta.map((e) => e.collectionOrRecordPath);
        for (let k = 0; k < baseTableNames.length; k++) {
          const baseTableName = baseTableNames[k];
          try {
            await pg.pool.query(`select * from audit_${baseTableName} limit 1`);
          } catch (e) {
            try {
              console.log(`Creating audit table. BaseTableName: ${baseTableName}. DB Title: ${pg.title}`);
              pg.pool.query(`
              CREATE TABLE audit_${baseTableName}
              (
                id serial PRIMARY KEY,
                itemId text,
                beforeItem jsonb,
                afterItem jsonb,
                action text,
                recorded_at timestamp not null
              );
              CREATE INDEX ON audit_${baseTableName} (itemId);
              `);
            } catch (ee) {
              const msg = `Trouble ensuring an audit table has been created.`;
              console.error(msg);
              this.errorHandler({
                message: msg,
                error: ee,
                meta: { baseTableName, dbTitle: pg.title, errorMsg: ee.message }
              });
              throw new Error(msg);
            }
          }
        }
      }
    }
  }

  getStats() {
    return {
      totalErrors: this.totalErrors,
      totalSyncTasksProcessed: this.totalSyncTasksProcessed,
      totalSyncValidationTasksInUnpexectedState: this.totalSyncValidationTasksInUnpexectedState,
      totalSyncTasksWaitingInQueue: this.syncQueue.getPendingLength(),
      totalSyncTasksCurrentlyRunning: Object.keys(this.syncTaskRunningIdOrKeys).length,
      totalSyncTasksPendingRetry: this.totalSyncTasksPendingRetry,
      totalSyncTasksSkipped: this.totalSyncTasksSkipped,

      totalSyncValidatorTasksProcessed: this.totalSyncValidatorTasksProcessed,
      totalSyncValidatorsTasksSkipped: this.totalSyncValidatorsTasksSkipped,
      totalSyncValidatorTasksCurrentlyRunning: Object.keys(this.syncValidatorTaskRunningIdOrKeys).length,
      totalSyncValidatorTasksPendingRetry: this.totalSyncValidatorTasksPendingRetry,

      totalMirrorPgs: this.mirrorPgs.length,
      totalAuditPgs: this.auditPgs.length
    };
  }

  queueSyncTasks(tasks: SyncTask[]) {
    tasks.forEach((task) => {
      if (
        !this.collectionOrRecordPathsMeta.map((e) => e.collectionOrRecordPath).includes(task.collectionOrRecordPath)
      ) {
        this.errorHandler({
          message: `Cannot sync item. The collectionOrRecordPath is not registered.`,
          meta: {
            collectionOrRecordPath: task.collectionOrRecordPath
          }
        });
        return;
      }
      this.syncQueue.add(this.handleSyncTasks(task));
    });
  }

  _waitUntilSyncQueueDrained() {
    return new Promise((resolve) => {
      const isDone = () => this.syncQueue.getPendingLength() + this.syncQueue.getQueueLength() === 0;
      if (isDone()) {
        resolve();
      } else {
        const internval = setInterval(() => {
          if (isDone()) {
            resolve();
            clearInterval(internval);
          }
        }, 300);
      }
    });
  }

  _waitUntilSyncValidatorQueueDrained() {
    return new Promise((resolve) => {
      const isDone = () => this.syncValidatorQueue.getPendingLength() + this.syncValidatorQueue.getQueueLength() === 0;
      if (isDone()) {
        resolve();
      } else {
        const interval = setInterval(() => {
          if (isDone()) {
            resolve();
            clearInterval(interval);
          }
        }, 300);
      }
    });
  }

  public validatePgMirrors(p: {}) {}

  private handleSyncTasks(task: SyncTask) {
    return async () => {
      // Check if another task is already being executed for this key
      if (this.syncTaskRunningIdOrKeys[task.idOrKey]) {
        if (this.syncTaskRunningIdOrKeys[task.idOrKey] > task.dateMS) {
          // if the pending task is older than the one being executed we discard it
          this.totalSyncTasksSkipped += 1;
          return;
        } else {
          // wait a second then queue this task again
          this.totalSyncTasksPendingRetry += 1;
          await new Promise((r) => setTimeout(() => r(), 1000));
          this.queueSyncTasks([task]);
          this.totalSyncTasksPendingRetry -= 1;
          return;
        }
      }

      this.syncTaskRunningIdOrKeys[task.idOrKey] = task.dateMS;

      // Order matters. This line should not be any higher in this fn
      if (this.syncTaskDebuggerFn) {
        await this.syncTaskDebuggerFn({ task });
      }

      try {
        const table = `mirror_${task.collectionOrRecordPath}`;
        const auditTable = `audit_${task.collectionOrRecordPath}`;
        let shouldRunPostHook = false;
        await Promise.all([
          ...this.mirrorPgs.map(async (pg) => {
            try {
              let r1 = await pg.pool.query(`select id, last_sync_task_date_ms from ${table} where id = $1`, [
                task.idOrKey
              ]);

              if (task.action === 'create') {
                if (r1.rows.length > 0) {
                  // Trying to create an item but the item already exist. We assume the item that already exists is newer.
                  // TODO: Maybe track how often this happens in the future
                  return;
                }
                let item = task.afterItem;
                item = this.preMirrorTransform({
                  collectionOrRecordPath: task.collectionOrRecordPath,
                  item: { ...item }
                });
                await pg.pool.query(
                  `insert into ${table} (id, item, updated_at, last_sync_task_date_ms) values ($1, $2, now(), $3)`,
                  [task.idOrKey, item, task.dateMS]
                );
                shouldRunPostHook = true;
              } else if (task.action === 'update') {
                let item = task.afterItem;
                item = this.preMirrorTransform({
                  collectionOrRecordPath: task.collectionOrRecordPath,
                  item: { ...item }
                });
                // If for some reason the item was never inserted previous just insert it as part of the update
                if (r1.rows.length === 0) {
                  await pg.pool.query(
                    `insert into ${table} (id, item, updated_at, last_sync_task_date_ms) values ($1, $2, now(), $3)`,
                    [task.idOrKey, item, task.dateMS]
                  );
                  shouldRunPostHook = true;
                  return;
                }
                if (extractLastSyncTaskDateMs(r1.rows[0]) > task.dateMS) {
                  // Looks like another sync task has updated things more recently. Skip update.
                  this.totalSyncTasksSkipped += 1;
                  return;
                }
                await pg.pool.query(
                  `update ${table} set item = $1, updated_at = now(), last_sync_task_date_ms = $2 where id = $3`,
                  [item, task.dateMS, task.idOrKey]
                );
                shouldRunPostHook = true;
              } else if (task.action === 'delete') {
                await pg.pool.query(`delete from ${table} where id = $1`, [task.idOrKey]);
                shouldRunPostHook = true;
              }
            } catch (e) {
              this.errorHandler({
                message: `Trouble mirroring in handleMirrorEvent.`,
                meta: { task, errorMsg: e.message },
                error: e
              });
            }
          }),
          ...this.auditPgs.map(async (pg) => {
            try {
              await pg.pool.query(
                `insert into ${auditTable} (itemId, beforeItem, afterItem, action, recorded_at) values ($1, $2, $3, $4, now())`,
                [
                  task.idOrKey,
                  task.beforeItem
                    ? this.preMirrorTransform({
                        collectionOrRecordPath: task.collectionOrRecordPath,
                        item: task.beforeItem
                      })
                    : {},
                  task.afterItem
                    ? this.preMirrorTransform({
                        collectionOrRecordPath: task.collectionOrRecordPath,
                        item: task.afterItem
                      })
                    : {},
                  task.action
                ]
              );
            } catch (e) {
              this.errorHandler({
                message: `Trouble adding audit in handleMirrorEvent.`,
                meta: {
                  task,
                  pgTitle: pg.title,
                  errorMsg: e.message
                },
                error: e
              });
            }
          })
        ]);

        if (shouldRunPostHook) {
          await this.postMirrorHook({
            action: task.action === 'delete' ? 'delete' : 'create/update',
            collectionOrRecordPath: task.collectionOrRecordPath,
            item: task.afterItem
          });
        }
      } catch (e) {
        this.errorHandler({
          message: `Trouble running handleSyncTasks.`,
          meta: {
            task,
            errorMsg: e.message
          },
          error: e
        });
      } finally {
        delete this.syncTaskRunningIdOrKeys[task.idOrKey];
        this.totalSyncTasksProcessed += 1;
      }
    };
  }

  public queueSyncTaskValidator(tasks: SyncTaskValidator[]) {
    tasks.forEach((task) => {
      if (
        !this.collectionOrRecordPathsMeta.map((e) => e.collectionOrRecordPath).includes(task.collectionOrRecordPath)
      ) {
        this.errorHandler({
          message: `Cannot sync item. The collectionOrRecordPath is not registered.`,
          meta: {
            collectionOrRecordPath: task.collectionOrRecordPath
          }
        });
        return;
      }
      this.syncValidatorQueue.add(this.handleSyncTaskValidator(task));
    });
  }

  private async handleUnexpectedItemState(p: {
    idOrKey: string;
    collectionOrRecordPath: string;
    dateMS: number;
    msg: string;
    pgMirrorIndex: number;
  }): Promise<{ result: ValidationResult }> {
    let result: ValidationResult = {
      collectionOrRecordPath: p.collectionOrRecordPath,
      idOrKey: p.idOrKey,
      itemState: 'ITEMS_WERE_IN_EXPECTED_STATE',
      poolTitle: this.mirrorPgs[p.pgMirrorIndex].title,
      description: p.msg
    };
    try {
      const table = `mirror_${p.collectionOrRecordPath}`;
      const r1 = await Promise.all([
        this.fetchItemFromFirebase({ collectionOrRecordPath: p.collectionOrRecordPath, idOrKey: p.idOrKey }),
        this.mirrorPgs[
          p.pgMirrorIndex
        ].pool.query(`select id, last_sync_task_date_ms, item from ${table} where id = $1`, [p.idOrKey])
      ]).catch((e) => {
        throw e;
      });

      const firebaseObject = r1[0];
      const pgObject = r1[1].rows[0]?.item || null;

      if (firebaseObject && pgObject) {
        // Since both exists check if they are in sync
        if (stable(pgObject) === stable(firebaseObject)) {
          // Since things are the same looks like it got synced correctly at some point
          // Do nothing
        } else {
          await this.mirrorPgs[
            p.pgMirrorIndex
          ].pool.query(`update ${table} set item = $1, updated_at = now(), last_sync_task_date_ms = $2 where id = $3`, [
            firebaseObject,
            p.dateMS,
            p.idOrKey
          ]);
          result.itemState = 'ITEMS_DID_NOT_MATCH';
          result.pgItem = pgObject;
          result.fbItem = firebaseObject;
        }
      } else if (firebaseObject && !pgObject) {
        // Insert item
        await this.mirrorPgs[
          p.pgMirrorIndex
        ].pool.query(`insert into ${table} (id, item, updated_at, last_sync_task_date_ms) values ($1, $2, now(), $3)`, [
          p.idOrKey,
          firebaseObject,
          p.dateMS
        ]);
        result.itemState = 'ITEM_WAS_MISSING_IN_MIRROR';
        result.fbItem = firebaseObject;
      } else if (!firebaseObject && pgObject) {
        await this.mirrorPgs[p.pgMirrorIndex].pool.query(`delete from ${table} where id = $1`, [p.idOrKey]);
        result.itemState = 'ITEM_WAS_NOT_DELETED_IN_MIRROR';
        result.pgItem = pgObject;
      } else if (!firebaseObject && !pgObject) {
        // Since both have been deleted they appear to be in sync.
        // Do nothing
      }
    } catch (e) {
      this.errorHandler({ message: 'Error running handleUnexpectedSyncItem', error: e, meta: p });
    }

    return { result: result };
  }

  private async fetchItemFromFirebase(p: { idOrKey: string; collectionOrRecordPath: string }): Promise<Object | null> {
    // Figure out if firestore or rtdb
    let meta = this.collectionOrRecordPathsMeta.find((t) => t.collectionOrRecordPath === p.collectionOrRecordPath);
    if (!meta) {
      this.errorHandler({
        message: `Unable to find meta for a collectionOrRecordPath. collectionOrRecordPath: ${p.collectionOrRecordPath}. Cannot fetchItemFromFirebase.`
      });
      return null;
    }

    if (meta.source === 'firestore') {
      const r = await this.firestore.collection(p.collectionOrRecordPath).doc(p.idOrKey).get();
      const data = r.data();
      if (data) {
        return this.preMirrorTransform({ collectionOrRecordPath: p.collectionOrRecordPath, item: { ...data } });
      }
    } else if (meta.source === 'rtdb') {
      const r = await this.rtdb.ref(`${p.collectionOrRecordPath}/${p.idOrKey}`).once('value');
      const data = r.val();
      if (data) {
        return this.preMirrorTransform({ collectionOrRecordPath: p.collectionOrRecordPath, item: { ...data } });
      }
    } else {
      this.errorHandler({ message: `Unknown meta source in fetchItemFromFirebase` });
    }
    return null;
  }

  private handleSyncTaskValidator(task: SyncTaskValidator) {
    return async () => {
      if (this.syncValidatorTaskRunningIdOrKeys[task.idOrKey]) {
        if (this.syncValidatorTaskRunningIdOrKeys[task.idOrKey] > task.dateMS) {
          // if the pending task is older than the one being executed we discard it
          this.totalSyncValidatorsTasksSkipped += 1;
          return;
        } else {
          // wait a second then queue this task again
          this.totalSyncValidatorTasksPendingRetry += 1;
          await new Promise((r) => setTimeout(() => r(), 1000));
          this.queueSyncTaskValidator([task]);
          this.totalSyncValidatorTasksPendingRetry -= 1;
          return;
        }
      }

      this.syncValidatorTaskRunningIdOrKeys[task.idOrKey] = task.dateMS;

      // Order matters. This line should not be any higher in this fn
      if (this.syncValidatorTaskDebuggerFn) {
        await this.syncValidatorTaskDebuggerFn({ task });
      }

      let shouldrunPostHook = false;

      try {
        await Promise.all(
          this.mirrorPgs.map(async (pg, index) => {
            try {
              const table = `mirror_${task.collectionOrRecordPath}`;
              const r1 = await pg.pool.query(`select id, last_sync_task_date_ms, item from ${table} where id = $1`, [
                task.idOrKey
              ]);
              const lastSyncTaskDateMs = extractLastSyncTaskDateMs(r1.rows[0]);

              if (task.action === 'create' || task.action === 'update') {
                if (r1.rows.length !== 1) {
                  // Since unexpected length we sync it just in case
                  const he = await this.handleUnexpectedItemState({
                    collectionOrRecordPath: task.collectionOrRecordPath,
                    idOrKey: task.idOrKey,
                    dateMS: task.dateMS,
                    msg: 'handleSyncTaskValidator create/update has an unexpected length',
                    pgMirrorIndex: index
                  });
                  if (he.result.itemState !== 'ITEMS_WERE_IN_EXPECTED_STATE') {
                    this.totalSyncValidationTasksInUnpexectedState += 1;
                    shouldrunPostHook = true;
                  }

                  return;
                }

                if (lastSyncTaskDateMs > task.dateMS) {
                  // Since the lasted sync date is newer then we just ignore this
                  this.totalSyncValidatorsTasksSkipped += 1;
                  return;
                }

                if (lastSyncTaskDateMs < task.dateMS) {
                  const he = await this.handleUnexpectedItemState({
                    collectionOrRecordPath: task.collectionOrRecordPath,
                    idOrKey: task.idOrKey,
                    dateMS: task.dateMS,
                    msg:
                      'handleSyncTaskValidator create/update the last_sync_task_date_ms is less than task.dateMS suggesting the syncTask was never run',
                    pgMirrorIndex: index
                  });
                  if (he.result.itemState !== 'ITEMS_WERE_IN_EXPECTED_STATE') {
                    this.totalSyncValidationTasksInUnpexectedState += 1;
                    shouldrunPostHook = true;
                  }
                  return;
                }

                if (lastSyncTaskDateMs === task.dateMS) {
                  if (
                    stable(r1.rows[0].item) !==
                    stable(
                      this.preMirrorTransform({
                        item: task.afterItem,
                        collectionOrRecordPath: task.collectionOrRecordPath
                      })
                    )
                  ) {
                    const he = await this.handleUnexpectedItemState({
                      collectionOrRecordPath: task.collectionOrRecordPath,
                      idOrKey: task.idOrKey,
                      dateMS: task.dateMS,
                      msg:
                        'handleSyncTaskValidator create/update the last_sync_task_date_ms matches but the items do not match',
                      pgMirrorIndex: index
                    });
                    if (he.result.itemState !== 'ITEMS_WERE_IN_EXPECTED_STATE') {
                      this.totalSyncValidationTasksInUnpexectedState += 1;
                      shouldrunPostHook = true;
                    }
                    return;
                  }
                }
              } else if (task.action === 'delete') {
                if (r1.rows.length > 0) {
                  const he = await this.handleUnexpectedItemState({
                    collectionOrRecordPath: task.collectionOrRecordPath,
                    idOrKey: task.idOrKey,
                    dateMS: task.dateMS,
                    msg: 'handleSyncTaskValidator delete a row shows up but it should have been deleted',
                    pgMirrorIndex: index
                  });
                  if (he.result.itemState !== 'ITEMS_WERE_IN_EXPECTED_STATE') {
                    this.totalSyncValidationTasksInUnpexectedState += 1;
                    shouldrunPostHook = true;
                  }
                  return;
                }
              } else {
                this.errorHandler({ message: 'Unknown taskValidator action type', meta: { task } });
              }
            } catch (e) {
              this.errorHandler({
                message: 'Error while running handleSyncTaskValidator',
                meta: { task, pgTitle: pg.title, errorMsg: e.message }
              });
            }
          })
        );

        if (shouldrunPostHook) {
          // Since we had at least one unexpected result we run the posthook again
          await this.postMirrorHook({
            action: task.action === 'delete' ? 'delete' : 'create/update',
            collectionOrRecordPath: task.collectionOrRecordPath,
            item: task.afterItem
          });
        }
      } catch (e) {
        this.errorHandler({
          message: 'Trouble running handleSyncTaskValidator',
          meta: { task, errorMsg: e.message }
        });
      } finally {
        this.totalSyncValidatorTasksProcessed += 1;
        delete this.syncValidatorTaskRunningIdOrKeys[task.idOrKey];
      }
    };
  }

  private async collectionOrRecordPathMirrorValidation(p: {
    collectionOrRecordPathMeta: CollectionOrRecordPathMeta;
    validationErrorLogger: ValidationErrorLogger;
    processingErrorLogger: ProcessingErrorLogger;
    currentResult: FullMirrorValidationRunResult;
    batchSize: number;
  }) {
    const table = `mirror_${p.collectionOrRecordPathMeta.collectionOrRecordPath}`;
    const validateItem = async (item: any) => {
      try {
        const unexpectedStateResults: ItemState[] = [];
        for (let i = 0; i < this.mirrorPgs.length; i++) {
          const pp = this.mirrorPgs[i];

          const r1 = await pp.pool.query(`select * from ${table} where id = $1`, [item.id]);
          if (r1.rows.length === 0) {
            // Might have an issue where item missing in PG
            const { result } = await this.handleUnexpectedItemState({
              collectionOrRecordPath: p.collectionOrRecordPathMeta.collectionOrRecordPath,
              dateMS: Date.now(),
              idOrKey: item.id,
              msg: '',
              pgMirrorIndex: i
            });
            p.currentResult.validationResults[result.itemState] += 1;
            if (result.itemState !== 'ITEMS_WERE_IN_EXPECTED_STATE') {
              unexpectedStateResults.push(result.itemState);
              p.currentResult.totalValidationErrors += 1;
              p.validationErrorLogger(result);
            }
          } else {
            const dbItem = r1.rows[0].item;
            const item_transformed = this.preMirrorTransform({
              collectionOrRecordPath: p.collectionOrRecordPathMeta.collectionOrRecordPath,
              item: { ...item }
            });
            if (stable(dbItem) !== stable(item_transformed)) {
              // Might have an issue where pg and firebase don't match
              const { result } = await this.handleUnexpectedItemState({
                collectionOrRecordPath: p.collectionOrRecordPathMeta.collectionOrRecordPath,
                dateMS: Date.now(),
                idOrKey: item.id,
                msg: '',
                pgMirrorIndex: i
              });
              p.currentResult.validationResults[result.itemState] += 1;

              if (result.itemState !== 'ITEMS_WERE_IN_EXPECTED_STATE') {
                unexpectedStateResults.push(result.itemState);
                p.currentResult.totalValidationErrors += 1;
                p.validationErrorLogger(result);
              }
            } else {
              p.currentResult.validationResults['ITEMS_WERE_IN_EXPECTED_STATE'] += 1;
            }
          }
          await pp.pool.query(`update ${table} set validation_number = 0 where id = $1`, [item.id]);
          p.currentResult.totalRowsProcessed += 1;
        }

        if (unexpectedStateResults.filter((r) => r !== 'ITEMS_WERE_IN_EXPECTED_STATE').length) {
          // Since we had at least one unexpected result we run the posthook again
          await this.postMirrorHook({
            action: 'create/update',
            collectionOrRecordPath: p.collectionOrRecordPathMeta.collectionOrRecordPath,
            item
          });
        }
      } catch (e) {
        p.currentResult.totalProcessingErrors += 1;
        p.processingErrorLogger({ error: e });
      } finally {
        p.currentResult.totalDocsOrNodesProcessed += 1;
      }
    };

    // Pull down and validate all the items in a firestore collection or a realtime database node
    if (p.collectionOrRecordPathMeta.source === 'firestore') {
      await fetchAndProcessFirestoreCollection({
        firestore: this.firestore,
        collection: p.collectionOrRecordPathMeta.collectionOrRecordPath,
        firestoreFetchBatchSize: p.batchSize * 10,
        processFnConcurrency: p.batchSize,
        processFn: validateItem
      });
    } else if (p.collectionOrRecordPathMeta.source === 'rtdb') {
      await fetchAndProcessRtdbRecordPath({
        rtdbBatchSize: p.batchSize * 10,
        processFnConcurrency: p.batchSize,
        recordPath: p.collectionOrRecordPathMeta.collectionOrRecordPath,
        rtdb: this.rtdb,
        processFn: validateItem
      });
    } else {
      throw new Error('Unknown meta source for fullMirrorValidation');
    }

    // Query the mirror tables and look for rows and look for extra items
    for (let i = 0; i < this.mirrorPgs.length; i++) {
      let pp = this.mirrorPgs[i];
      let potentialExtraItems = await pp.pool.query(`select * from ${table} where validation_number = $1`, [
        p.currentResult.runId
      ]);

      for (let k = 0; k < potentialExtraItems.rows.length; k++) {
        // Might have an issue where something was not removed from PG
        let extraItem = potentialExtraItems.rows[k].item;
        const { result } = await this.handleUnexpectedItemState({
          collectionOrRecordPath: p.collectionOrRecordPathMeta.collectionOrRecordPath,
          dateMS: Date.now(),
          idOrKey: extraItem.id,
          msg: '',
          pgMirrorIndex: i
        });
        p.currentResult.validationResults[result.itemState] += 1;
        if (result.itemState !== 'ITEMS_WERE_IN_EXPECTED_STATE') {
          p.currentResult.totalValidationErrors += 1;
          p.validationErrorLogger(result);
          await this.postMirrorHook({
            action: 'delete',
            collectionOrRecordPath: p.collectionOrRecordPathMeta.collectionOrRecordPath,
            item: {}
          });
        } else {
          p.currentResult.validationResults['ITEMS_WERE_IN_EXPECTED_STATE'] += 1;
        }
      }
    }
  }

  public async fullMirrorValidation(p: {
    collectionsOrRecordPaths: string[];
    batchSize: number;
    progressLogger: (status: {
      estimatedPercentComplete: number;
      minutesHasRun: number;
      currentResult: FullMirrorValidationRunResult;
    }) => void;
    validationErrorLogger: ValidationErrorLogger;
    processingErrorLogger: ProcessingErrorLogger;
  }): Promise<FullMirrorValidationResult> {
    let startMS = Date.now();
    let result: FullMirrorValidationRunResult = {
      initialRowCounts: {},
      status: 'running',
      runId: Date.now(),
      startMS: Date.now(),
      totalInitialRowsFromMirrorTables: 0,
      totalDocsOrNodesProcessed: 0,
      totalProcessingErrors: 0,
      totalValidationErrors: 0,
      validationResults: {
        ITEMS_DID_NOT_MATCH: 0,
        ITEMS_WERE_IN_EXPECTED_STATE: 0,
        ITEM_WAS_MISSING_IN_MIRROR: 0,
        ITEM_WAS_NOT_DELETED_IN_MIRROR: 0
      },
      totalRowsProcessed: 0
    };

    const progressLoggerInterval = setInterval(() => {
      const estimatedPercentComplete = result.totalDocsOrNodesProcessed / result.totalInitialRowsFromMirrorTables;
      const minutesHasRun = (Date.now() - result.startMS) / 1000 / 60;
      p.progressLogger({ currentResult: result, estimatedPercentComplete, minutesHasRun });
    }, 5000);
    try {
      // Make sure each collectionsOrRecordPath is valid and get initial size of tables

      for (let k = 0; k < this.mirrorPgs.length; k++) {
        const pg = this.mirrorPgs[k];
        result.initialRowCounts[pg.title] = {};

        for (let i = 0; i < p.collectionsOrRecordPaths.length; i++) {
          const c = p.collectionsOrRecordPaths[i];
          const v = this.collectionOrRecordPathsMeta.find((e) => e.collectionOrRecordPath === c);
          if (!v) {
            throw new Error(`Unable to run fullMirrorValidation. ${c} is not a valid collectionOrRecordPath`);
          }

          const table = `mirror_${c}`;
          const r1 = await pg.pool.query(`select count(id) as count from ${table}`);
          await pg.pool.query(`update ${table} set validation_number = $1`, [result.runId]);
          if (r1.rows.length !== 1) {
            throw new Error(
              `Unexpected row count in fullMirrorValidation while checking initial length of mirror table. PgTitle: ${pg.title}. C: ${c}`
            );
          }
          const originalRowCount = r1.rows[0].count;
          result.totalInitialRowsFromMirrorTables += parseInt(originalRowCount);
          result.initialRowCounts[pg.title][c] = originalRowCount;
        }
      }

      // Run the validations for each collectionsOrRecordPath
      for (let i = 0; i < p.collectionsOrRecordPaths.length; i++) {
        const c = p.collectionsOrRecordPaths[i];
        const meta = this.collectionOrRecordPathsMeta.find((t) => t.collectionOrRecordPath === c);
        if (!meta) {
          throw new Error(`Cannot run fullMirrorValidation for ${c} with meta.`);
        }

        await this.collectionOrRecordPathMirrorValidation({
          currentResult: result,
          batchSize: p.batchSize,
          collectionOrRecordPathMeta: meta,
          validationErrorLogger: p.validationErrorLogger,
          processingErrorLogger: p.processingErrorLogger
        });
      }
    } catch (e) {
      return { status: 'failed', errorMsg: e.message, finalFullMirrorValidationRunResult: result };
    } finally {
      clearInterval(progressLoggerInterval);
    }

    result.status = 'finished';
    result.finishMS = Date.now();
    result.totalRunTimeSeconds = Math.round((startMS - Date.now()) / 1000);

    return result;
  }

  public async trimOldAudits(p: {
    collectionsOrRecordPaths: string[];
    daysToRetain: number;
    progress: (status: string) => void;
  }): Promise<{ status: 'success' | 'error' }> {
    let hadError = false;
    for (let i = 0; i < p.collectionsOrRecordPaths.length; i++) {
      const c = p.collectionsOrRecordPaths[i];
      const v = this.collectionOrRecordPathsMeta.find((e) => e.collectionOrRecordPath === c);
      if (!v) {
        throw new Error(`Unable to run fullMirrorValidation. ${c} is not a valid collectionOrRecordPath`);
      }
    }
    p.progress('Collections and record paths appear valid. Will start audit trim for each.');

    for (let i = 0; i < p.collectionsOrRecordPaths.length; i++) {
      const c = p.collectionsOrRecordPaths[i];
      const table = `audit_${c}`;
      const query = `delete from ${table} where recorded_at < (NOW() - INTERVAL '${p.daysToRetain} days') `;
      const start = Date.now();
      for (let k = 0; k < this.auditPgs.length; k++) {
        try {
          p.progress(`Start audit trim for ${p.collectionsOrRecordPaths}. Pool Title: ${this.auditPgs[k].title}`);
          await this.auditPgs[k].pool.query(query);
        } catch (e) {
          p.progress(
            `Trouble running audit trim for ${p.collectionsOrRecordPaths}. Pool Title: ${this.auditPgs[k].title} Error: ${e.message}`
          );
          console.error(e);

          hadError = true;
        }
        p.progress(
          `Ran audit trim for ${p.collectionsOrRecordPaths}. Pool Title: ${
            this.auditPgs[k].title
          }. Total minutes to run: ${Date.now() - start / 1000 / 60}`
        );
      }
    }

    return { status: hadError ? 'error' : 'success' };
  }

  public static generateSyncTaskFromWriteTrigger(p: {
    type: 'firestore' | 'rtdb';
    collectionOrRecordPath: string;
    firestoreTriggerWriteChangeObject: any;
  }): { syncTask: SyncTask; syncTaskValidator: SyncTaskValidator } {
    const change = p.firestoreTriggerWriteChangeObject;

    let action: SyncTask['action'];

    if (change.after.exists && change.before.exists) {
      action = 'update';
    } else if (change.after.exists && !change.before.exists) {
      action = 'create';
    } else if (!change.after.exists && change.before.exists) {
      action = 'delete';
    } else {
      throw new Error('Unable to determine the action for generateSyncTaskFromWriteTrigger');
    }

    let beforeItem = p.type === 'firestore' ? change.before.data() : change.before.val();
    let afterItem = p.type === 'firestore' ? change.after.data() : change.after.val();

    let idOrKey = '';
    if (p.type === 'firestore') {
      idOrKey = !!beforeItem ? change.before.id : change.after.id;
    } else {
      idOrKey = !!beforeItem ? change.before.key : change.after.key;
    }

    if (!idOrKey) {
      throw new Error('Unable to generate sync task! Cannot find idOrKey!');
    }

    const dateMS = Date.now();

    const syncTask: SyncTask = {
      action: action,
      collectionOrRecordPath: p.collectionOrRecordPath,
      dateMS,
      idOrKey,
      beforeItem,
      afterItem
    };

    const syncTaskValidator: SyncTaskValidator = {
      action: action,
      collectionOrRecordPath: p.collectionOrRecordPath,
      dateMS,
      idOrKey,
      afterItem
    };

    return { syncTask, syncTaskValidator };
  }
}

function extractLastSyncTaskDateMs(obj: any): number {
  if (!obj) {
    return 0;
  } else if (obj['last_sync_task_date_ms']) {
    return parseInt(obj['last_sync_task_date_ms']) || 0;
  } else {
    return 0;
  }
}
