import { Pool } from 'pg';
import { SyncTask, SyncTaskValidator } from './models';
import * as fbAdmin from 'firebase-admin';
import * as Queue from 'promise-queue';

type ErrorHanlder = (p: { message: string; error?: any; meta?: any }) => void;
type SyncTaskDebuggerFn = (p: { task: any }) => Promise<void>;

export type CollectionOrRecordPathMeta = { collectionOrRecordPath: string; source: 'rtdb' | 'firestore' };

export class FirebaseLiftPostgresSyncTool {
  private errorHandler: ErrorHanlder = null as any;

  private mirrorPgs: { title: string; pool: Pool }[] = [];
  private auditPgs: { title: string; pool: Pool }[] = [];

  private rtdb: fbAdmin.database.Database = null as any;
  private firestore: fbAdmin.firestore.Firestore = null as any;
  private collectionOrRecordPathMeta: CollectionOrRecordPathMeta[] = [];

  private syncQueue: Queue = null as any;

  private syncTaskDebuggerFn: null | SyncTaskDebuggerFn = null;
  private runningIdOrKeys: Record<string, number> = {}; // Used to track if the same item is already being processed

  private totalSyncTasksProcessed = 0;
  private totalErrors = 0;
  private totalSyncTasksPendingRetry = 0;
  private totalSyncTasksSkipped = 0;

  constructor(config: {
    mirrorsPgs: { title: string; pool: Pool }[];
    auditPgs: { title: string; pool: Pool }[];
    collectionOrRecordPathMeta: CollectionOrRecordPathMeta[];
    rtdb: fbAdmin.database.Database;
    firestore: fbAdmin.firestore.Firestore;
    syncQueueConcurrency: number;
    errorHandler: (p: { message: string; error?: any }) => void;
  }) {
    console.log('Init FirebaseLiftPostgresSyncTool');
    this.mirrorPgs = config.mirrorsPgs;
    this.auditPgs = config.auditPgs;
    this.syncQueue = new Queue(config.syncQueueConcurrency, Infinity);
    this.collectionOrRecordPathMeta = config.collectionOrRecordPathMeta;
    this.rtdb = config.rtdb;
    this.firestore = config.firestore;
    this.errorHandler = config.errorHandler;
    this.ensureMirrorTablesExists();
    this.ensureAuditTablesExists();
  }

  public _registerSyncTaskDebugFn(fn: SyncTaskDebuggerFn) {
    this.syncTaskDebuggerFn = fn;
  }

  private async ensureMirrorTablesExists() {
    console.log('Running ensureMirrorTablesExists');
    if (this.mirrorPgs.length > 0) {
      for (let i = 0; i < this.mirrorPgs.length; i++) {
        const pg = this.mirrorPgs[i];
        const baseTableNames = this.collectionOrRecordPathMeta.map((e) => e.collectionOrRecordPath);
        for (let k = 0; k < baseTableNames.length; k++) {
          const baseTableName = baseTableNames[k];
          try {
            await pg.pool.query(`select count(*) from mirror_${baseTableName}`);
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
              this.totalErrors += 1;
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
        const baseTableNames = this.collectionOrRecordPathMeta.map((e) => e.collectionOrRecordPath);
        for (let k = 0; k < baseTableNames.length; k++) {
          const baseTableName = baseTableNames[k];
          try {
            await pg.pool.query(`select count(*) from audit_${baseTableName}`);
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
              this.totalErrors += 1;
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
      totalSyncTasksWaitingInQueue: this.syncQueue.getPendingLength(),
      totalSyncTasksCurrentlyRunning: Object.keys(this.runningIdOrKeys).length,
      totalSyncTasksPendingRetry: this.totalSyncTasksPendingRetry,
      totalSyncTasksSkipped: this.totalSyncTasksSkipped
    };
  }

  queueSyncTasks(tasks: SyncTask[]) {
    tasks.forEach((task) => {
      if (!this.collectionOrRecordPathMeta.map((e) => e.collectionOrRecordPath).includes(task.collectionOrRecordPath)) {
        this.errorHandler({
          message: `Cannot sync item. The collectionOrRecordPath is not whitelisted.`,
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

  public validatePgMirrors(p: {}) {}

  private handleSyncTasks(task: SyncTask) {
    return async () => {
      if (this.runningIdOrKeys[task.idOrKey]) {
        if (this.runningIdOrKeys[task.idOrKey] > task.dateMS) {
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

      this.runningIdOrKeys[task.idOrKey] = task.dateMS;

      // Order matters. This line should not be any higher in this fn
      if (this.syncTaskDebuggerFn) {
        await this.syncTaskDebuggerFn({ task });
      }

      try {
        const table = `mirror_${task.collectionOrRecordPath}`;
        const auditTable = `audit_${task.collectionOrRecordPath}`;
        await Promise.all([
          ...this.mirrorPgs.map(async (pg) => {
            try {
              let r1 = await pg.pool.query(`select id, last_sync_task_date_ms from ${table} where id = $1`, [
                task.idOrKey
              ]);

              if (task.action === 'create') {
                if (r1.rows.length > 0) {
                  this.errorHandler({
                    message: `Trying to create an item but the item already exist.`,
                    meta: {
                      task,
                      pgTitle: pg.title
                    }
                  });
                  return;
                }
                let item = task.afterItem;
                await pg.pool.query(
                  `insert into ${table} (id, item, updated_at, last_sync_task_date_ms) values ($1, $2, now(), $3)`,
                  [task.idOrKey, item, task.dateMS]
                );
              } else if (task.action === 'update') {
                let item = task.afterItem;
                if (r1.rows && r1.rows[0] && r1.rows[0].last_sync_task_date_ms > task.dateMS) {
                  // Looks like another sync task has updated things more recently. Skip update.
                  this.totalSyncTasksSkipped += 1;
                  return;
                }
                await pg.pool.query(
                  `update ${table} set item = $1, updated_at = now(), last_sync_task_date_ms = $2 where id = $3`,
                  [item, task.dateMS, task.idOrKey]
                );
              } else if (task.action === 'delete') {
                await pg.pool.query(`delete from ${table} where id = $1`, [task.idOrKey]);
              }
            } catch (e) {
              this.totalErrors += 1;
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
                  task.beforeItem ? task.beforeItem : {},
                  task.afterItem ? task.afterItem : {},
                  task.action
                ]
              );
            } catch (e) {
              this.totalErrors += 1;
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
      } catch (e) {
        this.totalErrors += 1;
        this.errorHandler({
          message: `Trouble running handleSyncTasks.`,
          meta: {
            task,
            errorMsg: e.message
          },
          error: e
        });
      } finally {
        delete this.runningIdOrKeys[task.idOrKey];
        this.totalSyncTasksProcessed += 1;
      }
    };
  }

  queueSyncTaskValidator(task: SyncTaskValidator[]) {}

  fullSyncValidation(p: { collectionsOrRecordPaths: string[] }) {}

  public static generateSyncTaskFromWriteTrigger(p: {
    type: 'firestore' | 'rtdb';
    collectionOrRecordPath: string;
    firestoreTriggerWriteChangeObject: any;
    dataScrubber?: (node: any) => Object;
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
    if (p.dataScrubber) {
      if (beforeItem) {
        beforeItem = p.dataScrubber(beforeItem);
      }
      if (afterItem) {
        afterItem = p.dataScrubber(afterItem);
      }
    }

    let idOrKey = '';
    if (p.type === 'firestore') {
      idOrKey = !!beforeItem ? beforeItem.id : afterItem.id;
    } else {
      idOrKey = !!beforeItem ? beforeItem.key : afterItem.key;
    }

    if (!idOrKey) {
      throw new Error('Unable to generate sync task! Cannot find idOrKey!');
    }

    const syncTask: SyncTask = {
      action: action,
      collectionOrRecordPath: p.collectionOrRecordPath,
      dateMS: Date.now(),
      idOrKey,
      beforeItem,
      afterItem
    };

    const syncTaskValidator: SyncTaskValidator = {
      action: action,
      collectionOrRecordPath: p.collectionOrRecordPath,
      dateMS: Date.now(),
      idOrKey,
      afterItem
    };

    return { syncTask, syncTaskValidator };
  }
}
