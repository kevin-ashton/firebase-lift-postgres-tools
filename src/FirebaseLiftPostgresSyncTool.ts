import { Pool } from 'pg';
import { SyncTask } from './models';
import * as fbAdmin from 'firebase-admin';
import * as Queue from 'promise-queue';

type ErrorHanlder = (p: { message: string; error?: any }) => void;
type SyncTaskDebuggerFn = (p: { task: any }) => Promise<void>;

export class FirebaseLiftPostgresSyncTool {
  private mirrorPgs: { title: string; pool: Pool }[] = [];
  private auditPgs: { title: string; pool: Pool }[] = [];
  private fbApp: fbAdmin.app.App = null as any;
  private baseTableNames: string[] = [];
  private queue: Queue = null as any;
  private errorHandler: ErrorHanlder = null as any;
  private syncTaskDebuggerFn: null | SyncTaskDebuggerFn = null;
  private runningIdOrKeys: Record<string, number> = {}; // Used to track if the same item is already being processed

  private totalSyncTasksProcessed = 0;
  private totalErrors = 0;
  private totalSyncTasksPendingRetry = 0;
  private totalSyncTasksSkipped = 0;

  constructor(config: {
    mirrorsPgs: { title: string; pool: Pool }[];
    auditPgs: { title: string; pool: Pool }[];
    fbApp: fbAdmin.app.App;
    syncQueueConcurrency: number;
    errorHandler: (p: { message: string; error?: any }) => void;
    baseTableNames: string[]; // Names of collections or root record paths
  }) {
    console.log('Init FirebaseLiftPostgresSyncTool');
    this.mirrorPgs = config.mirrorsPgs;
    this.auditPgs = config.auditPgs;
    this.fbApp = config.fbApp;
    this.queue = new Queue(config.syncQueueConcurrency, Infinity);
    this.baseTableNames = config.baseTableNames;
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
        for (let k = 0; k < this.baseTableNames.length; k++) {
          const baseTableName = this.baseTableNames[k];
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
              const msg = `Trouble ensuring an mirror table has been created. BaseTableName: ${baseTableName}. DB Title: ${pg.title} Msg: ${ee.message}`;
              console.error(msg);
              this.errorHandler({ message: msg, error: ee });
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
        for (let k = 0; k < this.baseTableNames.length; k++) {
          const baseTableName = this.baseTableNames[k];
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
              const msg = `Trouble ensuring an audit table has been created. BaseTableName: ${baseTableName}. DB Title: ${pg.title} Msg: ${ee.message}`;
              console.error(msg);
              this.errorHandler({ message: msg, error: ee });
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
      totalSyncTasksWaitingInQueue: this.queue.getPendingLength(),
      totalSyncTasksCurrentlyRunning: Object.keys(this.runningIdOrKeys).length,
      totalSyncTasksPendingRetry: this.totalSyncTasksPendingRetry,
      totalSyncTasksSkipped: this.totalSyncTasksSkipped
    };
  }

  queueSyncTasks(tasks: SyncTask[]) {
    tasks.forEach((task) => {
      if (!this.baseTableNames.includes(task.collectionOrRecordPath)) {
        this.errorHandler({
          message: `Cannot sync item. The collectionOrRecordPath is not whitelisted. collectionOrRecordPath: ${task.collectionOrRecordPath} `
        });
        return;
      }
      this.queue.add(this.handleSyncTasks(task));
    });
  }

  // Little helper fn for testing
  _waitUntilQueueDrained() {
    return new Promise((resolve) => {
      const isDone = () => this.queue.getPendingLength() + this.queue.getQueueLength() === 0;
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
                    message: `Trying to create an item but the item already exist. IdOrKey: ${task.idOrKey} DB Title: ${pg.title}`
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
                message: `Trouble mirroring in handleMirrorEvent. ItemId: ${task.idOrKey} ErrorMsg: ${e.message} `,
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
                message: `Trouble adding audit in handleMirrorEvent. ItemId: ${task.idOrKey}, DB Title: ${pg.title} ErrorMsg: ${e.message} `,
                error: e
              });
            }
          })
        ]);
      } catch (e) {
        this.totalErrors += 1;
        this.errorHandler({
          message: `Trouble running handleSyncTasks. ItemId: ${task.idOrKey} ErrorMsg: ${e.message} `,
          error: e
        });
      } finally {
        delete this.runningIdOrKeys[task.idOrKey];
        this.totalSyncTasksProcessed += 1;
      }
    };
  }
}
