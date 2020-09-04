import { Pool } from 'pg';
import { SyncTask } from './models';
import * as fbAdmin from 'firebase-admin';
import * as Queue from 'promise-queue';

type ErrorHanlder = (p: { message: string; error?: any }) => void;

export class FirebaseLiftPostgresSyncTool {
  private mirrorPgs: Pool[] = [];
  private auditPgs: Pool[] = [];
  private fbApp: fbAdmin.app.App = null as any;
  private baseTableNames: string[] = [];
  private queue: Queue = null as any;
  private errorHandler: ErrorHanlder = null as any;

  private totalSyncTasksProcessed = 0;
  private totalErrors = 0;

  constructor(config: {
    mirrorsPgs: Pool[];
    auditPgs: Pool[];
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

  private async ensureMirrorTablesExists() {
    console.log('Running ensureMirrorTablesExists');
    if (this.mirrorPgs.length > 0) {
      for (let i = 0; i < this.mirrorPgs.length; i++) {
        const pg = this.mirrorPgs[i];
        for (let k = 0; k < this.baseTableNames.length; k++) {
          const baseTableName = this.baseTableNames[k];
          try {
            await pg.query(`select count(*) from mirror_${baseTableName}`);
          } catch (e) {
            try {
              console.log('Creating mirror table');
              pg.query(`
              CREATE TABLE mirror_${baseTableName}
              (
                id text PRIMARY KEY,
                item jsonb not null,
                updated_at timestamp not null,
                validation_number bigint
              );`);
            } catch (ee) {
              this.totalErrors += 1;
              const msg = `Trouble ensuring a mirror table has been created. ${ee.message}`;
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
            await pg.query(`select count(*) from audit_${baseTableName}`);
          } catch (e) {
            try {
              console.log('Creating audit table');
              pg.query(`
              CREATE TABLE audit_${baseTableName}
              (
                id serial PRIMARY KEY,
                beforeItem jsonb,
                afterItem jsonb,
                action text,
                recorded_at timestamp not null
              );`);
            } catch (ee) {
              this.totalErrors += 1;
              const msg = `Trouble ensuring a audit table has been created. ${ee.message}`;
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
      syncTasksWaitingInQueue: this.queue.getPendingLength(),
      syncTasksCurrentlyRunning: this.queue.getQueueLength
    };
  }

  queueSyncTasks(tasks: SyncTask[]) {
    tasks.forEach((task) => {
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

  private handleSyncTasks(task: SyncTask) {
    return async () => {
      let itemId = 'UNKNOWN';
      try {
        const table = `mirror_${task.collectionOrRecordPath}`;
        const auditTable = `audit_${task.collectionOrRecordPath}`;
        await Promise.all([
          ...this.mirrorPgs.map(async (pool) => {
            try {
              if (task.action === 'create' || task.action === 'update') {
                let item = task.afterItem;
                itemId = item.id;
                let r1 = await pool.query(`select id from ${table} where id = $1`, [itemId]);
                if (r1.rows.length > 0) {
                  await pool.query(`update ${table} set item = $1, updated_at = now() where id = $2`, [item, itemId]);
                } else {
                  await pool.query(`insert into ${table} (id, item, updated_at) values ($1, $2, now())`, [
                    item.id,
                    item
                  ]);
                }
              } else if (task.action === 'delete') {
                itemId = task.beforeItem.id;
                await pool.query(`delete from ${table} where id = $1`, [itemId]);
              }
            } catch (e) {
              this.totalErrors += 1;
              this.errorHandler({
                message: `Trouble mirroring in handleMirrorEvent. ItemId: ${itemId} ErrorMsg: ${e.message} `,
                error: e
              });
            }
          }),
          ...this.auditPgs.map(async (pool) => {
            try {
              await pool.query(
                `insert into ${auditTable} (beforeItem, afterItem, action, recorded_at) values ($1, $2, $3, now())`,
                [task.beforeItem ? task.beforeItem : {}, task.afterItem ? task.afterItem : {}, task.action]
              );
            } catch (e) {
              this.totalErrors += 1;
              this.errorHandler({
                message: `Trouble adding audit in handleMirrorEvent. ItemId: ${itemId} ErrorMsg: ${e.message} `,
                error: e
              });
            }
          })
        ]);
      } catch (e) {
        this.totalErrors += 1;
        this.errorHandler({
          message: `Trouble running handleSyncTasks. ItemId: ${itemId} ErrorMsg: ${e.message} `,
          error: e
        });
      }
      this.totalSyncTasksProcessed += 1;
    };
  }
}
