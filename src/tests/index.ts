import {
  init,
  getFirebaseLiftPostgresSyncTool,
  getPool1,
  baseExampleTableNames,
  generateMockFirebaseChangeObject,
  reset,
  getPool2
} from './helpers';
import { describe, run, test, otest, setOptions } from 'nano-test-runner';
import * as assert from 'assert';
import { generateSyncTaskFromWriteTrigger } from '../taskGenerators';
import * as stable from 'json-stable-stringify';
import { SyncTask } from '../models';

setOptions({ runPattern: 'serial', suppressConsole: true });

async function main() {
  await init();

  describe('FirebaseLiftPostgresSyncTool Basics', () => {
    test('Ensure mirror tables are created on startup', async () => {
      console.log('Attempt to drop table');
      try {
        await getPool1().query(`drop table mirror_${baseExampleTableNames[0]}`);
      } catch (e) {}

      await assert.rejects(getPool1().query(`select count(*) from mirror_${baseExampleTableNames[0]}`));
      getFirebaseLiftPostgresSyncTool();
      // Hacky but give it a moment to create the tables
      await new Promise((r) => setTimeout(() => r(), 1000));
      let r = await getPool1().query(`select count(*) from mirror_${baseExampleTableNames[0]}`);
      assert.deepEqual(r.rows.length, 1);
    });

    const item1 = {
      id: 'foo1',
      foo: 'foo ' + Math.random(),
      bar: 'bar ' + Math.random()
    };

    let createTask = generateSyncTaskFromWriteTrigger({
      type: 'firestore',
      collectionOrRecordPath: 'person',
      firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
        itemIdOrKey: item1.id,
        type: 'create',
        afterItem: item1,
        beforeItem: undefined,
        dbType: 'firestore'
      })
    });
    run(async () => {
      await reset();
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([createTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();
    });

    test('Create item into mirror/audit tables', async () => {
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(item1));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r2.rows[0].item), stable(item1));
      let r3 = await getPool2().query(`select * from audit_person where itemId = $1`, [item1.id]);
      assert.deepEqual(stable(r3.rows[0].afteritem), stable(item1));
      assert.deepEqual(stable(r3.rows[0].beforeitem), stable({}));
    });

    let updatedItem = { ...item1, ...{ foo: 'foo ' + Math.random() } };
    let updateTask = generateSyncTaskFromWriteTrigger({
      type: 'firestore',
      collectionOrRecordPath: 'person',
      firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
        itemIdOrKey: item1.id,
        type: 'update',
        afterItem: updatedItem,
        beforeItem: item1,
        dbType: 'firestore'
      })
    });
    run(async () => {
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();
    });

    test('Update item into mirror/audit tables', async () => {
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(updatedItem));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r2.rows[0].item), stable(updatedItem));
      let r3 = await getPool2().query(
        `select * from audit_person where itemId = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepEqual(stable(r3.rows[0].beforeitem), stable(item1));
      assert.deepEqual(stable(r3.rows[0].afteritem), stable(updatedItem));
    });

    let deleteTask = generateSyncTaskFromWriteTrigger({
      type: 'firestore',
      collectionOrRecordPath: 'person',
      firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
        itemIdOrKey: item1.id,
        type: 'delete',
        afterItem: undefined,
        beforeItem: updatedItem,
        dbType: 'firestore'
      })
    });

    run(async () => {
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([deleteTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();
      await new Promise((r) => setTimeout(() => r(), 1000));
    });

    test('Delete item into mirror/audit tables', async () => {
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(r1.rows.length, 0);
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(r2.rows.length, 0);
      let r3 = await getPool2().query(
        `select * from audit_person where itemId = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepEqual(stable(r3.rows[0].beforeitem), stable(updatedItem));
      assert.deepEqual(stable(r3.rows[0].afteritem), stable({}));
    });

    test('Race condition (2 tasks, same item, same moment, correct order)', async () => {
      await reset();

      let originalPendingRetry = getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksPendingRetry;

      getFirebaseLiftPostgresSyncTool()._registerSyncTaskDebugFn(async () => {
        await new Promise((r) => setTimeout(() => r(), 1000));
      });

      // Start the first one
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([createTask]);
      await new Promise((r) => setTimeout(() => r(), 200));
      // Queue the second one (bump time to make sure they are a bit different)
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([{ ...updateTask, ...{ dateMS: updateTask.dateMS + 10 } }]);
      await new Promise((r) => setTimeout(() => r(), 200));

      // Make sure there is a new pendingRetry
      assert.deepEqual(
        originalPendingRetry + 1,
        getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksPendingRetry
      );
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();

      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(updatedItem));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r2.rows[0].item), stable(updatedItem));
      let r3 = await getPool2().query(
        `select * from audit_person where itemId = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepEqual(stable(r3.rows[0].beforeitem), stable(item1));
      assert.deepEqual(stable(r3.rows[0].afteritem), stable(updatedItem));
    });

    const updateItem2 = { ...updatedItem, ...{ foo: 'foo ' + Math.random() } };
    const updateTask2: SyncTask = {
      ...updateTask,
      ...{ dateMS: updateTask.dateMS + 100, beforeItem: updatedItem, afterItem: updateItem2 }
    };

    test('Race condition (2 tasks, same item, same moment, wrong order)', async () => {
      await reset();

      let originalTotalSyncTasksSkipped = getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksSkipped;

      getFirebaseLiftPostgresSyncTool()._registerSyncTaskDebugFn(async () => {});

      getFirebaseLiftPostgresSyncTool().queueSyncTasks([createTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();

      getFirebaseLiftPostgresSyncTool()._registerSyncTaskDebugFn(async () => {
        await new Promise((r) => setTimeout(() => r(), 1000));
      });

      // Queue update #2
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask2]);
      await new Promise((r) => setTimeout(() => r(), 200));
      // Queue update #1 (which is older than the one currently executing)
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask]);
      await new Promise((r) => setTimeout(() => r(), 200));

      assert.deepEqual(
        originalTotalSyncTasksSkipped + 1,
        getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksSkipped
      );

      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();

      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(updateItem2));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r2.rows[0].item), stable(updateItem2));
      let r3 = await getPool2().query(
        `select * from audit_person where itemId = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepEqual(stable(r3.rows[0].beforeitem), stable(updatedItem));
      assert.deepEqual(stable(r3.rows[0].afteritem), stable(updateItem2));
    });

    test('Race condition (2 tasks, same item, not same moment, wrong order)', async () => {
      await reset();

      let originalTotalSyncTasksSkipped = getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksSkipped;

      getFirebaseLiftPostgresSyncTool()._registerSyncTaskDebugFn(async () => {});

      getFirebaseLiftPostgresSyncTool().queueSyncTasks([createTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();

      // Queue update #2
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask2]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();

      // Queue update #1 (which is older than the one currently executing)
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask]);
      await new Promise((r) => setTimeout(() => r(), 500));

      assert.deepEqual(
        originalTotalSyncTasksSkipped + 2, // skip by two since we have two mirror targets
        getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksSkipped
      );

      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();

      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(updateItem2));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r2.rows[0].item), stable(updateItem2));
      // We don't need to check the audit since it should still record both. We don't care as much of they are slighly out of order.
    });
  });
}

main();
