import {
  getFirebaseLiftPostgresSyncTool,
  getPool1,
  generateMockFirebaseChangeObject,
  collectionOrRecordPathsMeta,
  reset,
  getPool2,
  exampleTransformFn
} from './helpers';
import { FirebaseLiftPostgresSyncTool } from '../FirebaseLiftPostgresSyncTool';
import { describe, run, test, otest, setOptions } from 'nano-test-runner';
import * as assert from 'assert';
import * as stable from 'json-stable-stringify';
import { SyncTask } from '../models';

export function syncTasksTests() {
  describe('FirebaseLiftPostgresSyncTool Sync Tasks Basics', () => {
    test('Ensure mirror tables are created on startup', async () => {
      console.log('Attempt to drop table');
      try {
        await getPool1().query(`drop table mirror_${collectionOrRecordPathsMeta[0].collectionOrRecordPath}`);
      } catch (e) {}

      await assert.rejects(
        getPool1().query(`select count(*) from mirror_${collectionOrRecordPathsMeta[0].collectionOrRecordPath}`)
      );
      getFirebaseLiftPostgresSyncTool();
      // Hacky but give it a moment to create the tables
      await new Promise((r) => setTimeout(() => r(), 1000));
      let r = await getPool1().query(
        `select count(*) from mirror_${collectionOrRecordPathsMeta[0].collectionOrRecordPath}`
      );
      assert.deepStrictEqual(r.rows.length, 1);
    });

    const item1 = {
      id: 'foo1',
      foo: 'foo ' + Math.random(),
      bar: 'bar ' + Math.random()
    };

    const item1_transformed = exampleTransformFn({ collectionOrRecordPath: 'person', item: item1 });

    let createTask = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
      type: 'firestore',
      collectionOrRecordPath: 'person',
      firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
        itemIdOrKey: item1.id,
        type: 'create',
        afterItem: item1,
        beforeItem: undefined,
        dbType: 'firestore'
      })
    }).syncTask;
    run(async () => {
      await reset();
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([createTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();
    });

    test('Create item into mirror/audit tables', async () => {
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(item1_transformed));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r2.rows[0].item), stable(item1_transformed));
    });

    let updatedItem = { ...item1, ...{ foo: 'foo ' + Math.random() } };
    const updatedItem_transformed = exampleTransformFn({ collectionOrRecordPath: 'person', item: updatedItem });
    let updateTask = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
      type: 'firestore',
      collectionOrRecordPath: 'person',
      firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
        itemIdOrKey: item1.id,
        type: 'update',
        afterItem: updatedItem,
        beforeItem: item1,
        dbType: 'firestore'
      })
    }).syncTask;
    run(async () => {
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();
    });

    test('Update item into mirror/audit tables', async () => {
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(updatedItem_transformed));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r2.rows[0].item), stable(updatedItem_transformed));
      let r3 = await getPool2().query(
        `select * from audit_person where itemId = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepStrictEqual(stable(r3.rows[0].beforeitem), stable(item1_transformed));
      assert.deepStrictEqual(stable(r3.rows[0].afteritem), stable(updatedItem_transformed));
    });

    let deleteTask = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
      type: 'firestore',
      collectionOrRecordPath: 'person',
      firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
        itemIdOrKey: item1.id,
        type: 'delete',
        afterItem: undefined,
        beforeItem: updatedItem,
        dbType: 'firestore'
      })
    }).syncTask;

    run(async () => {
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([deleteTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();
      await new Promise((r) => setTimeout(() => r(), 1000));
    });

    test('Delete item into mirror/audit tables', async () => {
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(r1.rows.length, 0);
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(r2.rows.length, 0);
      let r3 = await getPool2().query(
        `select * from audit_person where itemId = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepStrictEqual(stable(r3.rows[0].beforeitem), stable(updatedItem_transformed));
      assert.deepStrictEqual(stable(r3.rows[0].afteritem), stable({}));
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
      assert.deepStrictEqual(
        originalPendingRetry + 1,
        getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksPendingRetry
      );
      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();

      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(updatedItem_transformed));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r2.rows[0].item), stable(updatedItem_transformed));
      let r3 = await getPool2().query(
        `select * from audit_person where itemId = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepStrictEqual(stable(r3.rows[0].beforeitem), stable(item1_transformed));
      assert.deepStrictEqual(stable(r3.rows[0].afteritem), stable(updatedItem_transformed));
    });

    const updateItem2 = { ...updatedItem, ...{ foo: 'foo ' + Math.random() } };
    const updatedItem2_transformed = exampleTransformFn({ collectionOrRecordPath: 'person', item: updateItem2 });
    const updateTask2: SyncTask = {
      ...updateTask,
      ...{ dateMS: updateTask.dateMS + 100, beforeItem: updatedItem, afterItem: updateItem2 }
    };

    test('Race condition (2 tasks, same item, same moment, wrong order)', async () => {
      await reset();

      let originalTotalSyncTasksSkipped = getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksSkipped;

      getFirebaseLiftPostgresSyncTool()._registerSyncTaskDebugFn(async () => {});

      getFirebaseLiftPostgresSyncTool().queueSyncTasks([createTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();

      getFirebaseLiftPostgresSyncTool()._registerSyncTaskDebugFn(async () => {
        await new Promise((r) => setTimeout(() => r(), 1000));
      });

      // Queue update #2
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask2]);
      await new Promise((r) => setTimeout(() => r(), 200));
      // Queue update #1 (which is older than the one currently executing)
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask]);
      await new Promise((r) => setTimeout(() => r(), 200));

      assert.deepStrictEqual(
        originalTotalSyncTasksSkipped + 1,
        getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksSkipped
      );

      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();

      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(updatedItem2_transformed));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r2.rows[0].item), stable(updatedItem2_transformed));
      let r3 = await getPool2().query(
        `select * from audit_person where itemId = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepStrictEqual(stable(r3.rows[0].beforeitem), stable(updatedItem_transformed));
      assert.deepStrictEqual(stable(r3.rows[0].afteritem), stable(updatedItem2_transformed));
    });

    test('Race condition (2 tasks, same item, not same moment, wrong order)', async () => {
      await reset();

      let originalTotalSyncTasksSkipped = getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksSkipped;

      getFirebaseLiftPostgresSyncTool()._registerSyncTaskDebugFn(async () => {});

      getFirebaseLiftPostgresSyncTool().queueSyncTasks([createTask]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();

      // Queue update #2
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask2]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();

      // Queue update #1 (which is older than the one currently executing)
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([updateTask]);
      await new Promise((r) => setTimeout(() => r(), 500));

      assert.deepStrictEqual(
        originalTotalSyncTasksSkipped + 2, // skip by two since we have two mirror targets
        getFirebaseLiftPostgresSyncTool().getStats().totalSyncTasksSkipped
      );

      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();

      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(updatedItem2_transformed));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r2.rows[0].item), stable(updatedItem2_transformed));
      // We don't need to check the audit since it should still record both. We don't care as much of they are slighly out of order.
    });
  });
}
