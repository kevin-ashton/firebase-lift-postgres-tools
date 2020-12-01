import { describe, otest, run, test, xtest } from 'nano-test-runner';
import {
  reset,
  getFirebaseLiftPostgresSyncTool,
  getFirebaseApp,
  collectionOrRecordPathsMeta,
  generateMockFirebaseChangeObject,
  getPool1,
  getPool2,
  exampleTransformFn
} from './helpers';
import { FirebaseLiftPostgresSyncTool } from '../FirebaseLiftPostgresSyncTool';
import * as assert from 'assert';
import * as stable from 'json-stable-stringify';

const item1 = {
  id: 'foo1',
  foo: 'foo ' + Math.random(),
  bar: 'bar ' + Math.random()
};

const item1_transformed = exampleTransformFn({ collectionOrRecordPath: 'device', item: item1 });

const rtdbRecordPath = collectionOrRecordPathsMeta[1].collectionOrRecordPath;

export async function rtdbBasicTests() {
  const rtdb = getFirebaseApp().database().ref(rtdbRecordPath);

  describe('RTDB Sanity Check', () => {
    test('Validate mirrors and basic validators', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      const orignalStats = tool.getStats();

      await rtdb.child(item1.id).set(item1);
      const { syncTask, syncTaskValidator } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
        type: 'rtdb',
        collectionOrRecordPath: rtdbRecordPath,
        firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
          itemIdOrKey: item1.id,
          type: 'create',
          afterItem: item1,
          beforeItem: undefined,
          dbType: 'rtdb'
        })
      });

      tool.queueSyncTasks([syncTask]);
      await tool._waitUntilSyncQueueDrained();
      tool.queueSyncTaskValidator([syncTaskValidator]);
      await tool._waitUntilSyncQueueDrained();

      assert.deepStrictEqual(tool.getStats().totalErrors, orignalStats.totalErrors);

      let r1 = await getPool1().query('select * from mirror_device where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(item1_transformed));
      let r2 = await getPool2().query('select * from mirror_device where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r2.rows[0].item), stable(item1_transformed));
    });

    test('Ensure has the ability to sync from rtdb', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      const originalStats = tool.getStats();

      await rtdb.child(item1.id).set(item1);
      const { syncTask, syncTaskValidator } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
        type: 'rtdb',
        collectionOrRecordPath: rtdbRecordPath,
        firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
          itemIdOrKey: item1.id,
          type: 'create',
          afterItem: item1,
          beforeItem: undefined,
          dbType: 'rtdb'
        })
      });

      tool.queueSyncTasks([syncTask]);
      await tool._waitUntilSyncQueueDrained();

      // Change it to something different
      const item1Update1 = { ...item1, ...{ update1: `foo - ${Math.random()}` } };
      await getPool1().query(`update mirror_${rtdbRecordPath} set item = $1 where id = $2`, [item1Update1, item1.id]);

      assert.deepStrictEqual(
        tool.getStats().totalSyncValidationTasksInUnpexectedState,
        originalStats.totalSyncValidationTasksInUnpexectedState
      );

      tool.queueSyncTaskValidator([syncTaskValidator]);
      await tool._waitUntilSyncValidatorQueueDrained();

      assert.deepStrictEqual(
        tool.getStats().totalSyncValidationTasksInUnpexectedState,
        originalStats.totalSyncValidationTasksInUnpexectedState + 1
      );

      // Make sure it has healed
      let r1 = await getPool1().query('select * from mirror_device where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(item1_transformed));
      let r2 = await getPool2().query('select * from mirror_device where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r2.rows[0].item), stable(item1_transformed));
    });
  });
}
