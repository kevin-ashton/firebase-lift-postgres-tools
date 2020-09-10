import { describe, otest, run, test, xtest } from 'nano-test-runner';
import {
  reset,
  getFirebaseLiftPostgresSyncTool,
  getFirebaseApp,
  collectionOrRecordPathMeta,
  generateMockFirebaseChangeObject,
  getPool1,
  getPool2
} from './helpers';
import { FirebaseLiftPostgresSyncTool } from '../FirebaseLiftPostgresSyncTool';
import * as assert from 'assert';
import * as stable from 'json-stable-stringify';

const item1 = {
  id: 'foo1',
  foo: 'foo ' + Math.random(),
  bar: 'bar ' + Math.random()
};

const rtdbRecordPath = collectionOrRecordPathMeta[1].collectionOrRecordPath;

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

      assert.deepEqual(tool.getStats().totalErrors, orignalStats.totalErrors);

      let r1 = await getPool1().query('select * from mirror_device where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(item1));
      let r2 = await getPool2().query('select * from mirror_device where id = $1', [item1.id]);
      assert.deepEqual(stable(r2.rows[0].item), stable(item1));
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

      assert.deepEqual(tool.getStats().totalErrors, originalStats.totalErrors);

      tool.queueSyncTaskValidator([syncTaskValidator]);
      await tool._waitUntilSyncValidatorQueueDrained();

      assert.deepEqual(tool.getStats().totalErrors, originalStats.totalErrors + 1);

      // Make sure it has healed
      let r1 = await getPool1().query('select * from mirror_device where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(item1));
      let r2 = await getPool2().query('select * from mirror_device where id = $1', [item1.id]);
      assert.deepEqual(stable(r2.rows[0].item), stable(item1));
    });
  });
}
