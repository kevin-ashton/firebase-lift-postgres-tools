import { describe, otest, run, test, xtest } from 'nano-test-runner';
import {
  reset,
  getFirebaseLiftPostgresSyncTool,
  getFirebaseApp,
  collectionOrRecordPathMeta,
  generateMockFirebaseChangeObject,
  getPool1
} from './helpers';
import { FirebaseLiftPostgresSyncTool } from '../FirebaseLiftPostgresSyncTool';
import * as assert from 'assert';
import * as stable from 'json-stable-stringify';
import { Pool } from 'pg';

const item1 = {
  id: 'foo1',
  foo: 'foo ' + Math.random(),
  bar: 'bar ' + Math.random()
};

const collection = collectionOrRecordPathMeta[0].collectionOrRecordPath;

export function syncTaskValidatorsTests() {
  describe('FirebaseLiftPostgresSyncTool Sync Validator Tasks Basics', () => {
    const firestoreCollection = getFirebaseApp().firestore().collection(collection);

    test('Basic validator working', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();

      await firestoreCollection.doc(item1.id).set(item1);

      const startingErrors = getFirebaseLiftPostgresSyncTool().getStats().totalErrors;

      const { syncTask, syncTaskValidator } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
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

      tool.queueSyncTasks([syncTask]);
      await tool._waitUntilSyncQueueDrained();

      tool.queueSyncTaskValidator([syncTaskValidator]);

      await tool._waitUntilSyncValidatorQueueDrained();

      assert.deepEqual(startingErrors, tool.getStats().totalErrors);
    });

    otest('Missing postgres row after create/update', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();

      await firestoreCollection.doc(item1.id).set(item1);

      const startingErrors = tool.getStats().totalErrors;

      const { syncTask, syncTaskValidator } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
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

      tool.queueSyncTasks([syncTask]);
      await tool._waitUntilSyncQueueDrained();

      // Delete item which should cause an error
      await getPool1().query(`delete from mirror_${collection} where id = $1`, [item1.id]);

      // Let validator run
      tool.queueSyncTaskValidator([syncTaskValidator]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilSyncValidatorQueueDrained();
      // Confirm an error occured
      assert.deepEqual(startingErrors + 1, getFirebaseLiftPostgresSyncTool().getStats().totalErrors);

      // Confirm it healed the issue
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(item1));
    });

    /*

  1. Mirror data to postgres
  2. Incremental check if changes propigated to postgres
    * After each write see if criticle columns have changed
    * Add item to RTDB for check in several minutes
  3. Full check of changes to postgres

  4. Run derived checks

*/

    /* Race condition

  1. Change is waiting to be processed
  2. Doesn't match in firestore
  3. Wait for 1 minutes (assume that is more than enough time for the queue to finish processing)


  a) Missing in firestore
  b) Don't match
  c) Missing in postgres


  1) Fetch all items in firebase
  2) Process them one at a time
  3) Any out of sync get them a 1-2 minute delay and process it again
  4) Any still out of fix issue and record error



  */
  });
}
