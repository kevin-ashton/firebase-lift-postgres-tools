import { describe, otest, run, test, xtest } from 'nano-test-runner';
import {
  reset,
  getFirebaseLiftPostgresSyncTool,
  getFirebaseApp,
  collectionOrRecordPathsMeta,
  generateMockFirebaseChangeObject,
  getPool2,
  exampleTransformFn,
  getPostMirrorHasRunNTimes
} from './helpers';
import { FirebaseLiftPostgresSyncTool } from '../FirebaseLiftPostgresSyncTool';
import * as assert from 'assert';
import * as stable from 'json-stable-stringify';

const item1 = {
  id: 'foo1',
  foo: 'foo ' + Math.random(),
  bar: 'bar ' + Math.random()
};

const item1_transformed = exampleTransformFn({ collectionOrRecordPath: 'person', item: item1 });

const collection = collectionOrRecordPathsMeta[0].collectionOrRecordPath;
const rtdbRecordPath = collectionOrRecordPathsMeta[1].collectionOrRecordPath;

export function auditTrimTests() {
  describe('Audit trim tests', () => {
    const firestoreCollection = getFirebaseApp().firestore().collection(collection);
    const rtdb = getFirebaseApp().database().ref(rtdbRecordPath);

    test('Basic Create/Update Validator', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      await firestoreCollection.doc(item1.id).set(item1);

      const startingErrors = getFirebaseLiftPostgresSyncTool().getStats().totalErrors;
      await firestoreCollection.doc(item1.id).set(item1);
      const { syncTask } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
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

      let r1 = await getPool2().query(`select * from audit_person`);
      assert.deepStrictEqual(1, r1.rowCount);

      await tool.trimOldAudits({ collectionsOrRecordPaths: ['person'], daysToRetain: 90, progress: () => {} });

      // Since it hasn't been 90 days the audit item should still be there
      let r2 = await getPool2().query(`select * from audit_person`);
      assert.deepStrictEqual(1, r2.rowCount);

      // Manually age the audit record
      await getPool2().query(`update audit_person set recorded_at = (NOW() - INTERVAL '100 days')`);

      await tool.trimOldAudits({ collectionsOrRecordPaths: ['person'], daysToRetain: 90, progress: () => {} });

      let r3 = await getPool2().query(`select * from audit_person`);
      assert.deepStrictEqual(0, r3.rowCount);

      assert.deepStrictEqual(startingErrors, tool.getStats().totalErrors);
    });
  });
}
