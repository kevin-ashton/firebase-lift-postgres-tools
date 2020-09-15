import { describe, otest, run, test, xtest } from 'nano-test-runner';
import {
  reset,
  getFirebaseLiftPostgresSyncTool,
  getFirebaseApp,
  collectionOrRecordPathMeta,
  generateMockFirebaseChangeObject,
  getPool1,
  exampleObfuscateFn
} from './helpers';
import { FirebaseLiftPostgresSyncTool } from '../FirebaseLiftPostgresSyncTool';
import * as assert from 'assert';
import * as stable from 'json-stable-stringify';

const item1 = {
  id: 'foo1',
  foo: 'foo ' + Math.random(),
  bar: 'bar ' + Math.random()
};

const item1_Obfus = exampleObfuscateFn({ collectionOrRecordPath: 'person', item: item1 });

const collection = collectionOrRecordPathMeta[0].collectionOrRecordPath;
const rtdbRecordPath = collectionOrRecordPathMeta[1].collectionOrRecordPath;

export function syncTaskValidatorsTests() {
  describe('FirebaseLiftPostgresSyncTool Sync Validator Tasks Basics', () => {
    const firestoreCollection = getFirebaseApp().firestore().collection(collection);
    const rtdb = getFirebaseApp().database().ref(rtdbRecordPath);

    test('Basic Create/Update Validator', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      await firestoreCollection.doc(item1.id).set(item1);

      const startingErrors = getFirebaseLiftPostgresSyncTool().getStats().totalErrors;
      await firestoreCollection.doc(item1.id).set(item1);
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
      assert.deepStrictEqual(startingErrors, tool.getStats().totalErrors);
    });

    test('Basic delete Validator', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      await firestoreCollection.doc(item1.id).set(item1);
      const originalErrors = tool.getStats().totalErrors;

      await firestoreCollection.doc(item1.id).set(item1);
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

      await firestoreCollection.doc(item1.id).delete();
      const {
        syncTask: syncTask2,
        syncTaskValidator: syncTaskValidator2
      } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
        type: 'firestore',
        collectionOrRecordPath: 'person',
        firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
          itemIdOrKey: item1.id,
          type: 'delete',
          afterItem: undefined,
          beforeItem: undefined,
          dbType: 'firestore'
        })
      });
      tool.queueSyncTasks([syncTask2]);
      await tool._waitUntilSyncQueueDrained();

      tool.queueSyncTaskValidator([syncTaskValidator2]);
      await tool._waitUntilSyncValidatorQueueDrained();

      assert.deepStrictEqual(originalErrors, tool.getStats().totalErrors);

      // Confirm it healed the issue
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(r1.rows.length, 0);
    });

    test('Missing postgres row after create/update', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      await firestoreCollection.doc(item1.id).set(item1);
      const startingErrors = tool.getStats().totalErrors;
      await firestoreCollection.doc(item1.id).set(item1);
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
      assert.deepStrictEqual(startingErrors + 1, getFirebaseLiftPostgresSyncTool().getStats().totalErrors);
      // Confirm it healed the issue
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(item1_Obfus));
    });

    test('Multiple validators, different times, run out of order', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      await firestoreCollection.doc(item1.id).set(item1);
      const originalTotalSyncValidatorsTasksSkipped = getFirebaseLiftPostgresSyncTool().getStats()
        .totalSyncValidatorsTasksSkipped;

      await firestoreCollection.doc(item1.id).set(item1);
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

      const item1Update1 = { ...item1, ...{ update1: `foo - ${Math.random()}` } };
      await firestoreCollection.doc(item1.id).set(item1Update1);
      const {
        syncTask: syncTask2,
        syncTaskValidator: syncTaskValidator2
      } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
        type: 'firestore',
        collectionOrRecordPath: 'person',
        firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
          itemIdOrKey: item1.id,
          type: 'update',
          afterItem: item1Update1,
          beforeItem: item1,
          dbType: 'firestore'
        })
      });
      tool.queueSyncTasks([syncTask2]);
      await tool._waitUntilSyncQueueDrained();

      const item1Update2 = { ...item1, ...{ update2: `foo - ${Math.random()}` } };
      await firestoreCollection.doc(item1.id).set(item1Update2);
      const {
        syncTask: syncTask3,
        syncTaskValidator: syncTaskValidator3
      } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
        type: 'firestore',
        collectionOrRecordPath: 'person',
        firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
          itemIdOrKey: item1.id,
          type: 'update',
          afterItem: item1Update2,
          beforeItem: item1,
          dbType: 'firestore'
        })
      });
      tool.queueSyncTasks([syncTask3]);
      await tool._waitUntilSyncQueueDrained();

      tool.queueSyncTaskValidator([syncTaskValidator3]);
      await tool._waitUntilSyncValidatorQueueDrained();

      assert.deepStrictEqual(originalTotalSyncValidatorsTasksSkipped, tool.getStats().totalSyncValidatorsTasksSkipped);

      tool.queueSyncTaskValidator([syncTaskValidator2]);
      await tool._waitUntilSyncValidatorQueueDrained();

      assert.deepStrictEqual(
        originalTotalSyncValidatorsTasksSkipped + tool.getStats().totalMirrorPgs,
        tool.getStats().totalSyncValidatorsTasksSkipped
      );
    });

    test('Create/Update syncTask failed to run', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      await firestoreCollection.doc(item1.id).set(item1);
      const originalErrors = tool.getStats().totalErrors;

      await firestoreCollection.doc(item1.id).set(item1);
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

      const item1Update1 = { ...item1, ...{ update1: `foo - ${Math.random()}` } };
      const item1Update1_Obfus = exampleObfuscateFn({ collectionOrRecordPath: 'person', item: item1Update1 });
      await firestoreCollection.doc(item1.id).set(item1Update1);
      const {
        syncTask: syncTask2,
        syncTaskValidator: syncTaskValidator2
      } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
        type: 'firestore',
        collectionOrRecordPath: 'person',
        firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
          itemIdOrKey: item1.id,
          type: 'update',
          afterItem: item1Update1,
          beforeItem: item1,
          dbType: 'firestore'
        })
      });
      // normally where the sync task would run
      await new Promise((r) => setTimeout(() => r(), 200));

      tool.queueSyncTaskValidator([syncTaskValidator2]);
      await tool._waitUntilSyncValidatorQueueDrained();

      assert.deepStrictEqual(originalErrors + tool.getStats().totalMirrorPgs, tool.getStats().totalErrors);

      // Confirm it healed the issue
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(item1Update1_Obfus));

      let r2 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r2.rows[0].item), stable(item1Update1_Obfus));
    });

    test('Delete syncTask failed to run', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      await firestoreCollection.doc(item1.id).set(item1);
      const originalErrors = tool.getStats().totalErrors;

      await firestoreCollection.doc(item1.id).set(item1);
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

      await firestoreCollection.doc(item1.id).delete();
      const {
        syncTask: syncTask2,
        syncTaskValidator: syncTaskValidator2
      } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
        type: 'firestore',
        collectionOrRecordPath: 'person',
        firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
          itemIdOrKey: item1.id,
          type: 'delete',
          afterItem: undefined,
          beforeItem: undefined,
          dbType: 'firestore'
        })
      });

      tool.queueSyncTaskValidator([syncTaskValidator2]);
      await tool._waitUntilSyncValidatorQueueDrained();

      assert.deepStrictEqual(originalErrors + tool.getStats().totalMirrorPgs, tool.getStats().totalErrors);

      // Confirm it healed the issue
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(r1.rows.length, 0);
    });

    test('SyncTask ran but objects dont match', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      await firestoreCollection.doc(item1.id).set(item1);
      const originalErrors = tool.getStats().totalErrors;

      await firestoreCollection.doc(item1.id).set(item1);
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

      const item1Update1 = { ...item1, ...{ update1: `foo - ${Math.random()}` } };
      await getPool1().query(`update mirror_${collection} set item = $1 where id = $2`, [item1Update1, item1.id]);

      tool.queueSyncTaskValidator([syncTaskValidator]);
      await tool._waitUntilSyncValidatorQueueDrained();

      assert.deepStrictEqual(originalErrors + 1, tool.getStats().totalErrors);

      // Confirm it healed the issue
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepStrictEqual(stable(r1.rows[0].item), stable(item1_Obfus));
    });

    test('Race Condition: same item, correct order, multiple validators', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      const originalStats = tool.getStats();

      await firestoreCollection.doc(item1.id).set(item1);
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

      const item1Update1 = { ...item1, ...{ update1: `foo - ${Math.random()}` } };
      await firestoreCollection.doc(item1.id).set(item1Update1);
      const {
        syncTask: syncTask2,
        syncTaskValidator: syncTaskValidator2
      } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
        type: 'firestore',
        collectionOrRecordPath: 'person',
        firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
          itemIdOrKey: item1.id,
          type: 'update',
          afterItem: item1Update1,
          beforeItem: item1,
          dbType: 'firestore'
        })
      });

      tool.queueSyncTasks([syncTask, syncTask2]);
      await tool._waitUntilSyncQueueDrained();

      tool._registerSyncValidatorTaskDebugFn(async () => {
        await new Promise((r) => setTimeout(() => r(), 500));
      });
      tool.queueSyncTaskValidator([syncTaskValidator]);
      await new Promise((r) => setTimeout(() => r(), 100));
      assert.deepStrictEqual(tool.getStats().totalSyncValidatorTasksCurrentlyRunning, 1);

      tool.queueSyncTaskValidator([syncTaskValidator2]);
      await new Promise((r) => setTimeout(() => r(), 100));
      assert.deepStrictEqual(tool.getStats().totalSyncValidatorTasksPendingRetry, 1);

      await tool._waitUntilSyncValidatorQueueDrained();
      assert.deepStrictEqual(
        tool.getStats().totalSyncValidatorTasksCurrentlyRunning,
        originalStats.totalSyncValidatorTasksCurrentlyRunning
      );
      assert.deepStrictEqual(
        tool.getStats().totalSyncValidatorTasksPendingRetry,
        originalStats.totalSyncValidatorTasksPendingRetry
      );
      assert.deepStrictEqual(tool.getStats().totalErrors, originalStats.totalErrors);
    });

    test('Race Condition: same item, incorrect order, multiple validators', async () => {
      const tool = getFirebaseLiftPostgresSyncTool();
      await reset();
      const originalStats = tool.getStats();

      await firestoreCollection.doc(item1.id).set(item1);
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

      const item1Update1 = { ...item1, ...{ update1: `foo - ${Math.random()}` } };
      await firestoreCollection.doc(item1.id).set(item1Update1);
      const {
        syncTask: syncTask2,
        syncTaskValidator: syncTaskValidator2
      } = FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
        type: 'firestore',
        collectionOrRecordPath: 'person',
        firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
          itemIdOrKey: item1.id,
          type: 'update',
          afterItem: item1Update1,
          beforeItem: item1,
          dbType: 'firestore'
        })
      });

      tool.queueSyncTasks([syncTask, syncTask2]);
      await tool._waitUntilSyncQueueDrained();

      tool._registerSyncValidatorTaskDebugFn(async () => {
        await new Promise((r) => setTimeout(() => r(), 500));
      });
      tool.queueSyncTaskValidator([syncTaskValidator2]);
      await new Promise((r) => setTimeout(() => r(), 100));
      assert.deepStrictEqual(tool.getStats().totalSyncValidatorTasksCurrentlyRunning, 1);

      tool.queueSyncTaskValidator([syncTaskValidator]);
      await new Promise((r) => setTimeout(() => r(), 100));
      assert.deepStrictEqual(
        tool.getStats().totalSyncValidatorTasksPendingRetry,
        originalStats.totalSyncValidatorTasksPendingRetry
      );

      await tool._waitUntilSyncValidatorQueueDrained();
      assert.deepStrictEqual(
        tool.getStats().totalSyncValidatorTasksCurrentlyRunning,
        originalStats.totalSyncValidatorTasksCurrentlyRunning
      );
      assert.deepStrictEqual(
        tool.getStats().totalSyncValidatorTasksPendingRetry,
        originalStats.totalSyncValidatorTasksPendingRetry
      );
      assert.deepStrictEqual(tool.getStats().totalErrors, originalStats.totalErrors);
    });
  });
}
