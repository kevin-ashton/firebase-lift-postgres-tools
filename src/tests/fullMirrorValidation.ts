import { describe, otest, run, test, xtest } from 'nano-test-runner';
import {
  reset,
  getFirebaseLiftPostgresSyncTool,
  getFirebaseApp,
  collectionOrRecordPathsMeta,
  generateMockFirebaseChangeObject,
  getPool1,
  getPool2
} from './helpers';
import { FirebaseLiftPostgresSyncTool } from '../FirebaseLiftPostgresSyncTool';
import * as assert from 'assert';
import * as stable from 'json-stable-stringify';

interface Foo {
  id: string;
  foo: string;
  bar: number;
}

const persons: Foo[] = [];
const devices: Foo[] = [];

async function resetLocal() {
  await reset();
  for (let i = 0; i < 5; i++) {
    const now = Date.now();
    persons.push({ id: `${now}${Math.round(Math.random() * 1000000)}`, foo: 'foo', bar: Math.random() });
    devices.push({ id: `${now}${Math.round(Math.random() * 1000000)}`, foo: 'foo', bar: Math.random() });
  }

  const personTasks = persons.map((i) => {
    return FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
      type: 'firestore',
      collectionOrRecordPath: 'person',
      firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
        itemIdOrKey: i.id,
        type: 'create',
        afterItem: i,
        beforeItem: undefined,
        dbType: 'firestore'
      })
    });
  });

  const deviceTasks = devices.map((i) => {
    return FirebaseLiftPostgresSyncTool.generateSyncTaskFromWriteTrigger({
      type: 'firestore',
      collectionOrRecordPath: 'device',
      firestoreTriggerWriteChangeObject: generateMockFirebaseChangeObject({
        itemIdOrKey: i.id,
        type: 'create',
        afterItem: i,
        beforeItem: undefined,
        dbType: 'firestore'
      })
    });
  });
  getFirebaseLiftPostgresSyncTool().queueSyncTasks(personTasks.map((t) => t.syncTask));
  getFirebaseLiftPostgresSyncTool().queueSyncTasks(deviceTasks.map((t) => t.syncTask));
  Promise.all(
    personTasks.map(async (t) => {
      const item = t.syncTask.afterItem;
      await getFirebaseApp().firestore().collection('person').doc(item.id).set(item);
    })
  );
  Promise.all(
    deviceTasks.map(async (t) => {
      const item = t.syncTask.afterItem;
      await getFirebaseApp().database().ref(`device/${item.id}`).set(item);
    })
  );
  await getFirebaseLiftPostgresSyncTool()._waitUntilSyncQueueDrained();
  getFirebaseLiftPostgresSyncTool().queueSyncTaskValidator(personTasks.map((t) => t.syncTaskValidator));
  getFirebaseLiftPostgresSyncTool().queueSyncTaskValidator(deviceTasks.map((t) => t.syncTaskValidator));
  await getFirebaseLiftPostgresSyncTool()._waitUntilSyncValidatorQueueDrained();
}

export function fullMirrorValidations() {
  describe('Full mirror validations', () => {
    test('Large dataset with no errors', async () => {
      await resetLocal();

      const r = await getFirebaseLiftPostgresSyncTool().fullMirrorValidation({
        batchSize: 20,
        collectionsOrRecordPaths: ['device', 'person'],
        progressLogger: (p) => {
          console.log(p);
        },
        validationErrorLogger: (e) => {
          console.log('error');
        }
      });

      if (r.status === 'failed') {
        throw new Error('Failed to run correctly');
      }
      assert.deepStrictEqual(r.totalErrors, 0);
      assert.deepStrictEqual(r.totalDocsOrNodesProcessed, persons.length + devices.length);
      assert.deepStrictEqual(
        r.totalRowsProcessed,
        (persons.length + devices.length) * getFirebaseLiftPostgresSyncTool().getStats().totalMirrorPgs
      );
      assert.deepStrictEqual(
        r.validationResults.ITEMS_WERE_IN_EXPECTED_STATE,
        (persons.length + devices.length) * getFirebaseLiftPostgresSyncTool().getStats().totalMirrorPgs
      );
    });

    test('Missing item in firestore mirror', async () => {
      await resetLocal();

      await getPool1().query('delete from mirror_person where id = $1', [persons[0].id]);

      const r = await getFirebaseLiftPostgresSyncTool().fullMirrorValidation({
        batchSize: 20,
        collectionsOrRecordPaths: ['person'],
        progressLogger: (p) => {
          console.log(p);
        },
        validationErrorLogger: (e) => {
          console.log('error');
        }
      });

      let r2 = await getPool1().query('select * from mirror_person where id = $1', [persons[0].id]);
      if (r.status === 'failed') {
        throw new Error('Failed to run correctly');
      }

      assert.deepStrictEqual(r.totalErrors, 1);
      assert.deepStrictEqual(r.validationResults.ITEM_WAS_MISSING_IN_MIRROR, 1);

      // Make sure it self healed
      assert.deepStrictEqual(r2.rows.length, 1);
    });

    test('Missing item in rtdb mirror', async () => {
      await resetLocal();

      await getPool1().query('delete from mirror_device where id = $1', [devices[0].id]);

      const r = await getFirebaseLiftPostgresSyncTool().fullMirrorValidation({
        batchSize: 20,
        collectionsOrRecordPaths: ['device'],
        progressLogger: (p) => {
          console.log(p);
        },
        validationErrorLogger: (e) => {
          console.log('error');
        }
      });

      let r2 = await getPool1().query('select * from mirror_device where id = $1', [devices[0].id]);
      if (r.status === 'failed') {
        throw new Error('Failed to run correctly');
      }

      assert.deepStrictEqual(r.totalErrors, 1);
      assert.deepStrictEqual(r.validationResults.ITEM_WAS_MISSING_IN_MIRROR, 1);

      // Make sure it self healed
      assert.deepStrictEqual(r2.rows.length, 1);
    });

    test('Items dont match in firestore mirror', async () => {
      await resetLocal();

      await getPool1().query('update mirror_person set item = $1 where id = $2', [{ bad: 'data' }, persons[0].id]);

      const r = await getFirebaseLiftPostgresSyncTool().fullMirrorValidation({
        batchSize: 20,
        collectionsOrRecordPaths: ['person'],
        progressLogger: (p) => {
          console.log(p);
        },
        validationErrorLogger: (e) => {
          console.log('error');
        }
      });

      let r2 = await getPool1().query('select * from mirror_person where id = $1', [persons[0].id]);
      if (r.status === 'failed') {
        throw new Error('Failed to run correctly');
      }

      assert.deepStrictEqual(r.totalErrors, 1);
      assert.deepStrictEqual(r.validationResults.ITEMS_DID_NOT_MATCH, 1);

      // Make sure it self healed
      assert.deepStrictEqual(r2.rows.length, 1);
    });

    test('Items dont match in rtdb mirror', async () => {
      await resetLocal();

      await getPool1().query('update mirror_device set item = $1 where id = $2', [{ bad: 'data' }, devices[0].id]);

      const r = await getFirebaseLiftPostgresSyncTool().fullMirrorValidation({
        batchSize: 20,
        collectionsOrRecordPaths: ['device'],
        progressLogger: (p) => {
          console.log(p);
        },
        validationErrorLogger: (e) => {
          console.log('error');
        }
      });

      let r2 = await getPool1().query('select * from mirror_device where id = $1', [devices[0].id]);
      if (r.status === 'failed') {
        throw new Error('Failed to run correctly');
      }

      assert.deepStrictEqual(r.totalErrors, 1);
      assert.deepStrictEqual(r.validationResults.ITEMS_DID_NOT_MATCH, 1);

      // Make sure it self healed
      assert.deepStrictEqual(r2.rows.length, 1);
    });

    test('Extra row in pg for firestore mirror', async () => {
      await resetLocal();

      await getFirebaseApp().firestore().collection('person').doc(persons[0].id).delete();

      const r = await getFirebaseLiftPostgresSyncTool().fullMirrorValidation({
        batchSize: 20,
        collectionsOrRecordPaths: ['person'],
        progressLogger: (p) => {
          console.log(p);
        },
        validationErrorLogger: (e) => {
          console.log('error');
        }
      });

      console.log(JSON.stringify(r, null, 2));

      let r2 = await getPool1().query('select * from mirror_person where id = $1', [persons[0].id]);
      if (r.status === 'failed') {
        throw new Error('Failed to run correctly');
      }

      assert.deepStrictEqual(r.totalErrors, 2);
      assert.deepStrictEqual(r.validationResults.ITEM_WAS_NOT_DELETED_IN_MIRROR, 2);

      // // Make sure it self healed
      assert.deepStrictEqual(r2.rows.length, 0);
    });

    test('Extra row in pg for rtdb mirror', async () => {
      await resetLocal();

      await getFirebaseApp().database().ref(`device/${devices[0].id}`).remove();

      const r = await getFirebaseLiftPostgresSyncTool().fullMirrorValidation({
        batchSize: 20,
        collectionsOrRecordPaths: ['device'],
        progressLogger: (p) => {
          console.log(p);
        },
        validationErrorLogger: (e) => {
          console.log('error');
        }
      });

      let r2 = await getPool1().query('select * from mirror_device where id = $1', [devices[0].id]);
      if (r.status === 'failed') {
        throw new Error('Failed to run correctly');
      }

      assert.deepStrictEqual(r.totalErrors, 2);
      assert.deepStrictEqual(r.validationResults.ITEM_WAS_NOT_DELETED_IN_MIRROR, 2);

      // // Make sure it self healed
      assert.deepStrictEqual(r2.rows.length, 0);
    });
  });
}
