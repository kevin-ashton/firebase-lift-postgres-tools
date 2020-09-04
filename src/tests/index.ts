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

    run(async () => {
      await reset();
      let task = generateSyncTaskFromWriteTrigger({
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

      getFirebaseLiftPostgresSyncTool().queueSyncTasks([task]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();
    });

    test('Create item into mirror/audit tables', async () => {
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(item1));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r2.rows[0].item), stable(item1));
      let r3 = await getPool2().query(`select * from audit_person where afteritem->>'id' = $1`, [item1.id]);
      assert.deepEqual(stable(r3.rows[0].afteritem), stable(item1));
      assert.deepEqual(stable(r3.rows[0].beforeitem), stable({}));
    });

    let updatedItem = { ...item1, ...{ foo: 'foo ' + Math.random() } };
    run(async () => {
      let task = generateSyncTaskFromWriteTrigger({
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
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([task]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();
    });

    test('Update item into mirror/audit tables', async () => {
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r1.rows[0].item), stable(updatedItem));
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(stable(r2.rows[0].item), stable(updatedItem));
      let r3 = await getPool2().query(
        `select * from audit_person where afteritem->>'id' = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepEqual(stable(r3.rows[0].beforeitem), stable(item1));
      assert.deepEqual(stable(r3.rows[0].afteritem), stable(updatedItem));
    });

    run(async () => {
      let task = generateSyncTaskFromWriteTrigger({
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
      getFirebaseLiftPostgresSyncTool().queueSyncTasks([task]);
      await getFirebaseLiftPostgresSyncTool()._waitUntilQueueDrained();
      await new Promise((r) => setTimeout(() => r(), 1000));
    });

    test('Delete item into mirror/audit tables', async () => {
      let r1 = await getPool1().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(r1.rows.length, 0);
      let r2 = await getPool2().query('select * from mirror_person where id = $1', [item1.id]);
      assert.deepEqual(r2.rows.length, 0);
      let r3 = await getPool2().query(
        `select * from audit_person where beforeitem->>'id' = $1 order by recorded_at desc limit 1`,
        [item1.id]
      );
      assert.deepEqual(stable(r3.rows[0].beforeitem), stable(updatedItem));
      assert.deepEqual(stable(r3.rows[0].afteritem), stable({}));
    });
  });
}

main();
