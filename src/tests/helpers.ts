import { clearFirestoreData } from '@firebase/testing';
import { FirebaseLiftPostgresSyncTool, CollectionOrRecordPathMeta } from '../FirebaseLiftPostgresSyncTool';
import * as fbAdmin from 'firebase-admin';
import * as pg from 'pg';

const testFirebaseConfig = { projectId: 'fir-lift', databaseURL: 'http://localhost:9000/?ns=fir-lift' };
export const collectionOrRecordPathMeta: CollectionOrRecordPathMeta[] = [
  { collectionOrRecordPath: 'person', source: 'firestore' },
  { collectionOrRecordPath: 'device', source: 'rtdb' }
];

let app: fbAdmin.app.App;
let pool1: pg.Pool;
let pool2: pg.Pool;

export function getPool1() {
  if (!pool1) {
    pool1 = new pg.Pool({ host: '127.0.0.1', database: 'example1', user: 'postgres', password: '', port: 5432 });
  }
  return pool1;
}

export function getPool2() {
  if (!pool2) {
    pool2 = new pg.Pool({ host: '127.0.0.1', database: 'example2', user: 'postgres', password: '', port: 5433 });
  }
  return pool2;
}

export function init() {
  app = fbAdmin.initializeApp(testFirebaseConfig);
  const db = app.firestore();
  db.settings({ host: 'localhost:8080', ssl: false });
}

export async function reset() {
  console.log('Reset and clear data');
  await clearFirestoreData({ projectId: testFirebaseConfig.projectId });

  const baseExampleTableNames = collectionOrRecordPathMeta.map((e) => e.collectionOrRecordPath);
  for (let i = 0; i < baseExampleTableNames.length; i++) {
    try {
      await getPool1().query(`truncate table mirror_${baseExampleTableNames[i]}`);
    } catch (e) {}
    try {
      await getPool2().query(`truncate table mirror_${baseExampleTableNames[i]}`);
    } catch (e) {}
    try {
      await getPool1().query(`truncate table audit_${baseExampleTableNames[i]}`);
    } catch (e) {}
    try {
      await getPool2().query(`truncate table audit_${baseExampleTableNames[i]}`);
    } catch (e) {}
  }
}

let tool: FirebaseLiftPostgresSyncTool;

export function getFirebaseLiftPostgresSyncTool() {
  if (!tool) {
    const db1 = { title: 'main_db', pool: getPool1() };
    const db2 = { title: 'backup_db', pool: getPool2() };
    tool = new FirebaseLiftPostgresSyncTool({
      mirrorsPgs: [db1, db2],
      auditPgs: [db2],
      collectionOrRecordPathMeta,
      errorHandler: (e) => {
        console.log('Error');
        console.log(e);
      },
      firestore: app.firestore(),
      rtdb: app.database(),
      syncQueueConcurrency: 10
    });
  }

  return tool;
}

export function generateMockFirebaseChangeObject(p: {
  beforeItem: any;
  afterItem: any;
  itemIdOrKey: string;
  dbType: 'firestore' | 'rtdb';
  type: 'create' | 'update' | 'delete';
}) {
  const dataFnName = p.dbType === 'firestore' ? 'data' : 'val';
  const idOrKeyName = p.dbType === 'firestore' ? 'id' : 'key';

  if (p.type === 'create') {
    return {
      before: { exists: false, [dataFnName]: () => null },
      after: { exists: true, [dataFnName]: () => ({ ...p.afterItem, ...{ [idOrKeyName]: p.itemIdOrKey } }) }
    };
  } else if (p.type === 'update') {
    return {
      before: { exists: true, [dataFnName]: () => ({ ...p.beforeItem, ...{ [idOrKeyName]: p.itemIdOrKey } }) },
      after: { exists: true, [dataFnName]: () => ({ ...p.afterItem, ...{ [idOrKeyName]: p.itemIdOrKey } }) }
    };
  } else if (p.type === 'delete') {
    return {
      before: { exists: true, [dataFnName]: () => ({ ...p.beforeItem, ...{ [idOrKeyName]: p.itemIdOrKey } }) },
      after: { exists: false, [dataFnName]: () => null }
    };
  } else {
    throw new Error('Invalid type for generateMockFirebaseChangeObject. Type: ' + p.type);
  }
}
