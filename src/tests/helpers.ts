import { clearFirestoreData } from '@firebase/testing';
import { FirebaseLiftPostgresSyncTool } from '../FirebaseLiftPostgresSyncTool';
import * as fbAdmin from 'firebase-admin';
import * as pg from 'pg';

const testFirebaseConfig = { projectId: 'fir-lift', databaseURL: 'http://localhost:9000/?ns=fir-lift' };
export const baseExampleTableNames = ['person', 'book', 'device'];

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
    tool = new FirebaseLiftPostgresSyncTool({
      mirrorsPgs: [getPool1(), getPool2()],
      auditPgs: [getPool2()],
      errorHandler: (e) => {
        console.log('Error');
        console.log(e);
      },
      baseTableNames: baseExampleTableNames,
      fbApp: app,
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
