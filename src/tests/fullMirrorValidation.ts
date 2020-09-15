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

export function fullMirrorValidations() {
  describe('Full mirror validations', () => {
    test('foo', async () => {
      console.log('Hello');
    });
  });
}
