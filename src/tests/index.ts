import { init } from './helpers';
import { setOptions } from 'nano-test-runner';
import { firestoreSyncTests } from './firestoreSync';

setOptions({ runPattern: 'serial', suppressConsole: true });

async function main() {
  await init();
  firestoreSyncTests();
}

main();
