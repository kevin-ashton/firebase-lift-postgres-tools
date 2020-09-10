import { init } from './helpers';
import { setOptions } from 'nano-test-runner';
import { syncTasksTests } from './syncTasks';
import { syncTaskValidatorsTests } from './syncTaskValidators';

setOptions({ runPattern: 'serial', suppressConsole: true });

async function main() {
  await init();
  syncTasksTests();
  syncTaskValidatorsTests();
}

main();
