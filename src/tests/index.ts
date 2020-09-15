import { init } from './helpers';
import { setOptions } from 'nano-test-runner';
import { syncTasksTests } from './syncTasks';
import { syncTaskValidatorsTests } from './syncTaskValidators';
import { rtdbBasicTests } from './rtdb';
import { fullMirrorValidations } from './fullMirrorValidation';

setOptions({ runPattern: 'serial', suppressConsole: true });

async function main() {
  await init();
  syncTasksTests();
  syncTaskValidatorsTests();
  fullMirrorValidations();
  rtdbBasicTests();
}

main();
