import { describe, otest, run, test, xtest } from 'nano-test-runner';
import { reset } from './helpers';

export function firestoreEnsureSync() {
  run(async () => {
    await reset();

    const items = [];
  });

  /*

  1. Mirror data to postgres
  2. Incremental check if changes propigated to postgres
    * After each write see if criticle columns have changed
    * Add item to RTDB for check in several minutes
  3. Full check of changes to postgres

  4. Run derived checks

*/

  /* Race condition

  1. Change is waiting to be processed
  2. Doesn't match in firestore
  3. Wait for 1 minutes (assume that is more than enough time for the queue to finish processing)


  a) Missing in firestore
  b) Don't match
  c) Missing in postgres


  1) Fetch all items in firebase
  2) Process them one at a time
  3) Any out of sync get them a 1-2 minute delay and process it again
  4) Any still out of fix issue and record error



  */
}
