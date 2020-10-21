# firebase-lift-postgres-tools
Unlocking the power of SQL queries for firebase

## Intro
Firebase has some amazing abilities with subscriptions, reasonable queries for a nosql solution, and ability to scale with no effort.

## Run & Test

Setup 2 postgres databases on your local machine. See the test config for database names and login details

```
  yarn install
  yarn start
  yarn ts-watch
  yarn test
```

## Notes

SyncTask - runs once
SyncTaskValidator - optional task that can be run sometime after the initial task to validate things

fullMirrorValidation - Runs on demand. Should be careful since it will burn through your firestore docs and realtime data

PreMirrorTransformFn
  * Should be deterministic (so we the validate works correctly)
  * Expects the item to be returned

PostMirrorHookFn
  * Will run after a SyncTask completes
  * Expected to be re-runnable


## Assumptions

* Assumes the docs will be compatible with `firebase-lift`.
* Assume firestore docs and rtdb nodes have an `id` property that mactches their docId or key.


## Limitations

* Designed to work with single server to process the syncTasks and validators. Hence, at an extremly large scale this would not work. If you have millions of records changed a minute this would not make sense.
