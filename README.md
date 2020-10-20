# firebase-lift-postgres-tools
Unlocking the power of SQL queries for firebase

## Intro
Firebase has some amazing abilities with subscriptions, reasonable queries for a nosql solution, and ability to scale with no effort.

## Run & Test

Setup 2 postgres databases on your local machine. See the test config for database names and login details

```
  yarn install
  yarn start
  yarn test
```

##

SyncTask - runs once
SyncTaskValidator - optional task that can be run sometime after the initial task to validate things

.fullMirrorValidation - Runs on demand. Should be careful since it will burn through your firestore docs and realtime data

PreMirrorTransformFn
  * Should be deterministic (so we the validate works correctly)
  * Expects the item to be returned

PostMirrorHookFn
  * Will run after a SyncTask completes
  * Expected to be re-runnable


## TODO

* Comments in the code - will need this
* Readme with examples

* Ability to monitor the status of server (write to rtdb every 5 seconds or something like that)
* Need to make sure errors are actionable (this needs to be easy to use and keep error free)
* Ability to stop the server from processing to empty the queue so it can restart (maybe that belongs in the main server. Really just need a way to know if anything is still running)
* Can we validate N full collections at once
* How and when should we run our sync validator tasks
* If sync validation failed we should run any post hooks (so we can get other tables updated)


## Assumptions

* Assumes the docs will be compatible with `firebase-lift`.
* Assume firestore docs and rtdb nodes have an `id` property that mactches their docId or key.


## Limitations

* Designed to work with single server to process the syncTasks and validators. Hence, at an extremly large scale this would not work. If you have millions of records changed a minute this would not make sense.
