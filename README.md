# firebase-lift-postgres-tools
Unlocking the power of SQL queries for firebase

## Intro
Firebase has some amazing abilities with subscriptions, reasonable queries for a nosql solution, and ability to scale with no effort.


## Limitations

* Designed to work with single server to process the syncTasks and validators. Hence, at an extremly large scale this would not work. If you have millions of records changed a minute this would not make sense.
