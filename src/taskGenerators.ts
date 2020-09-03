import { SyncTask, SyncTaskFirestore, SyncTaskRtdb } from './models';

export function generateSyncTaskFromFirestoreWriteTrigger(p: {
  collection: string;
  firestoreTriggerWriteChangeObject: any;
  firestoreTriggerWriteContextObject: any;
  dataScrubber?: (node: any) => Object;
}): SyncTask {
  const change = p.firestoreTriggerWriteChangeObject;
  const context = p.firestoreTriggerWriteContextObject;

  let action: SyncTaskFirestore['type'];

  if (change.after.exists && change.before.exists) {
    action = 'firestore-update';
  } else if (change.after.exists && !change.before.exists) {
    action = 'firestore-create';
  } else if (!change.after.exists && change.before.exists) {
    action = 'firestore-delete';
  } else {
    throw new Error('Unable to determine the action for generateSyncTaskFromFirestoreWriteTrigger');
  }

  let beforeItem = change.before.data();
  let afterItem = change.after.data();
  if (p.dataScrubber) {
    if (beforeItem) {
      beforeItem = p.dataScrubber(beforeItem);
    }
    if (afterItem) {
      afterItem = p.dataScrubber(afterItem);
    }
  }

  const task: SyncTask = {
    type: action,
    actionPerformedBy: context.authType === 'USER' ? context.auth?.uid || 'UNKNOWN' : context.authType,
    collection: p.collection,
    dateMS: Date.now(),
    idOrKey: !!beforeItem ? beforeItem.id : afterItem.id,
    beforeItem,
    afterItem
  };

  return task;
}

export function generateSyncTaskFromRtdbWriteTrigger(p: {
  recordPath: string;
  rtdbTriggerWriteChangeObject: any;
  rtdbTriggerWriteContextObject: any;
  dataScrubber?: (node: any) => Object;
}): SyncTask {
  const change = p.rtdbTriggerWriteChangeObject;
  const context = p.rtdbTriggerWriteContextObject;
  let action: SyncTaskRtdb['type'];

  if (change.after.exists && change.before.exists) {
    action = 'rtdb-update';
  } else if (change.after.exists && !change.before.exists) {
    action = 'rtdb-create';
  } else if (!change.after.exists && change.before.exists) {
    action = 'rtdb-delete';
  } else {
    throw new Error('Unable to determine the action for generateSyncTaskFromFirestoreWriteTrigger');
  }

  let beforeItem = change.before.val();
  let afterItem = change.after.val();

  if (p.dataScrubber) {
    if (beforeItem) {
      beforeItem = p.dataScrubber(beforeItem);
    }
    if (afterItem) {
      afterItem = p.dataScrubber(afterItem);
    }
  }

  const task: SyncTask = {
    type: action,
    actionPerformedBy: context.authType === 'USER' ? context.auth?.uid || 'UNKNOWN' : context.authType,
    recordPath: p.recordPath,
    dateMS: Date.now(),
    idOrKey: !!beforeItem ? beforeItem.key : afterItem.key,
    beforeItem,
    afterItem
  };

  return task;
}
