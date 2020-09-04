import { SyncTask } from './models';

export function generateSyncTaskFromWriteTrigger(p: {
  type: 'firestore' | 'rtdb';
  collectionOrRecordPath: string;
  firestoreTriggerWriteChangeObject: any;
  dataScrubber?: (node: any) => Object;
}): SyncTask {
  const change = p.firestoreTriggerWriteChangeObject;

  let action: SyncTask['action'];

  if (change.after.exists && change.before.exists) {
    action = 'update';
  } else if (change.after.exists && !change.before.exists) {
    action = 'create';
  } else if (!change.after.exists && change.before.exists) {
    action = 'delete';
  } else {
    throw new Error('Unable to determine the action for generateSyncTaskFromWriteTrigger');
  }

  let beforeItem = p.type === 'firestore' ? change.before.data() : change.before.val();
  let afterItem = p.type === 'firestore' ? change.after.data() : change.after.val();
  if (p.dataScrubber) {
    if (beforeItem) {
      beforeItem = p.dataScrubber(beforeItem);
    }
    if (afterItem) {
      afterItem = p.dataScrubber(afterItem);
    }
  }

  let idOrKey = '';
  if (p.type === 'firestore') {
    idOrKey = !!beforeItem ? beforeItem.id : afterItem.id;
  } else {
    idOrKey = !!beforeItem ? beforeItem.key : afterItem.key;
  }

  if (!idOrKey) {
    throw new Error('Unable to generate sync task! Cannot find idOrKey!');
  }

  const task: SyncTask = {
    type: p.type,
    action: action,
    collectionOrRecordPath: p.collectionOrRecordPath,
    dateMS: Date.now(),
    idOrKey,
    beforeItem,
    afterItem
  };

  return task;
}
