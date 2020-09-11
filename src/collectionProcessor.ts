import * as admin from 'firebase-admin';
import { queue } from 'async';

export async function fetchAndProcessFirestoreCollection(p: {
  firestoreFetchBatchSize: number;
  app: admin.app.App;
  processFnConcurrency: number;
  collection: string;
  processFn: (item: any) => Promise<void>;
}) {
  console.log(`Fetch and process for ${p.collection} in firestore`);
  let lastDoc: any;

  let processQueue = queue<any>(async (task) => {
    try {
      await p.processFn(task);
    } catch (e) {
      console.error(
        `Error running processQueueFn for ${p.collection}. Please catch and handle errors in the processFn!`
      );
    }
  }, p.processFnConcurrency);

  while (true) {
    let q = p.app.firestore().collection(p.collection).orderBy('id', 'asc').limit(p.firestoreFetchBatchSize);

    if (lastDoc) {
      q = q.startAfter(lastDoc);
    }

    let r = await q.get();

    lastDoc = r.docs[r.docs.length - 1];

    r.forEach((doc) => {
      let item = doc.data();
      processQueue.push(item);
    });

    if (processQueue.length() > 0) {
      await processQueue.drain();
    }

    if (r.size !== p.firestoreFetchBatchSize) {
      break;
    }
  }
}

export async function fetchAndProcessRtdbRecordPath(p: {
  batchSize: number;
  app: admin.app.App;
  processFnConcurrency: number;
  recordPath: string;
  processFn: (item: any) => Promise<void>;
}) {
  console.log(`Fetch and process for ${p.recordPath} in RTDB`);

  if (p.batchSize < 3) {
    throw new Error('Batch size cannot be less than 3');
  }

  let lastDocKey: string | null = null;
  let processQueue = queue<any>(async (task) => {
    try {
      await p.processFn(task);
    } catch (e) {
      console.error(
        `Error running processQueueFn for ${p.recordPath}. Please catch and handle errors in the processFn!`
      );
    }
  }, p.processFnConcurrency);

  while (true) {
    let ref = p.app.database().ref(p.recordPath).orderByKey().limitToFirst(p.batchSize);
    if (lastDocKey) {
      ref = ref.startAt(lastDocKey);
    }

    const q = await ref.once('value');

    const docs: any[] = [];
    q.forEach((doc) => {
      // Skip doc if we already processed it
      if (lastDocKey === doc.key) {
        return;
      }
      lastDocKey = doc.key;
      docs.push(doc.val());
    });

    docs.forEach((doc) => {
      processQueue.push(doc);
    });

    if (processQueue.length() > 0) {
      await processQueue.drain();
    }

    if (q.numChildren() !== p.batchSize) {
      break;
    }
  }
}
