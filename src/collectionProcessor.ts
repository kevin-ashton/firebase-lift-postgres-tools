import * as admin from 'firebase-admin';
import { queue } from 'async';

// Function to process every document in a firestore collection
// Keep in mind that documents while this is running may not be included.
export async function fetchAndProcessFirestoreCollection(p: {
  firestoreFetchBatchSize: number;
  firestore: admin.firestore.Firestore;
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
    let q = p.firestore.collection(p.collection).orderBy('id', 'asc').limit(p.firestoreFetchBatchSize);

    if (lastDoc) {
      q = q.startAfter(lastDoc);
    }

    let r = await q.get();

    lastDoc = r.docs[r.docs.length - 1];

    r.forEach((doc) => {
      let item = doc.data();
      if (doc.id !== item.id) {
        console.warn(
          `Firestore doc id does not match the id property on the actual document. Skipping doc. DocId: ${doc.id}. Id Property Value: ${item.id} `
        );
      } else {
        processQueue.push(item);
      }
    });

    if (processQueue.length() > 0) {
      await processQueue.drain();
    }

    if (r.size !== p.firestoreFetchBatchSize) {
      break;
    }
  }
}

// Function to process every document in a realtime database node
// Keep in mind that nodes while this is running may not be included.
export async function fetchAndProcessRtdbRecordPath(p: {
  rtdbBatchSize: number;
  rtdb: admin.database.Database;
  processFnConcurrency: number;
  recordPath: string;
  processFn: (item: any) => Promise<void>;
}) {
  console.log(`Fetch and process for ${p.recordPath} in RTDB`);

  if (p.rtdbBatchSize < 2) {
    throw new Error('Batch size cannot be less than 2');
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
    let ref = p.rtdb.ref(p.recordPath).orderByKey().limitToFirst(p.rtdbBatchSize);
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

      let data = doc.val();

      if (data.id !== lastDocKey) {
        console.warn(
          `RTDB doc id does not match the id property on the actual document. Skipping doc. LastKeyId: ${lastDocKey}. Id Property Value: ${data.id} `
        );
      } else {
        docs.push(data);
      }
    });

    docs.forEach((doc) => {
      processQueue.push(doc);
    });

    if (processQueue.length() > 0) {
      await processQueue.drain();
    }

    if (q.numChildren() !== p.rtdbBatchSize) {
      break;
    }
  }
}
