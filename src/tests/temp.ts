import { fetchAndProcessRtdbRecordPath } from '../collectionProcessor';
import { getFirebaseApp, init } from './helpers';

async function main() {
  init();
  const app = getFirebaseApp();

  const data: { age: number; name: string; id: string }[] = [
    {
      id: 'c',
      name: 'Kevin',
      age: 36
    },
    {
      id: 'a',
      name: 'Hazel',
      age: 1
    },
    {
      id: 'f',
      name: 'Karoline',
      age: 31
    },
    {
      id: 'b',
      name: 'Elaine',
      age: 5
    },
    {
      id: 'd',
      name: 'Henry',
      age: 3
    }
  ];

  for (let i = 0; i < data.length; i++) {
    const p = data[i];
    await app.database().ref(`person/${p.id}`).set(p);
  }

  await fetchAndProcessRtdbRecordPath({
    app,
    batchSize: 2,
    processFn: async (i) => {
      console.log('Process item');
      console.log(i);
    },
    processFnConcurrency: 1,
    recordPath: 'person'
  });

  // let val = await app.database().ref('person').limitToFirst(500).once('value');

  // val.forEach((b) => {
  //   console.log(b.val());
  // });

  // console.log('Done');
  process.exit(0);
}

main();
