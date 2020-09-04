export interface SyncTask {
  idOrKey: string;
  type: 'firestore' | 'rtdb';
  action: 'create' | 'update' | 'delete';
  collectionOrRecordPath: string;
  beforeItem: any;
  afterItem: any;
  dateMS: number;
}
