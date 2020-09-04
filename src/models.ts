export interface SyncTask {
  idOrKey: string;
  action: 'create' | 'update' | 'delete';
  collectionOrRecordPath: string;
  beforeItem: any;
  afterItem: any;
  dateMS: number;
}
