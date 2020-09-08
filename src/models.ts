export interface SyncTask {
  idOrKey: string;
  action: 'create' | 'update' | 'delete';
  collectionOrRecordPath: string;
  beforeItem: any;
  afterItem: any;
  dateMS: number;
}

export interface SyncTaskValidator {
  idOrKey: string;
  action: 'create' | 'update' | 'delete';
  collectionOrRecordPath: string;
  afterItem: any;
  dateMS: number;
}
