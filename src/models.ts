export type Action = 'create' | 'update' | 'delete';

export interface SyncTask {
  idOrKey: string;
  action: Action;
  collectionOrRecordPath: string;
  beforeItem: any;
  afterItem: any;
  dateMS: number;
}

export interface SyncTaskValidator {
  idOrKey: string;
  action: Action;
  collectionOrRecordPath: string;
  afterItem: any;
  dateMS: number;
}
