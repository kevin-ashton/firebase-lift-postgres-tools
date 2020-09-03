interface SyncTaskRoot {
  idOrKey: string;
  beforeItem: any;
  afterItem: any;
  dateMS: number;
  actionPerformedBy: string;
}

export interface SyncTaskFirestore extends SyncTaskRoot {
  type: 'firestore-create' | 'firestore-update' | 'firestore-delete';
  collection: string;
}

export interface SyncTaskRtdb extends SyncTaskRoot {
  type: 'rtdb-create' | 'rtdb-update' | 'rtdb-delete';
  recordPath: string;
}

export type SyncTask = SyncTaskFirestore | SyncTaskRtdb;
