import * as admin from 'firebase-admin';
import { FirestoreIndexJson, IndexManager } from '../engine/indexes/index-manager';

class FlameConfigInstance {
  private _db: admin.firestore.Firestore | null = null;
  private _indexManager: IndexManager = new IndexManager();

  get db(): admin.firestore.Firestore {
    if (!this._db) {
      throw new Error('FlameConfig.db is not set. Call FlameConfig.setDb() first.');
    }
    return this._db;
  }

  get indexManager(): IndexManager {
    return this._indexManager;
  }

  setDb(db: admin.firestore.Firestore): void {
    this._db = db;
  }

  setIndexes(indexesJson: FirestoreIndexJson): void {
    this._indexManager = new IndexManager();
    this._indexManager.loadIndexes(indexesJson);
  }

  initialize(options: {
    db?: admin.firestore.Firestore;
    indexes?: FirestoreIndexJson;
  }): void {
    if (options.db) {
      this.setDb(options.db);
    }
    if (options.indexes) {
      this.setIndexes(options.indexes);
    }
  }

  /**
   * Resets the configuration. Useful for testing.
   */
  reset(): void {
    this._db = null;
    this._indexManager = new IndexManager();
  }
}

export const FlameConfig = new FlameConfigInstance();
