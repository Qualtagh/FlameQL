import * as admin from 'firebase-admin';

const PROJECT_ID = 'flameql-test';

process.env.FIRESTORE_EMULATOR_HOST = 'localhost:8080';
process.env.GCLOUD_PROJECT = PROJECT_ID;

if (admin.apps.length === 0) {
  admin.initializeApp({
    projectId: PROJECT_ID,
  });
}

export const db = admin.firestore();

export async function clearDatabase() {
  const collections = await db.listCollections();
  for (const collection of collections) {
    const snapshot = await collection.get();
    const batch = db.batch();
    for (const doc of snapshot.docs) {
      batch.delete(doc.ref);
    }
    await batch.commit();
  }
}
