import { firestore } from 'firebase-admin';
import { TableData } from './sqlite-runner';

export async function seedFirestore(db: firestore.Firestore, tables: TableData[]) {
  for (const table of tables) {
    if (!table.rows.length) continue;
    const batch = db.batch();
    let fallback = 0;

    for (const row of table.rows) {
      const data = { ...row };
      const docId = resolveDocId(data, fallback++);
      delete (data as any).__rowid__;
      batch.set(db.collection(table.name).doc(docId), data);
    }

    await batch.commit();
  }
}

function resolveDocId(row: Record<string, any>, fallback: number): string {
  if (row.id !== undefined && row.id !== null) return String(row.id);
  if (row.uid !== undefined && row.uid !== null) return String(row.uid);
  if (row.__rowid__ !== undefined && row.__rowid__ !== null) return String(row.__rowid__);
  return String(fallback);
}
