import { execSync } from 'node:child_process';
import { existsSync, rmSync } from 'node:fs';
import path from 'node:path';
import { FlameConfig } from '../../src/api/config';

const SAMPLE_PROJECT = path.join(__dirname, 'fixtures', 'sample-project');
const GENERATED_FILE = path.join(SAMPLE_PROJECT, 'src', 'generated', 'firestore-indexes.ts');

describe('flameql generate', () => {
  beforeEach(() => {
    FlameConfig.reset();
    if (existsSync(GENERATED_FILE)) {
      rmSync(GENERATED_FILE);
    }
  });

  it('generates firestore-indexes.ts via npm and can be used with FlameConfig', async () => {
    expect(existsSync(GENERATED_FILE)).toBe(false);
    execSync('npm run generate', { cwd: SAMPLE_PROJECT, stdio: 'pipe' });
    expect(existsSync(GENERATED_FILE)).toBe(true);
    const { firestoreIndexes } = require(GENERATED_FILE);
    FlameConfig.setIndexes(firestoreIndexes);
    const usersIndexes = FlameConfig.indexManager.getIndexes('users');
    expect(usersIndexes.length).toBeGreaterThan(0);
    expect(usersIndexes[0].fields[0].fieldPath).toBe('status');
  });
});
