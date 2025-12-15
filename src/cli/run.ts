import { Command } from 'commander';
import { generateFirestoreIndexesTsModule } from './generate';

export function createFlameqlProgram(): Command {
  const program = new Command();

  program
    .name('flameql')
    .description('FlameQL CLI');

  program
    .command('generate')
    .description('Generate a TS module exporting firestore.indexes.json')
    .option(
      '--indexes-json <path>',
      'Path to firestore.indexes.json (auto-guessed if omitted)'
    )
    .option(
      '--output-dir <path>',
      'Directory for generated files (default: src/generated relative to firestore.indexes.json)'
    )
    .action(async (options: { indexesJson?: string; outputDir?: string }) => {
      const result = await generateFirestoreIndexesTsModule({
        cwd: process.cwd(),
        indexesJson: options.indexesJson,
        outputDir: options.outputDir,
      });

      // Keep the output simple for scriptability.
      // (Users can redirect or parse it easily.)
      console.log(result.outputFile);
    });

  return program;
}

export async function runFlameqlCli(argv: string[]): Promise<void> {
  await createFlameqlProgram().parseAsync(argv);
}
