#!/usr/bin/env node
import { runFlameqlCli } from './run';

runFlameqlCli(process.argv).catch((err: unknown) => {
  const message = err instanceof Error ? err.message : String(err);
  console.error(message);
  process.exitCode = 1;
});
