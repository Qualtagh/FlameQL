# FlameQL

## CLI

FlameQL ships with a small CLI binary named `flameql`.

### generate

Generates a TypeScript module which exports the contents of `firestore.indexes.json`.
This is useful for Cloud Functions / triggers builds where `firestore.indexes.json` is not bundled by default.

By default the CLI auto-guesses `firestore.indexes.json` by searching up from the current working directory and checking:

- `./firestore.indexes.json`
- `./functions/firestore.indexes.json`

And writes the generated file into `src/generated` relative to the located `firestore.indexes.json`.

Examples:

```bash
# Auto-guess indexes json + default output dir
npx flameql generate

# Explicit paths
npx flameql generate --indexes-json ./firestore.indexes.json --output-dir ./functions/src/generated
```

Output:

- `<output-dir>/firestore-indexes.ts`
