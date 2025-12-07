const commonRules = {
  'no-extra-parens': ['error', 'all', {
    conditionalAssign: true,
    ternaryOperandBinaryExpressions: true,
    nestedBinaryExpressions: true,
    ignoreJSX: 'all',
    enforceForSequenceExpressions: true,
  }],
  'comma-dangle': ['error', {
    arrays: 'always-multiline',
    objects: 'always-multiline',
    imports: 'always-multiline',
    exports: 'always-multiline',
    functions: 'never',
  }],
  quotes: ['error', 'single', { avoidEscape: true, allowTemplateLiterals: true }],
  'quote-props': ['error', 'as-needed'],
  'no-multiple-empty-lines': ['error', { max: 1, maxEOF: 1, maxBOF: 0 }],
  'no-trailing-spaces': 'error',
  'eol-last': ['error', 'always'],
};

module.exports = [
  {
    ignores: ['node_modules/**', 'dist/**'],
  },
  {
    files: ['**/*.ts'],
    languageOptions: {
      parser: require('@typescript-eslint/parser'),
      parserOptions: {
        project: ['./tsconfig.json', './tsconfig.test.json'],
        tsconfigRootDir: __dirname,
        sourceType: 'module',
      },
    },
    plugins: {
      '@typescript-eslint': require('@typescript-eslint/eslint-plugin'),
    },
    rules: {
      ...commonRules,
      '@typescript-eslint/no-deprecated': 'error',
      '@typescript-eslint/no-unnecessary-type-assertion': 'error',
    },
  },
  {
    files: ['**/*.cjs'],
    rules: {
      ...commonRules,
    },
  },
];
