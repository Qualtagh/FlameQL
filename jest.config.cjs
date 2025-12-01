module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/tests/**/*.test.ts'],
  setupFilesAfterEnv: ['<rootDir>/tests/setup.ts'],
  transform: {
    '^.+\\.tsx?$': ['ts-jest', {
      diagnostics: {
        warnOnly: false,
      },
    }],
  },
};
