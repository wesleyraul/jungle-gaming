import type { Config } from 'jest';

const baseProject = {
  moduleFileExtensions: ['js', 'json', 'ts'],
  transform: {
    '^.+\\.(t|j)s$': ['ts-jest', { tsconfig: 'tsconfig.json' }] as [string, Record<string, unknown>],
  },
  testEnvironment: 'node',
};

const config: Config = {
  rootDir: '.',
  projects: [
    {
      ...baseProject,
      displayName: 'unit',
      testMatch: ['<rootDir>/src/**/*.spec.ts'],
    },
    {
      ...baseProject,
      displayName: 'integration',
      testMatch: ['<rootDir>/test/integration/**/*.integration.spec.ts'],
    },
    {
      ...baseProject,
      displayName: 'concurrency',
      testMatch: ['<rootDir>/test/concurrency/**/*.concurrency.spec.ts'],
    },
  ],
};

export default config;
