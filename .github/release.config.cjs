module.exports = {
  branches: [
    'main',
    { name: 'release-candidate', prerelease: 'rc' },
    { name: 'develop', prerelease: 'dev' }
  ],
  tagFormat: 'v${version}',
  plugins: [
    [
      '@semantic-release/commit-analyzer',
      {
        releaseRules: [
          { breaking: true, release: 'major' },
          { type: 'feat', release: 'minor' },
          { type: 'fix', release: 'patch' },
          { type: 'perf', release: 'patch' },
          { type: 'refactor', release: 'patch' },
          { type: 'revert', release: 'patch' },
          { type: 'security', release: 'patch' },
          { type: 'deps', release: 'patch' },
          { type: 'build', scope: 'deps', release: 'patch' },
          { type: 'chore', scope: 'deps', release: 'patch' },
          { type: 'docs', release: false },
          { type: 'style', release: false },
          { type: 'test', release: false },
          { type: 'build', release: false },
          { type: 'ci', release: false },
          { type: 'chore', release: false }
        ]
      }
    ],
    '@semantic-release/release-notes-generator',
    [
      '@semantic-release/exec',
      {
        prepareCmd: 'bash scripts/prepare-cargo-release.sh ${nextRelease.version}',
        publishCmd: 'bash scripts/publish-cargo-release.sh ${nextRelease.version}'
      }
    ],
    '@semantic-release/github'
  ]
};
