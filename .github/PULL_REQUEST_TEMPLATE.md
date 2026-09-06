# Description

<!-- What this changes and why. If it fixes a bug, describe the behaviour before and after. -->

## Related issue

<!-- Jira key such as BKRS-000, and "Closes #000" for a GitHub issue. -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Performance
- [ ] Refactor, no behaviour change
- [ ] Documentation or repository housekeeping

## API compatibility

- [ ] No exported name changed
- [ ] An exported name changed, and the change is described above and noted in `CHANGELOG.md`

## Checklist

- [ ] The target branch is `dev`
- [ ] Tests cover the change, and `make test` passes locally
- [ ] `make mocks-generate` was run if an interface changed, and the result is committed
- [ ] New files carry the Apache 2.0 header
- [ ] `CHANGELOG.md` has an entry under `[Unreleased]`, unless the change is invisible to users
- [ ] Downstream consumers still compile: Aerospike Backup Service and backup-cli

## How this was tested

<!-- Commands you ran, and the environment. Note whether integration tests were run and
     against which services. -->
