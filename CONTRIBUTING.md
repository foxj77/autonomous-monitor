# Contributing

Thanks for your interest in contributing to autonomous-monitor.

## Bug reports and feature requests

Please open a GitHub issue. Use the templates provided. Include enough context for someone unfamiliar with your cluster to reproduce the problem:

- Kubernetes version
- How the monitor is deployed (Helm, kustomize, raw manifests)
- Relevant configuration (env vars, suppressed findings, etc.)
- A `Finding` JSON from your broker (anonymize names if you want)
- The `autonomous_monitor_poll_duration_seconds` p99 over the last hour if it's a performance issue

## Pull requests

1. Fork the repository
2. Create a feature branch from `main` (e.g. `feat/new-check-family`)
3. Make your change
4. Add or update tests — the test suite must pass with `-race`
5. Run `golangci-lint run` — no new warnings
6. Sign off your commits (`git commit -s`) — see [DCO](#developer-certificate-of-origin) below
7. Open a pull request against `main`

PRs that touch the Finding JSON contract need a note in the PR description explaining the change. Consumers depend on the schema being stable; once we cut a `v1.0.0` we will commit to compatibility.

## Local development

```bash
# fetch deps
go mod download

# run tests
go test -race -count=1 ./...

# lint
golangci-lint run

# build (requires librdkafka)
CGO_ENABLED=1 go build -tags musl -o autonomous-monitor .
```

If you don't have `librdkafka` installed, the easiest way to build is via the provided `Dockerfile`:

```bash
docker build -t autonomous-monitor:dev .
```

## Adding a new check family

Check families are plain methods on `*Monitor` in `checks.go`. Each returns a `checkResult` and registers a counter via the helpers in `metrics.go`. To add one:

1. Add a bool to the `CheckConfig` struct in `config.go` and a default in `LoadConfig`
2. Add a new env var like `CHECK_MY_THING_ENABLED` in the `Checks:` literal
3. Wire the new check into `Monitor.Poll` in `monitor.go`
4. Add tests using the existing `newTestMonitor` helper in `checks_test.go`
5. Document the new env var in the README

## Release process

Maintainers push a `vX.Y.Z` tag. The release workflow builds, signs, attaches binaries, and publishes the container image. Container images are immutable per tag.

## Developer Certificate of Origin

By contributing, you agree to the [Developer Certificate of Origin (DCO)](https://developercertificate.org/). Each commit must be signed off (`git commit -s`). This is a lightweight mechanism to confirm you have the right to submit the contribution under the project's license.

## Code of conduct

This project follows the [Contributor Covenant](./CODE_OF_CONDUCT.md). Be patient with newcomers and kind in code review.
