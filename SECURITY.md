# Security

## Reporting a vulnerability

Please **do not** file a public GitHub issue for security issues.

Email `security@foxj77.dev` (or open a [GitHub Security Advisory](https://github.com/foxj77/autonomous-monitor/security/advisories/new) for a private channel) with:

- A description of the vulnerability
- Reproduction steps or a proof-of-concept
- Affected versions (commit SHA or tag)
- Impact assessment

You should receive an acknowledgement within 72 hours. Critical issues are prioritised; we aim to ship a fix or mitigation within 7 days for high-severity issues and 30 days for medium / low.

## Supported versions

The latest minor release receives security fixes. Older minors are not patched unless explicitly requested.

## Supply chain

- The container image is signed with [`cosign`](https://docs.sigstore.dev/) using keyless OIDC signing
- An SBOM is generated for every release in SPDX JSON format
- Dependencies are tracked via Dependabot
- CI runs `govulncheck` and CodeQL on every PR
