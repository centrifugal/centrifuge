# Security Policy

## Reporting Security Vulnerabilities

Security reports for this library are handled together with the rest of the Centrifugal ecosystem, in the [Centrifugo](https://github.com/centrifugal/centrifugo) repository, where advisories are published.

If you discover a security vulnerability in this library, please report it through GitHub using the **“Report a vulnerability”** button in the Centrifugo repository’s [Security](https://github.com/centrifugal/centrifugo/security) tab, or directly via [this form](https://github.com/centrifugal/centrifugo/security/advisories/new). Reports submitted this way are visible only to maintainers. Mention that the report is about `centrifugal/centrifuge`, so that it can be triaged accordingly.

Please do **not** open a public GitHub issue for security-related problems.

When reporting a vulnerability, include as much detail as possible, such as:

* A description of the vulnerability
* Steps to reproduce
* Affected versions
* Potential impact

We will acknowledge receipt of the report and work to assess the issue promptly.

Since this library serves connections from untrusted clients, we are especially interested in reports about what a client can cause with the data it sends: panics, deadlocks, unbounded memory or goroutine growth, access to channels, publications or presence it is not allowed to see, and ways around authentication.

## Vulnerability Detection

Besides external reports, this library relies on:

* **Tests** – the test suite, with fuzz tests for the parsing of untrusted data, runs with the race detector on every push and pull request, against Redis and Valkey.
* **Static analysis and linters** – `golangci-lint` (including `gosec` and `govet`) runs on every push and pull request.
* **Dependency updates** – dependencies are monitored and updated via Dependabot.

## Triage and Assessment

Reported or detected vulnerabilities are reviewed by the project maintainers and assessed based on severity, exploitability, and the impact on applications using the library. Confirmed issues are fixed in a new release of this library, and the fix is propagated to [Centrifugo](https://github.com/centrifugal/centrifugo) by updating the dependency there.
