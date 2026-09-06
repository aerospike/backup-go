# Security Policy

## Supported versions

This module is at `v0.x` and makes no API compatibility promise. Only the latest released
tag receives security fixes; there are no maintenance branches for older tags.

| Version | Supported |
| --- | --- |
| `v0.11.1` (latest tag) | :white_check_mark: |
| Any earlier tag | :x: |

If you are on an older tag, upgrade to the latest one to pick up a fix. Check the
[releases page](https://github.com/aerospike/backup-go/releases) for the current tag.

## Reporting a vulnerability

**Do not open a public GitHub issue or pull request for a security problem.**

Report it by email to [prodsec@aerospike.com](mailto:prodsec@aerospike.com), following the
[Aerospike Vulnerability Disclosure Policy](https://aerospike.com/security/vulnerability-disclosure-policy/).

Please include:

- A description of the vulnerability, where it was found, and its potential impact.
- Steps to reproduce it: a proof-of-concept program, the module version, and the storage
  backend involved, if any.
- Contact details, if you want to be kept informed. Anonymous reports are accepted.

## What to expect

Aerospike acknowledges a report within three business days, triages it against CVSS, and
keeps you informed while the fix is prepared. Reporter details are kept confidential and are
not shared without explicit permission. Please hold off on public disclosure for 90 calendar
days from the acknowledgment, and coordinate in advance if you believe earlier disclosure is
warranted.

The full terms, including scope, out-of-scope findings and safe harbour, are in the
[Vulnerability Disclosure Policy](https://aerospike.com/security/vulnerability-disclosure-policy/).

## Scope notes for this repository

- This repository is a Go library. A vulnerability in a program that merely imports it is
  in scope only if the cause is in this module's code.
- Vulnerabilities in third-party dependencies with no Aerospike code involved should be
  reported to the upstream maintainer. If a dependency bump is needed here, a normal issue
  or pull request is the right channel.
