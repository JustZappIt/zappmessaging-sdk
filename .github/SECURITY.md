# Security Policy

## Supported versions

`main` is the only supported branch. Fixes land there; there are no backports.

## Reporting a vulnerability

**Do not open a public issue.** This SDK carries identity keys, encrypted message
content, and wallet-address and payment-request data across a P2P transport, so a
report that becomes public before a fix exists puts users at risk.

Use GitHub's private
[Security Advisories](https://github.com/JustZappIt/zappmessaging-sdk/security/advisories/new)
for this repository. That channel is private to you and the maintainers until an
advisory is published.

Useful things to include, as far as you have them:

- the affected component (`core/`, `ios/`, `android/`, `server/`) and version or commit
- what an attacker gains, and what access they need to start
- a reproduction, or the reasoning that led you to it

You will get an acknowledgement that the report was received. Please give maintainers a
chance to ship a fix before disclosing publicly.

## Scope

In scope: the JavaScript core, the Swift and Kotlin wrappers, the IPC protocol between
them, and the blind-peer server under `server/`.

Out of scope: the third-party dependencies vendored as prebuilt binaries
(BareKit, the Bare native addons). Report those upstream to their own projects, though
we would still like to know so the pinned versions can be moved.
