# Contributing to ZappMessaging

ZappMessaging is the open-source P2P messaging SDK (Hyperswarm + Bare) that
Zapp wallets embed for encrypted chat and in-chat ZEC payments. It runs with no
central server; an optional blind peer mirrors encrypted cores for offline
delivery without seeing message contents.

This document sets contribution expectations at the standard the wider Zcash
ecosystem holds itself to, modeled on the
[librustzcash contribution guidelines](https://github.com/zcash/librustzcash/blob/main/CONTRIBUTING.md)
and adapted for a JavaScript/Bare project rather than a Rust one.

## Code of Conduct

Be respectful, assume good faith, and keep disagreement about the code, not
the person. Harassment, personal attacks, and exclusionary behavior are not
tolerated in issues, pull requests, or any other project space. Maintainers
may remove comments or contributors that violate this.

## Asking questions

Open a GitHub issue with the "question" label. Check existing issues first;
your question may already be answered there.

## Reporting a security issue

**Do not open a public issue for a security vulnerability.** This SDK carries
identity keys, encrypted message content, and wallet-address/payment-request
data across a P2P transport, and a disclosure that's public before it's fixed
can put users at risk. Use GitHub's private
[Security Advisories](https://github.com/JustZappIt/zappmessaging-sdk/security/advisories/new)
for this repository instead, so a fix can be prepared before details go
public.

## Before you start

Look for an existing issue before opening a new one; the change you're
planning may already be discussed. Reference the issue a PR addresses when one
exists, and open one first for anything large enough that the approach is worth
agreeing on before the code is written.

Read [`core/API.md`](core/API.md) and [`README.md`](README.md) to orient
yourself in the architecture before making structural changes. Larger
architectural proposals should be discussed in an issue before a PR lands, to
avoid rework.

## Development setup

```bash
npm install
npm test    # unit tests: core/tests/*.test.js
```

`npm test` is what CI gates on. The golden-vector identity/crypto tests run on
the Bare runtime rather than Node, and `bare` is not installed by
`npm install`, so install it separately if you are touching identity or crypto:

```bash
npm install -g bare
npm run test:identity
```

There is no linter configured yet (`npm run lint` is a placeholder). Match
the existing code style by eye until one is added; a contribution that adds
one (e.g. `eslint` with a config matching the rest of the Zapp org) is
welcome as its own PR.

### The worklet bundles are generated: never hand-edit them

Both platforms load a bundle packed from `core/index.js` by `bare-pack`, not
written by hand. The two are tracked differently, so a `core/` change has to
handle them differently.

**Android's bundle is committed.** If your change touches anything under
`core/`, rebuild it and include the result in the same PR:

```bash
npm run build:android  # rebuilds android/src/main/assets/worklet.bundle
```

CI enforces this: `.github/workflows/ci.yml` repacks the bundle and fails on
`git diff --exit-code -- android/src/main/assets/worklet.bundle`, so a PR whose
committed bundle is stale relative to `core/` will not go green.

**iOS's bundle is gitignored** (`ios/Resources/worklet.bundle` and its
manifest), because hosts build it from their own checkout via `npm run setup`.
Do not commit it, and do not force-add it past `.gitignore`. Rebuild it locally
when testing an iOS change:

```bash
npm run build  # rebuilds ios/Resources/worklet.bundle + manifest, not committed
```

A bundle that is stale relative to `core/` does more than fail CI: it silently
breaks message delivery in a running app, because the native side loads the
bundle it was shipped, not the current source.

### iOS

The Xcode project is generated, not committed. Install XcodeGen once:

```bash
brew install xcodegen
```

Then, from a fresh checkout:

```bash
npm run setup                # native addons + ios/Resources/worklet.bundle
cd ios && xcodegen generate  # writes ios/ZappMessaging.xcodeproj
```

`ios/project.yml` is the source of truth. `ios/ZappMessaging.xcodeproj` is
gitignored and must never be committed: XcodeGen resolves environment
variables at generation time, so a committed project carries the generating
machine's absolute paths.

`ios/Package.swift` declares no test target, so `swift test` does not run the
iOS tests. They live in `ios/Tests/ZappMessagingTests` and run through the
generated project:

```bash
cd ios
xcodebuild test -project ZappMessaging.xcodeproj -scheme ZappMessaging \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro'
```

Substitute any simulator listed by `xcrun simctl list devices available`. CI
runs the Node side only, so an iOS change has to be verified locally.

## Commit messages

Commit messages are part of this project's documentation, not just a build
log. Follow this format:

```
type(scope): imperative, present-tense description
```

`type` is one of `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `chore`,
or `security`. `scope` names the affected area (`core`, `ipc`, `p2p`, `ios`,
`android`, `media`, `chat`, `security`, …). Examples:

```
fix(p2p): bootstrap firewalled chats through relay
fix(ipc): stop the 1 MB frame cap from bricking media-heavy rooms
fix(security): enforce group membership revocation
feat(chat): read receipts over the relay and live socket
```

The first line is the *title*: treat it like a headline a maintainer could
put straight into release notes. Use the commit body for the "why," not a
restatement of the diff; an issue or PR thread can be edited or deleted later,
the commit message ships with the code. Reference the issue a commit addresses
(`Closes #123`) when one exists.

Prefer small, focused PRs. Maintainers pick the merge method per PR, so write
every commit as though it will land verbatim: keep a refactor as its own commit
ahead of a behavior change if it makes review easier.

## Licensing and Developer's Certificate of Origin

This project is licensed under [Apache License 2.0](LICENSE). By submitting
a contribution, you certify the
[Developer's Certificate of Origin](https://developercertificate.org/):

- (a) the contribution was created in whole or in part by you and you have
  the right to submit it under the project's license; or
- (b) the contribution is based on prior work that, to the best of your
  knowledge, is covered under an appropriate open-source license, and you
  have the right under that license to submit that work, possibly modified,
  under the same license (unless you're permitted to submit under a
  different license); or
- (c) the contribution was provided to you by someone who certified (a) or
  (b) and you have not modified it; and
- (d) you understand this project and the contribution are public, and that
  a record of the contribution (including your sign-off) is maintained
  indefinitely and may be redistributed consistent with the project's
  license.

## Pull request checklist

- [ ] `npm test` passes
- [ ] If `core/` changed: `npm run build:android` was rerun and the updated
      `android/src/main/assets/worklet.bundle` is part of the PR
- [ ] If identity or crypto changed: `npm run test:identity` passes
- [ ] If `ios/` changed: `xcodebuild test -scheme ZappMessaging` passes against
      a freshly regenerated project
- [ ] `ios/ZappMessaging.xcodeproj` is not in the PR; `ios/project.yml` carries
      any project change instead
- [ ] Commit messages follow `type(scope): description`
- [ ] References the issue it addresses, if there is one
