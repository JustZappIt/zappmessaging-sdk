/**
 * Test-only resolver shim: lets the core (written for the Bare runtime) run
 * under `node --test`.
 *
 * bare-fs@4 / bare-path@3 / bare-os@3 are Bare-only (they reference the
 * `Bare` global and native addons at load time), so any suite touching the
 * storage stack crashes under plain Node. The subset of their APIs the core
 * uses (existsSync/readFileSync/writeFileSync/renameSync/mkdirSync/unlinkSync,
 * join/dirname, homedir/tmpdir) is call-compatible with Node's built-ins, so
 * map the bare specifiers to the built-ins when resolving under Node.
 *
 * Loaded via `--require` in the npm test script; node --test propagates it to
 * every spawned test process. Never used by the packed worklet bundle.
 */
const Module = require('module')
const path = require('path')

const NODE_EQUIVALENTS = {
  'bare-fs': 'fs',
  'bare-path': 'path',
  // Keep storage-dependent tests away from the developer's real app data.
  'bare-os': path.join(__dirname, 'bare-os-node.js'),
  // bare-crypto >=1.13 loads a native binding (bare-type) at import time.
  // Core uses only createHash/createHmac/sync-pbkdf2/randomBytes — adapted
  // onto node:crypto in the helper below.
  'bare-crypto': path.join(__dirname, 'bare-crypto-node.js'),
  // bare-ipc pulls in bare-pipe (native addon). ipc-handler only uses it as a
  // fallback when the Bare.IPC global is missing; tests provide a stub IPC.
  'bare-ipc': path.join(__dirname, 'bare-ipc-node.js')
}

const originalResolve = Module._resolveFilename
Module._resolveFilename = function (request, ...rest) {
  return originalResolve.call(this, NODE_EQUIVALENTS[request] || request, ...rest)
}
