/**
 * Inert bare-ipc stub for node --test (see node-compat.js). The real module
 * pulls in bare-pipe, a native addon that throws under plain Node at import.
 * ipc-handler only touches this when the Bare.IPC global is absent; tests that
 * construct an IPCHandler set a stub Bare.IPC, so this is never exercised —
 * it only needs to load without a native binding.
 */
class IPC {
  on () {}
  write () {}
  once () {}
  removeListener () {}
}
module.exports = new IPC()
module.exports.IPC = IPC
