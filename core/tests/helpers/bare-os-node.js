/**
 * Node adapter for bare-os that isolates each test process from the real home
 * directory. storage.js otherwise resolves to ~/Documents/zappmessaging.
 */
const fs = require('fs')
const os = require('os')
const path = require('path')

const testHome = path.join(os.tmpdir(), 'zappmessaging-node-tests', String(process.pid))

process.once('exit', () => {
  fs.rmSync(testHome, { recursive: true, force: true })
})

module.exports = {
  ...os,
  homedir: () => testHome
}
