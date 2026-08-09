const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'zapp-diagnostics-off-'))
const legacyLog = path.join(dataDir, 'zappmessaging', 'diag.log')
fs.mkdirSync(path.dirname(legacyLog), { recursive: true })
fs.writeFileSync(legacyLog, 'legacy private diagnostics')
global.Bare = { argv: [`--data-dir=${dataDir}`] }

const { createDiagnosticLogger } = require('../lib/diagnostics')

test('file diagnostics are disabled by default and legacy logs are removed', () => {
  createDiagnosticLogger('test')('must not be written')
  assert.ok(!fs.existsSync(path.join(dataDir, 'zappmessaging', 'debug.log')))
  assert.ok(!fs.existsSync(legacyLog))
  fs.rmSync(dataDir, { recursive: true, force: true })
})
