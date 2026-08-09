const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'zapp-diagnostics-'))
const identityFileKey = 'ab'.repeat(32)
global.Bare = {
  argv: [
    `--data-dir=${dataDir}`,
    '--log-level=debug',
    `--identity-file-key=${identityFileKey}`
  ]
}

const { createDiagnosticLogger } = require('../lib/diagnostics')

test('debug diagnostics redact secrets, private paths, content, and names', () => {
  const diag = createDiagnosticLogger('test')
  const payload = JSON.stringify({
    seedPhrase: 'alpha beta gamma',
    content: 'private "message" text',
    displayName: 'Private Name'
  })

  diag(payload, identityFileKey, dataDir, { content: 'not serialized' })

  const log = fs.readFileSync(path.join(dataDir, 'zappmessaging', 'debug.log'), 'utf8')
  assert.ok(log.includes('<redacted>'))
  assert.ok(log.includes('<data-dir>'))
  assert.ok(!log.includes(identityFileKey))
  assert.ok(!log.includes(dataDir))
  assert.ok(!log.includes('alpha beta gamma'))
  assert.ok(!log.includes('private'))
  assert.ok(!log.includes('Private Name'))
  assert.ok(!log.includes('not serialized'))

  fs.rmSync(dataDir, { recursive: true, force: true })
})
