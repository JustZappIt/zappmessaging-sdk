/**
 * Opt-in diagnostic logging for the Bare worklet.
 *
 * File diagnostics are disabled unless --log-level=debug is supplied. Debug
 * output shares one bounded file and avoids serializing arbitrary objects so
 * IPC payloads cannot accidentally be persisted by a caller.
 */

const MAX_LOG_BYTES = 1024 * 1024
const MAX_LINE_CHARS = 4096
const LEGACY_LOG_NAMES = [
  'diag.log',
  'p2p-diag.log',
  'chat-store-diag.log',
  'blind-mirror-diag.log',
  'hypercore-diag.log'
]
let legacyCleanupComplete = false

function createDiagnosticLogger (scope) {
  const { LOG_LEVEL, IDENTITY_FILE_KEY } = require('./config')

  let fs
  let path
  let logFile
  let sensitivePaths
  let legacyLogFiles
  try {
    fs = require('bare-fs')
    path = require('bare-path')
    const os = require('bare-os')
    const dataDirArg = (typeof Bare !== 'undefined' ? Bare.argv : [])
      .find(arg => arg.startsWith('--data-dir='))
    const baseDir = dataDirArg
      ? dataDirArg.substring(dataDirArg.indexOf('=') + 1)
      : path.join(os.homedir(), 'Documents')
    logFile = path.join(baseDir, 'zappmessaging', 'debug.log')
    sensitivePaths = [baseDir, os.homedir()].filter(Boolean)
    legacyLogFiles = LEGACY_LOG_NAMES
      .map(name => path.join(baseDir, 'zappmessaging', name))
      .concat(path.join(os.tmpdir(), 'zappmessaging_diag.log'))
  } catch (_) {
    return () => {}
  }

  if (!legacyCleanupComplete) {
    legacyCleanupComplete = true
    removeLegacyLogs(fs, legacyLogFiles)
  }

  if (LOG_LEVEL !== 'debug') return () => {}

  const safeScope = String(scope || 'CORE').replace(/[^A-Z0-9_-]/gi, '').toUpperCase()

  return (...args) => {
    try {
      const logDir = path.dirname(logFile)
      if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true })

      try {
        if (fs.statSync(logFile).size >= MAX_LOG_BYTES) fs.writeFileSync(logFile, '')
      } catch (_) {
        // The log does not exist yet.
      }

      const message = args.map(formatDiagnosticValue).join(' ')
      const redacted = redactSecrets(message, IDENTITY_FILE_KEY, sensitivePaths)
        .replace(/[\r\n]+/g, ' ')
        .slice(0, MAX_LINE_CHARS)
      fs.appendFileSync(logFile, `${new Date().toISOString()} [${safeScope}] ${redacted}\n`)
    } catch (_) {
      // Diagnostics must never affect messaging.
    }
  }
}

function removeLegacyLogs (fs, files) {
  for (const file of files) {
    try {
      if (fs.existsSync(file)) fs.unlinkSync(file)
    } catch (_) {
      // A cleanup failure must not prevent the worklet from starting.
    }
  }
}

function formatDiagnosticValue (value) {
  if (value instanceof Error) return `${value.name}: ${value.message}`
  if (value === null || value === undefined) return String(value)
  if (typeof value === 'object') return `[${value.constructor && value.constructor.name ? value.constructor.name : 'Object'}]`
  return String(value)
}

function redactSecrets (value, identityFileKey, sensitivePaths) {
  let redacted = value
    .replace(/--identity-file-key=[^\s]+/gi, '--identity-file-key=<redacted>')
    .replace(
      /("(?:seedPhrase|content|replyToContent|displayName|senderDisplayName|groupName|newMemberName)"\s*:\s*)("(?:\\.|[^"\\])*"|[^,\s}]*)/gi,
      '$1"<redacted>"'
    )
  if (identityFileKey) redacted = redacted.split(identityFileKey).join('<redacted>')
  for (const privatePath of sensitivePaths || []) {
    redacted = redacted.split(privatePath).join('<data-dir>')
  }
  return redacted
}

module.exports = { createDiagnosticLogger }
