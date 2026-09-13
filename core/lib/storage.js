/**
 * Storage - Provides platform-safe writable storage paths and utilities
 * 
 * Handles iOS/Android sandbox paths and provides JSON read/write utilities.
 */

const path = require('bare-path')
const fs = require('bare-fs')
const os = require('bare-os')
const { createDiagnosticLogger } = require('./diagnostics')

const diag = createDiagnosticLogger('STORAGE')

let _dataDir = null

/**
 * Get the application data directory
 * @returns {string} Path to data directory
 */
function getDataDir() {
  if (_dataDir) return _dataDir

  // Check for --data-dir argument (passed by Android BareWorkletManager)
  const dataDirArg = (typeof Bare !== 'undefined' ? Bare.argv : [])
    .find(arg => arg.startsWith('--data-dir='))

  if (dataDirArg) {
    // Android: use the provided app files directory
    _dataDir = path.join(dataDirArg.substring(dataDirArg.indexOf('=') + 1), 'zappmessaging')
  } else {
    // iOS: homedir() returns the app sandbox root
    // Use Documents subdirectory for persistent storage
    const home = os.homedir()
    _dataDir = path.join(home, 'Documents', 'zappmessaging')
  }

  // Ensure directory exists
  ensureDir(_dataDir)

  return _dataDir
}

/**
 * Ensure a directory exists, creating it if necessary
 * @param {string} dirPath - Directory path to ensure
 */
function ensureDir(dirPath) {
  try {
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true })
    }
  } catch (err) {
    diag('Failed to create directory:', err)
    throw err
  }
}

/**
 * Read and parse a JSON file
 * @param {string} filePath - Path to JSON file
 * @returns {Object|null} Parsed JSON object or null if file doesn't exist
 */
function readJSON(filePath) {
  try {
    if (!fs.existsSync(filePath)) {
      return null
    }
    const data = fs.readFileSync(filePath, 'utf8')
    return JSON.parse(data)
  } catch (err) {
    diag('Failed to read JSON file:', err)
    return null
  }
}

/**
 * Write an object to a JSON file
 * @param {string} filePath - Path to JSON file
 * @param {Object} data - Data to write
 */
function writeJSON(filePath, data) {
  try {
    const dir = path.dirname(filePath)
    ensureDir(dir)
    // Atomic write: write to temp file, then rename to avoid corruption on crash
    const tmpPath = filePath + '.tmp'
    fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2), 'utf8')
    fs.renameSync(tmpPath, filePath)
  } catch (err) {
    diag('Failed to write JSON file:', err)
    throw err
  }
}

/**
 * Check if a file exists
 * @param {string} filePath - Path to check
 * @returns {boolean} True if file exists
 */
function fileExists(filePath) {
  try {
    return fs.existsSync(filePath)
  } catch (err) {
    return false
  }
}

module.exports = {
  getDataDir,
  ensureDir,
  readJSON,
  writeJSON,
  fileExists
}
