'use strict'

const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { execFileSync, spawnSync } = require('node:child_process')

const root = path.resolve(__dirname, '..')
const bundlePath = path.join(root, 'ios', 'Resources', 'worklet.bundle')
const manifestPath = path.join(root, 'ios', 'Resources', 'worklet-manifest.json')
const addonsPath = path.join(root, 'ios', 'Addons')

function sha256 (file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')
}

function git (args) {
  return execFileSync('git', ['-C', root, ...args], { encoding: 'utf8' }).trim()
}

if (!fs.existsSync(bundlePath)) throw new Error('worklet.bundle is missing; run npm run build')

const addons = fs.readdirSync(addonsPath)
  .filter(name => name.endsWith('.xcframework'))
  .sort()

const manifest = {
  formatVersion: 1,
  sourceCommit: git(['rev-parse', 'HEAD']),
  sourceTree: git(['rev-parse', 'HEAD^{tree}']),
  sourceDirty: spawnSync(
    'git',
    ['-C', root, 'diff', '--quiet', 'HEAD', '--', 'core', 'ios/Sources', 'package.json', 'package-lock.json'],
    { stdio: 'ignore' }
  ).status !== 0,
  packageLockSHA256: sha256(path.join(root, 'package-lock.json')),
  bundleSHA256: sha256(bundlePath),
  bundleBytes: fs.statSync(bundlePath).size,
  addons
}

fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + '\n')
console.log(`Wrote ${path.relative(root, manifestPath)} (${manifest.bundleSHA256})`)
