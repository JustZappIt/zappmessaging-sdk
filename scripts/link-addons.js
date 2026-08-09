'use strict'

const link = require('bare-link')

async function main () {
  const preset = process.argv[2]
  const out = process.argv[3]
  if (!preset || !out) throw new Error('usage: link-addons <preset> <output-directory>')
  for await (const resource of link('.', { preset, out })) {
    process.stdout.write(resource + '\n')
  }
}

main().catch(error => {
  process.stderr.write((error && error.stack) || String(error))
  process.stderr.write('\n')
  process.exitCode = 1
})
