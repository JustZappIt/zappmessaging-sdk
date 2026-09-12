// Run: node --require ./core/tests/helpers/node-compat.js scripts/media-history-benchmark.js
// Synthetic Node/filesystem comparison; excludes codecs, devices and real networks.
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { performance } = require('node:perf_hooks')
const { ChatStore } = require('../core/lib/chat-store')

async function measure (cacheEnabled) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'media-history-bench-'))
  try {
    const store = new ChatStore()
    store.storagePath = dir
    store.conversations.clear()
    // Same production handlers; disable only the cache for a paired control.
    if (!cacheEnabled) store._cacheMessages = () => {}
    await store.createConversationWithId('bench', 'direct', ['22'.repeat(32)])
    const now = Date.now()
    const history = Array.from({ length: 2000 }, (_, i) => ({
      id: 'm' + i, content: 'x'.repeat(100), senderId: '22'.repeat(32), timestamp: now - 3000 + i,
      mediaId: 'ab'.repeat(32), mediaAuthorized: true
    }))
    fs.writeFileSync(path.join(dir, 'bench.json'), JSON.stringify(history))
    let start = performance.now()
    for (let i = 0; i < 160; i++) store.canServeMedia('ab'.repeat(32), '22'.repeat(32))
    const authorizationMs = performance.now() - start
    start = performance.now()
    for (let i = 0; i < 200; i++) {
      await store.addMessage('bench', { id: 'new' + i, content: 'x'.repeat(100), senderId: '22'.repeat(32) })
    }
    const catchupMs = performance.now() - start
    const rows = (await store.getMessages('bench', 5000)).length
    if (rows !== 2200) throw new Error('History loss')
    return { cacheEnabled, authorizationMs, catchupMs, rows }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
}
;(async () => {
  const samples = []
  for (let sample = 0; sample < 3; sample++) {
    for (const enabled of sample % 2 ? [true, false] : [false, true]) samples.push(await measure(enabled))
  }
  const median = values => values.sort((a, b) => a - b)[Math.floor(values.length / 2)]
  console.log(JSON.stringify({ history: 2000, authorizationChecks: 160, catchupRecords: 200,
    results: [false, true].map(cacheEnabled => {
      const matching = samples.filter(sample => sample.cacheEnabled === cacheEnabled)
      return { cacheEnabled, authorizationMs: median(matching.map(sample => sample.authorizationMs)),
        catchupMs: median(matching.map(sample => sample.catchupMs)), rows: matching[0].rows }
    }), samples }))
})().catch(error => { console.error(error.message); process.exitCode = 1 })
