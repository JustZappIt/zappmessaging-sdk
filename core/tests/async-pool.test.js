const test = require('node:test')
const assert = require('node:assert')

const { runBounded } = require('../lib/async-pool')

test('runBounded caps concurrency and preserves result order', async () => {
  let active = 0
  let maxActive = 0
  const started = []

  const results = await runBounded([0, 1, 2, 3, 4, 5], 3, async (value) => {
    started.push(value)
    active++
    maxActive = Math.max(maxActive, active)
    await new Promise(resolve => setTimeout(resolve, value % 2 === 0 ? 5 : 1))
    active--
    return value * 2
  })

  assert.ok(maxActive <= 3)
  assert.deepStrictEqual(started.slice(0, 3), [0, 1, 2])
  assert.deepStrictEqual(results.map(result => result.value), [0, 2, 4, 6, 8, 10])
})

test('runBounded contains individual worker failures', async () => {
  const results = await runBounded(['ok', 'bad', 'later'], 2, async (value) => {
    if (value === 'bad') throw new Error('expected')
    return value
  })

  assert.strictEqual(results[0].status, 'fulfilled')
  assert.strictEqual(results[1].status, 'rejected')
  assert.strictEqual(results[2].status, 'fulfilled')
})
