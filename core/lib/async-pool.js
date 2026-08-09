'use strict'

/**
 * Run an async worker over items with a fixed concurrency limit.
 * Items are claimed in input order so callers can put higher-priority work first.
 */
async function runBounded (items, limit, worker) {
  if (!Array.isArray(items) || items.length === 0) return []
  if (!Number.isInteger(limit) || limit < 1) throw new RangeError('limit must be a positive integer')

  const results = new Array(items.length)
  let next = 0

  async function runWorker () {
    while (next < items.length) {
      const index = next++
      try {
        results[index] = { status: 'fulfilled', value: await worker(items[index], index) }
      } catch (reason) {
        results[index] = { status: 'rejected', reason }
      }
    }
  }

  const workerCount = Math.min(limit, items.length)
  await Promise.all(Array.from({ length: workerCount }, () => runWorker()))
  return results
}

module.exports = { runBounded }
