'use strict'

// Doorbells are advisory work, bounded independently from durable appends.
// Timed-out underlying RPCs retain their slots until they settle: racing a
// timeout must not create an unbounded number of still-running requests.
class NotificationWork {
  constructor ({ concurrency = 2, maxQueued = 64, timeoutMs = 15000 } = {}) {
    this.concurrency = concurrency
    this.maxQueued = maxQueued
    this.timeoutMs = timeoutMs
    this.queue = []
    this.active = 0
    this.attempts = new Set()
    this.waiters = new Set()
    this.generation = 0
    this.paused = false
  }

  enqueue (attempt, delays) {
    if (this.paused || this.queue.length >= this.maxQueued) {
      return Promise.reject(new Error('notification queue unavailable'))
    }
    return new Promise((resolve, reject) => {
      this.queue.push({ attempt, delays, resolve, reject, generation: this.generation })
      this._drain()
    })
  }

  cancel () {
    this.paused = true
    this.generation++
    for (const job of this.queue.splice(0)) job.reject(new Error('notification cancelled'))
    for (const cancel of [...this.waiters]) cancel()
  }

  resume () { this.paused = false }

  _wait (promise, ms, generation, sleep = false) {
    return new Promise((resolve, reject) => {
      let timer
      const finish = (error, value) => {
        clearTimeout(timer)
        this.waiters.delete(cancel)
        if (error) reject(error)
        else resolve(value)
      }
      const cancel = () => finish(new Error('notification cancelled'))
      if (generation !== this.generation || this.paused) return cancel()
      this.waiters.add(cancel)
      timer = setTimeout(() => finish(sleep ? null : new Error('notification timeout')), ms)
      if (promise) promise.then(value => finish(null, value), error => finish(error))
    })
  }

  async _run (job) {
    let lastError
    for (const delay of job.delays) {
      if (delay > 0) await this._wait(null, delay, job.generation, true)
      if (this.paused || job.generation !== this.generation) throw new Error('notification cancelled')
      if (this.attempts.size >= this.concurrency) throw new Error('notification requests busy')
      const request = Promise.resolve().then(() => {
        if (this.paused || job.generation !== this.generation) throw new Error('notification cancelled')
        return job.attempt()
      })
      this.attempts.add(request)
      request.then(() => this.attempts.delete(request), () => this.attempts.delete(request))
      try {
        await this._wait(request, this.timeoutMs, job.generation)
        return
      } catch (error) {
        lastError = error
        // A timeout is not evidence the RPC stopped. Do not retry it.
        if (this.attempts.has(request)) throw error
      }
    }
    throw lastError || new Error('notification unavailable')
  }

  _drain () {
    while (!this.paused && this.active < this.concurrency && this.queue.length) {
      const job = this.queue.shift()
      this.active++
      this._run(job).then(job.resolve, job.reject).finally(() => {
        this.active--
        this._drain()
      })
    }
  }
}

module.exports = { NotificationWork }
