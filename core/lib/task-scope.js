'use strict'

// An identity may be replaced only after every old operation has settled.
// Closing admission prevents late callbacks from starting more work; draining
// keeps asynchronous store writes from crossing the identity boundary.
class TaskScope {
  constructor () {
    this.closed = false
    this.tasks = new Set()
  }

  run (fn) {
    if (this.closed) return Promise.reject(new Error('Messaging lifecycle stopped'))
    let task
    try { task = Promise.resolve(fn()) } catch (error) { return Promise.reject(error) }
    this.tasks.add(task)
    const forget = () => this.tasks.delete(task)
    task.then(forget, forget)
    return task
  }

  async drain () {
    this.closed = true
    while (this.tasks.size) await Promise.allSettled([...this.tasks])
  }
}

module.exports = { TaskScope }
