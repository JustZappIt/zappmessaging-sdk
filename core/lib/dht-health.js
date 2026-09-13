/**
 * DHT Health Monitor
 *
 * Monitors DHT health using the existing Hyperswarm instance — no temporary
 * swarms are created, avoiding file-descriptor and memory leaks that
 * previously caused message delivery to degrade over prolonged sessions.
 *
 * Health is determined by:
 *   1. Whether the main swarm's DHT is ready (lightweight `.ready()` call)
 *   2. The number of live peer connections tracked by P2PManager
 *
 * Status levels:
 *   HEALTHY  — DHT ready and at least one peer connected
 *   DEGRADED — DHT ready but zero peers connected
 *   CRITICAL — DHT not ready (bootstrap unreachable / network down)
 */

const EventEmitter = require('bare-events')
const config = require('./config')
const { createDiagnosticLogger } = require('./diagnostics')

const diag = createDiagnosticLogger('DHT')

const HealthStatus = {
  HEALTHY: 'healthy',
  DEGRADED: 'degraded',
  CRITICAL: 'critical'
}

class DHTHealthMonitor extends EventEmitter {
  /**
   * @param {Object} options
   * @param {number} options.checkInterval - ms between checks (default config.DHT_CHECK_INTERVAL_MS)
   * @param {number} options.checkTimeout  - ms to wait for dht.ready (default config.DHT_CHECK_TIMEOUT_MS)
   */
  constructor(options = {}) {
    super()

    this.checkInterval = options.checkInterval || config.DHT_CHECK_INTERVAL_MS
    this.checkTimeout = options.checkTimeout || config.DHT_CHECK_TIMEOUT_MS

    // External references (set via setSwarm / setPeerCountFn)
    this._swarm = null
    this._getPeerCount = () => 0

    // Health state
    this.currentStatus = HealthStatus.HEALTHY
    this.lastCheckTime = null
    this.consecutiveFailures = 0
    this.lastDhtReady = true
    this.lastPeerCount = 0

    // Monitoring control
    this.monitoringInterval = null
    this.isMonitoring = false
  }

  /**
   * Bind the monitor to the main Hyperswarm instance.
   * Must be called before startMonitoring().
   * @param {Object} swarm - Hyperswarm instance
   */
  setSwarm(swarm) {
    this._swarm = swarm
  }

  /**
   * Provide a function that returns the current live peer count.
   * @param {Function} fn - () => number
   */
  setPeerCountFn(fn) {
    this._getPeerCount = fn
  }

  /**
   * Lightweight health check — no new sockets or swarms are created.
   * Tests the existing swarm's DHT readiness + peer connectivity.
   */
  async checkHealth() {
    const timestamp = new Date().toISOString()
    let dhtReady = false
    const peerCount = this._getPeerCount()

    if (this._swarm && this._swarm.dht) {
      try {
        await Promise.race([
          this._swarm.dht.ready(),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('DHT ready timeout')), this.checkTimeout)
          )
        ])
        dhtReady = true
      } catch (e) {
        diag('[DHTHealth] DHT ready check failed:', e.message)
      }
    }

    // Determine status
    let status
    if (!dhtReady) {
      status = HealthStatus.CRITICAL
      this.consecutiveFailures++
    } else if (peerCount === 0) {
      status = HealthStatus.DEGRADED
      this.consecutiveFailures = 0
    } else {
      status = HealthStatus.HEALTHY
      this.consecutiveFailures = 0
    }

    this.lastDhtReady = dhtReady
    this.lastPeerCount = peerCount
    this.lastCheckTime = timestamp

    // Emit status change
    if (this.currentStatus !== status) {
      const previous = this.currentStatus
      this.currentStatus = status
      this.emit('status_change', {
        previous,
        current: status,
        dhtReady,
        peerCount,
        consecutiveFailures: this.consecutiveFailures
      })
    }

    const results = { status, dhtReady, peerCount, timestamp, consecutiveFailures: this.consecutiveFailures }
    this.emit('health_check', results)
    return results
  }

  /**
   * Start periodic monitoring.
   */
  async startMonitoring() {
    if (this.isMonitoring) return

    diag(`[DHTHealth] Starting monitoring (interval: ${this.checkInterval}ms)`)
    this.isMonitoring = true

    try {
      const initial = await this.checkHealth()
      diag('[DHTHealth] Initial check:', initial.status,
        'dht=' + initial.dhtReady, 'peers=' + initial.peerCount)
    } catch (e) {
      diag('[DHTHealth] Initial check failed:', e.message)
    }

    this.monitoringInterval = setInterval(async () => {
      try { await this.checkHealth() } catch (e) {
        diag('[DHTHealth] Periodic check failed:', e.message)
      }
    }, this.checkInterval)
  }

  /**
   * Stop monitoring.
   */
  stopMonitoring() {
    if (!this.isMonitoring) return
    diag('[DHTHealth] Stopping monitoring')
    if (this.monitoringInterval) {
      clearInterval(this.monitoringInterval)
      this.monitoringInterval = null
    }
    this.isMonitoring = false
  }

  getStatus() {
    return {
      status: this.currentStatus,
      dhtReady: this.lastDhtReady,
      peerCount: this.lastPeerCount,
      lastCheckTime: this.lastCheckTime,
      consecutiveFailures: this.consecutiveFailures,
      isMonitoring: this.isMonitoring
    }
  }

  isHealthy() {
    return this.currentStatus === HealthStatus.HEALTHY
  }

  isCritical() {
    return this.currentStatus === HealthStatus.CRITICAL
  }
}

module.exports = { DHTHealthMonitor, HealthStatus }
