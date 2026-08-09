/**
 * Media Store - Content-addressed media file storage
 *
 * Stores media files by their Blake2b hash for deduplication.
 * Files stored in ~/Documents/zappmessaging/media/{hash}.{ext}
 */

const fs = require('bare-fs')
const path = require('bare-path')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const { getDataDir, ensureDir } = require('./storage')

function diag (...args) { /* no-op; media-store errors are non-fatal */ }

class MediaStore {
  constructor() {
    this.mediaDir = path.join(getDataDir(), 'media')
    this._ensureDir()
  }

  _ensureDir() {
    ensureDir(this.mediaDir)
  }

  /**
   * Compute Blake2b content hash of a buffer
   * @param {Buffer} buffer - Data to hash
   * @returns {Buffer} 32-byte hash
   */
  hash(buffer) {
    return crypto.data(buffer)
  }

  /**
   * Save media buffer to disk
   * @param {Buffer} buffer - Media data
   * @param {string} ext - File extension (jpg, png, gif, etc.)
   * @returns {Object} { hashHex, filePath, fileSize }
   */
  saveMedia(buffer, ext) {
    const hashBuf = this.hash(buffer)
    const hashHex = b4a.toString(hashBuf, 'hex')
    const safeExt = (ext || 'bin').replace(/[^a-zA-Z0-9]/g, '').substring(0, 10)
    const fileName = hashHex + '.' + safeExt
    const filePath = path.join(this.mediaDir, fileName)

    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, buffer)
    }

    return { hashHex, filePath, fileSize: buffer.length }
  }

  /**
   * Save media with a known hash (used when reassembling from chunks)
   * @param {Buffer} buffer - Media data
   * @param {string} hashHex - Known hash (hex)
   * @param {string} ext - File extension
   * @returns {string} File path
   */
  saveMediaWithHash(buffer, hashHex, ext) {
    const safeExt = (ext || 'bin').replace(/[^a-zA-Z0-9]/g, '').substring(0, 10)
    const fileName = hashHex + '.' + safeExt
    const filePath = path.join(this.mediaDir, fileName)

    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, buffer)
    }

    return filePath
  }

  /**
   * Save a thumbnail for a media file
   * @param {Buffer} buffer - Thumbnail data
   * @param {string} hashHex - Media hash (hex)
   * @returns {string} Thumbnail file path
   */
  saveThumbnail(buffer, hashHex) {
    const fileName = hashHex + '_thumb.jpg'
    const filePath = path.join(this.mediaDir, fileName)

    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, buffer)
    }

    return filePath
  }

  /**
   * Read media file by hash
   * @param {string} hashHex - Media hash (hex)
   * @returns {Buffer|null} Media data or null
   */
  getMedia(hashHex) {
    // Try common extensions
    const exts = ['jpg', 'jpeg', 'png', 'gif', 'mp4']
    for (const ext of exts) {
      const filePath = path.join(this.mediaDir, hashHex + '.' + ext)
      if (fs.existsSync(filePath)) {
        return fs.readFileSync(filePath)
      }
    }
    return null
  }

  /**
   * Check if media exists locally
   * @param {string} hashHex - Media hash (hex)
   * @returns {boolean} True if media exists
   */
  hasMedia(hashHex) {
    const exts = ['jpg', 'jpeg', 'png', 'gif', 'mp4']
    for (const ext of exts) {
      const filePath = path.join(this.mediaDir, hashHex + '.' + ext)
      if (fs.existsSync(filePath)) return true
    }
    return false
  }

  /**
   * Get the local file path for a media hash
   * @param {string} hashHex - Media hash (hex)
   * @returns {string|null} File path or null
   */
  getMediaPath(hashHex) {
    const exts = ['jpg', 'jpeg', 'png', 'gif', 'mp4']
    for (const ext of exts) {
      const filePath = path.join(this.mediaDir, hashHex + '.' + ext)
      if (fs.existsSync(filePath)) return filePath
    }
    return null
  }

  /**
   * Delete media file by hash
   * @param {string} hashHex - Media hash (hex)
   * @returns {boolean} Success status
   */
  deleteMedia(hashHex) {
    const exts = ['jpg', 'jpeg', 'png', 'gif', 'mp4']
    let deleted = false
    for (const ext of exts) {
      const filePath = path.join(this.mediaDir, hashHex + '.' + ext)
      if (fs.existsSync(filePath)) {
        try {
          fs.unlinkSync(filePath)
          deleted = true
        } catch (err) {
          diag('Failed to delete media:', err)
        }
      }
    }
    
    // Also delete thumbnail if exists
    const thumbPath = path.join(this.mediaDir, hashHex + '_thumb.jpg')
    if (fs.existsSync(thumbPath)) {
      try {
        fs.unlinkSync(thumbPath)
      } catch (err) {
        diag('Failed to delete thumbnail:', err)
      }
    }
    
    return deleted
  }
}

module.exports = { MediaStore }
