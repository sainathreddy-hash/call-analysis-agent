/**
 * In-memory cache with TTL.
 * Survives warm Vercel serverless invocations (5-15 min).
 */
let cachedData = null;
let cachedAt = 0;
const CACHE_TTL = 10 * 60 * 1000; // 10 minutes

module.exports = {
  get() {
    if (cachedData && (Date.now() - cachedAt) < CACHE_TTL) {
      return cachedData;
    }
    return null;
  },
  set(data) {
    cachedData = data;
    cachedAt = Date.now();
  },
};
