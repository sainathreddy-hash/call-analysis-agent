/**
 * Vercel Serverless API — Catch-all handler
 * Replaces the local Node.js HTTP server.
 * All /api/* routes are handled here.
 */
const { fetchFromSheets } = require("../lib/sheets");
const { computeAll } = require("../lib/compute");
const { analyzeCallWithClaude } = require("../lib/claude-analyze");
const cache = require("../lib/cache");

async function ensureData() {
  let data = cache.get();
  if (data) return data;

  console.log("Cache miss — fetching from Google Sheets...");
  const rawCalls = await fetchFromSheets();
  console.log(`Fetched ${rawCalls.length} rows from Sheets`);

  data = computeAll(rawCalls);
  cache.set(data);
  console.log(`Computed and cached: ${data.calls.length} classified calls`);
  return data;
}

module.exports = async function handler(req, res) {
  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

  if (req.method === "OPTIONS") return res.status(200).end();

  const url = new URL(req.url, `https://${req.headers.host}`);
  const path = url.pathname;

  try {
    const data = await ensureData();

    // ─── Static aggregate endpoints ───
    if (path === "/api/stats") return res.json(data.stats);
    if (path === "/api/stories") return res.json(data.stories || []);
    if (path === "/api/curated-stories") return res.json([]);
    if (path === "/api/users") return res.json(data.userStats || {});
    if (path === "/api/escalations") return res.json(data.escalationData || {});
    if (path === "/api/regions") return res.json(data.regionData || []);
    if (path === "/api/sentiment-deep-dive") return res.json(data.sentimentDeepDive || {});
    if (path === "/api/advanced-insights") return res.json(data.advancedInsights || {});
    if (path === "/api/tech-issues") return res.json(data.techIssues || {});
    if (path === "/api/enhanced-escalations") return res.json(data.enhancedEscalations || {});
    if (path === "/api/intent-segmentation") return res.json(data.intentSegmentation || {});
    if (path === "/api/language-breakdown") return res.json(data.languageBreakdown || {});

    // ─── Paginated calls browse ───
    if (path === "/api/calls") {
      const product = url.searchParams.get("product");
      const category = url.searchParams.get("category");
      const sentiment = url.searchParams.get("sentiment");
      const page = parseInt(url.searchParams.get("page") || "1");
      const limit = parseInt(url.searchParams.get("limit") || "50");
      const search = (url.searchParams.get("search") || "").toLowerCase();

      let filtered = data.calls;
      if (product) filtered = filtered.filter(c => c.product === product);
      if (category) filtered = filtered.filter(c => c.category === category);
      if (sentiment) filtered = filtered.filter(c => c.sentimentAnalysis.sentiment === sentiment);
      if (search) filtered = filtered.filter(c =>
        c.transcript.toLowerCase().includes(search) ||
        c.summary.toLowerCase().includes(search) ||
        c.phone?.includes(search) ||
        c.call_sid?.includes(search)
      );

      const total = filtered.length;
      const start = (page - 1) * limit;
      const paged = filtered.slice(start, start + limit).map(c => ({
        ...c,
        transcript: c.transcript.slice(0, 400) + (c.transcript.length > 400 ? "..." : ""),
      }));

      return res.json({ calls: paged, total, page, pages: Math.ceil(total / limit) });
    }

    // ─── Single call detail ───
    if (path.startsWith("/api/call/")) {
      const sid = path.split("/api/call/")[1];
      const call = data.calls.find(c => c.call_sid === sid);
      if (!call) return res.status(404).json({ error: "Call not found" });
      return res.json(call);
    }

    // ─── Claude AI Analysis (POST) ───
    if (path === "/api/claude-analyze" && req.method === "POST") {
      const { call_sid } = req.body || {};
      const call = data.calls.find(c => c.call_sid === call_sid);
      if (!call) return res.status(404).json({ error: "Call not found" });

      const analysis = await analyzeCallWithClaude(
        call.transcript,
        { product: call.product, durationMin: call.durationMin, category: call.category }
      );
      return res.json({ call_sid, analysis });
    }

    return res.status(404).json({ error: "Not found" });
  } catch (err) {
    console.error("API error:", err);
    return res.status(500).json({ error: err.message });
  }
};
