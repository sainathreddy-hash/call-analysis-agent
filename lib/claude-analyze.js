/**
 * Claude AI-Powered Call Analysis
 * Uses Anthropic API for deep transcript analysis — frustration scoring, tonality, insights
 */
const Anthropic = require("@anthropic-ai/sdk").default;

const API_KEY = process.env.ANTHROPIC_API_KEY || "";

let client;
try {
  client = new Anthropic({ apiKey: API_KEY });
} catch (e) {
  console.warn("Claude API client init failed:", e.message);
}

/**
 * Analyze a single call transcript with Claude for deep frustration/sentiment scoring
 */
async function analyzeCallWithClaude(transcript, metadata = {}) {
  if (!client) return { error: "Claude API not configured" };

  // Trim transcript to fit context (keep first + last 3000 chars for long ones)
  let trimmedTranscript = transcript;
  if (transcript.length > 8000) {
    trimmedTranscript = transcript.slice(0, 4000) + "\n\n[... middle of call omitted for brevity ...]\n\n" + transcript.slice(-4000);
  }

  try {
    const response = await client.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 1024,
      messages: [{
        role: "user",
        content: `Analyze this Indian customer support call transcript. The call is from Stable Money, a fintech platform offering FDs, Bonds, Credit Cards, and Mutual Funds.

TRANSCRIPT:
${trimmedTranscript}

METADATA: Product: ${metadata.product || 'Unknown'}, Duration: ${metadata.durationMin || '?'}min, Category: ${metadata.category || 'Unknown'}

Respond in STRICT JSON only (no markdown, no explanation):
{
  "frustrationScore": <0-100, where 0=perfectly calm, 100=extremely frustrated/angry>,
  "customerTone": "<one of: calm, concerned, confused, frustrated, angry, aggressive, pleading, sarcastic, defeated>",
  "agentTone": "<one of: empathetic, helpful, professional, robotic, defensive, dismissive, confused>",
  "escalationDetected": <true/false>,
  "toneProgression": "<how tone changed during call, e.g. 'calm→frustrated→angry' or 'angry→calm'>",
  "keyFrustrationDrivers": ["<top 3 specific things causing frustration>"],
  "resolutionAchieved": <true/false>,
  "customerEffort": "<low/medium/high/extreme — how much effort customer had to put in>",
  "churnRisk": "<none/low/medium/high/critical>",
  "oneLiner": "<one line summary of what happened and why customer is frustrated/satisfied>"
}`
      }]
    });

    const text = response.content[0].text.trim();
    // Extract JSON from response (handle possible markdown wrapping)
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      return JSON.parse(jsonMatch[0]);
    }
    return { error: "Could not parse Claude response", raw: text.slice(0, 200) };
  } catch (e) {
    return { error: e.message };
  }
}

/**
 * Batch analyze multiple calls (with rate limiting)
 * Returns array of {call_sid, analysis}
 */
async function batchAnalyzeCalls(calls, concurrency = 3) {
  const results = [];
  const queue = [...calls];
  const inFlight = [];

  while (queue.length > 0 || inFlight.length > 0) {
    // Fill up to concurrency
    while (queue.length > 0 && inFlight.length < concurrency) {
      const call = queue.shift();
      const promise = analyzeCallWithClaude(
        call.transcript || (call.transcript_data && call.transcript_data.transcript) || "",
        { product: call.product, durationMin: call.durationMin, category: call.category }
      ).then(analysis => {
        results.push({ call_sid: call.call_sid, analysis });
        return call.call_sid;
      }).catch(err => {
        results.push({ call_sid: call.call_sid, analysis: { error: err.message } });
        return call.call_sid;
      });
      inFlight.push(promise);
    }

    // Wait for at least one to complete
    if (inFlight.length > 0) {
      const completedSid = await Promise.race(inFlight);
      const idx = inFlight.findIndex(p => p === completedSid);
      // Remove completed
      inFlight.splice(inFlight.indexOf(inFlight.find(async p => (await p) === completedSid)), 1);
      // Small delay for rate limiting
      await new Promise(r => setTimeout(r, 500));
    }
  }

  return results;
}

/**
 * Build advanced insights data from classified calls
 * This is computed at startup — no Claude API needed
 */
function buildAdvancedInsights(calls) {
  const total = calls.length;

  // ═══ L2 Issue Deep Dive ═══
  const l2Map = {};
  for (const c of calls) {
    const key = `${c.product} → ${c.category}`;
    if (!l2Map[key]) l2Map[key] = {
      product: c.product, category: c.category, count: 0,
      angry: 0, frustrated: 0, satisfied: 0, totalDur: 0,
      escalations: 0, topCalls: []
    };
    l2Map[key].count++;
    const sent = (c.sentimentAnalysis || {}).sentiment || 'Neutral';
    if (sent === 'Angry') l2Map[key].angry++;
    if (sent === 'Frustrated') l2Map[key].frustrated++;
    if (sent === 'Satisfied') l2Map[key].satisfied++;
    l2Map[key].totalDur += c.duration || 0;
    if (c.agentActions && c.agentActions.some(a => a.action && (a.action.includes('Escalat') || a.action.includes('Ticket')))) {
      l2Map[key].escalations++;
    }
    // Keep top 3 calls per L2 category (by story score or duration)
    if (l2Map[key].topCalls.length < 3 && c.customerQuote && c.customerQuote.length > 10) {
      l2Map[key].topCalls.push({
        call_sid: c.call_sid,
        quote: c.customerQuote.slice(0, 120),
        durationMin: c.durationMin,
        sentiment: sent,
      });
    }
  }
  const l2Data = Object.values(l2Map)
    .sort((a, b) => b.count - a.count)
    .slice(0, 40)
    .map(l => ({
      ...l,
      avgDur: Math.round(l.totalDur / l.count / 60 * 10) / 10,
      frustPct: Math.round((l.angry + l.frustrated) / l.count * 100),
      escPct: Math.round(l.escalations / l.count * 100),
    }));

  // ═══ Breaking Signals (what's breaking us) ═══
  const breakingMap = {};
  let totalBreaking = 0;
  for (const c of calls) {
    if (!c.painSignals) continue;
    for (const p of c.painSignals) {
      const key = p.signal;
      if (!breakingMap[key]) breakingMap[key] = { signal: key, count: 0, severity: p.severity, calls: [] };
      breakingMap[key].count++;
      totalBreaking++;
      if (breakingMap[key].calls.length < 3) {
        breakingMap[key].calls.push({ call_sid: c.call_sid, product: c.product });
      }
    }
  }
  const breakingSignals = Object.values(breakingMap)
    .sort((a, b) => b.count - a.count)
    .slice(0, 15)
    .map(b => ({ ...b, pct: Math.round(b.count / total * 100) }));

  // ═══ Helping Signals (what's working) ═══
  const helpMap = {};
  for (const c of calls) {
    const ha = (c.sentimentAnalysis || {}).helpingAnalysis;
    if (!ha || !ha.helping) continue;
    for (const h of ha.helping) {
      if (!helpMap[h.factor]) helpMap[h.factor] = { factor: h.factor, count: 0 };
      helpMap[h.factor].count++;
    }
  }
  const helpingSignals = Object.values(helpMap)
    .sort((a, b) => b.count - a.count)
    .slice(0, 10)
    .map(h => ({ ...h, pct: Math.round(h.count / total * 100) }));

  // ═══ Department Health Score ═══
  const deptMap = {};
  for (const c of calls) {
    const dept = c.product;
    if (!deptMap[dept]) deptMap[dept] = {
      dept, total: 0, satisfied: 0, angry: 0, frustrated: 0,
      resolved: 0, escalated: 0, totalDur: 0,
      dispositions: {}
    };
    deptMap[dept].total++;
    deptMap[dept].totalDur += c.duration || 0;
    const sent = (c.sentimentAnalysis || {}).sentiment || 'Neutral';
    if (sent === 'Satisfied') deptMap[dept].satisfied++;
    if (sent === 'Angry') deptMap[dept].angry++;
    if (sent === 'Frustrated') deptMap[dept].frustrated++;

    // Dispositions per department
    for (const a of (c.agentActions || [])) {
      deptMap[dept].dispositions[a.action] = (deptMap[dept].dispositions[a.action] || 0) + 1;
      if (a.action.includes('Resolved')) deptMap[dept].resolved++;
      if (a.action.includes('Escalat') || a.action.includes('Ticket')) deptMap[dept].escalated++;
    }
  }
  const deptHealth = Object.values(deptMap)
    .sort((a, b) => b.total - a.total)
    .map(d => {
      const satRate = d.satisfied / d.total;
      const resolveRate = d.resolved / d.total;
      const angryRate = (d.angry + d.frustrated) / d.total;
      // Health score: 100 * (satisfaction + resolution - anger) / 2, clamped 0-100
      const health = Math.max(0, Math.min(100, Math.round((satRate * 50 + resolveRate * 30 - angryRate * 40 + 30))));
      return {
        ...d,
        avgDur: Math.round(d.totalDur / d.total / 60 * 10) / 10,
        satPct: Math.round(satRate * 100),
        angryPct: Math.round(angryRate * 100),
        resolvePct: Math.round(resolveRate * 100),
        escPct: Math.round(d.escalated / d.total * 100),
        healthScore: health,
        topDispositions: Object.entries(d.dispositions)
          .sort((a, b) => b[1] - a[1])
          .slice(0, 5)
          .map(([k, v]) => ({ action: k, count: v, pct: Math.round(v / d.total * 100) })),
      };
    });

  // ═══ Co-Occurrence Patterns ═══
  const coMap = {};
  for (const c of calls) {
    if (!c.painSignals || c.painSignals.length < 2) continue;
    const signals = c.painSignals.map(p => p.signal).sort();
    for (let i = 0; i < signals.length; i++) {
      for (let j = i + 1; j < signals.length; j++) {
        const key = `${signals[i]} + ${signals[j]}`;
        coMap[key] = (coMap[key] || 0) + 1;
      }
    }
  }
  const coPatterns = Object.entries(coMap)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .map(([pattern, count]) => ({ pattern, count }));

  // ═══ Escalation Deep Dive ═══
  const escCalls = calls.filter(c => c.agentActions && c.agentActions.some(a => a.action && (a.action.includes('Escalat') || a.action.includes('Ticket'))));
  const escByDept = {};
  const escCategories = {};
  for (const c of escCalls) {
    escByDept[c.product] = (escByDept[c.product] || 0) + 1;
    escCategories[c.category] = (escCategories[c.category] || 0) + 1;
  }

  const escalationDeepDive = {
    totalEscalations: escCalls.length,
    escPct: Math.round(escCalls.length / total * 100),
    byDept: Object.entries(escByDept).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ dept: k, count: v, pct: Math.round(v / (deptMap[k]?.total || 1) * 100) })),
    topCategories: Object.entries(escCategories).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([k, v]) => ({ category: k, count: v })),
    avgEscDuration: Math.round(escCalls.reduce((s, c) => s + (c.duration || 0), 0) / (escCalls.length || 1) / 60 * 10) / 10,
  };

  // ═══ Disposition Analysis ═══
  const dispMap = {};
  for (const c of calls) {
    for (const a of (c.agentActions || [])) {
      if (!dispMap[a.action]) dispMap[a.action] = {
        action: a.action, count: 0, products: {},
        topCalls: []
      };
      dispMap[a.action].count++;
      dispMap[a.action].products[c.product] = (dispMap[a.action].products[c.product] || 0) + 1;
      if (dispMap[a.action].topCalls.length < 5 && c.customerQuote && c.customerQuote.length > 10) {
        dispMap[a.action].topCalls.push({
          call_sid: c.call_sid,
          product: c.product,
          category: c.category,
          quote: c.customerQuote.slice(0, 100),
          durationMin: c.durationMin,
        });
      }
    }
  }
  const dispositions = Object.values(dispMap)
    .sort((a, b) => b.count - a.count)
    .map(d => ({
      ...d,
      pct: Math.round(d.count / total * 100),
      productBreakdown: Object.entries(d.products)
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => ({ product: k, count: v })),
    }));

  // ═══ Hourly Distribution ═══
  const hourly = new Array(24).fill(0);
  for (const c of calls) {
    if (c.startTime) {
      try {
        const h = new Date(c.startTime).getHours();
        if (!isNaN(h)) hourly[h]++;
      } catch (e) {}
    }
  }

  // ═══ Action Plan (prioritized) ═══
  const actionPlan = [];
  // Priority 1: High frustration + high volume issues
  for (const l of l2Data.slice(0, 10)) {
    if (l.frustPct > 30 && l.count > 20) {
      actionPlan.push({
        priority: 'P0',
        issue: `${l.product} → ${l.category}`,
        reason: `${l.count} calls, ${l.frustPct}% frustrated/angry, ${l.escPct}% escalated`,
        metric: `${l.count} calls affected`,
      });
    }
  }
  // Priority 2: Top breaking signals
  for (const b of breakingSignals.slice(0, 5)) {
    actionPlan.push({
      priority: b.severity === 'critical' ? 'P0' : 'P1',
      issue: `Fix: ${b.signal}`,
      reason: `Detected in ${b.count} calls (${b.pct}%)`,
      metric: `${b.count} affected`,
    });
  }
  // Priority 3: Dept health
  for (const d of deptHealth) {
    if (d.healthScore < 40) {
      actionPlan.push({
        priority: 'P1',
        issue: `Improve ${d.dept} support quality`,
        reason: `Health score: ${d.healthScore}/100, ${d.angryPct}% angry/frustrated`,
        metric: `${d.total} calls`,
      });
    }
  }

  return {
    l2Data,
    breakingSignals,
    totalBreakingCalls: Object.values(breakingMap).reduce((s, b) => s + b.count, 0),
    helpingSignals,
    deptHealth,
    coPatterns,
    escalationDeepDive,
    dispositions,
    hourlyDistribution: hourly,
    actionPlan: actionPlan.slice(0, 15),
    generatedAt: new Date().toISOString(),
  };
}

module.exports = { analyzeCallWithClaude, batchAnalyzeCalls, buildAdvancedInsights };

