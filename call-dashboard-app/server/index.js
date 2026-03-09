const http = require("http");
const fs = require("fs");
const path = require("path");
const { classifyCall, paraphraseQuote, analyzeSentiment } = require("./classify");
const { analyzeCallWithClaude, buildAdvancedInsights } = require("./claude-analyze");

const PORT = 3456;
const DATA_PATH = path.join(__dirname, "..", "data", "classified_calls.json");
const SOURCE_PATH = path.resolve(__dirname, "../../call-insights-pipeline/output/transcribed_calls.json");
const CURATED_PATH = path.join(__dirname, "..", "data", "curated_stories.json");

// ─── Pre-process ───
function loadAndClassify() {
  console.log("Loading source data...");
  const raw = JSON.parse(fs.readFileSync(SOURCE_PATH, "utf-8"));
  console.log(`Loaded ${raw.length} raw calls. Classifying...`);

  const classified = raw
    .filter(c => c.transcript_data && c.transcript_data.transcript && c.transcript_data.transcript.length > 20)
    .map(c => classifyCall(c));

  console.log(`Classified ${classified.length} calls.`);

  // Build stats
  const stats = buildStats(classified);

  // Extract user stories (now with recording URL + department/channel)
  const stories = classified
    .filter(c => c.storyScore >= 3 && c.customerQuote.length > 20)
    .sort((a, b) => b.storyScore - a.storyScore)
    .slice(0, 40)
    .map(c => ({
      call_sid: c.call_sid,
      product: c.product,
      category: c.category,
      sentiment: c.sentimentAnalysis.sentiment,
      sentimentColor: c.sentimentAnalysis.sentimentColor,
      sentimentScore: c.sentimentAnalysis.sentimentScore,
      duration: c.durationMin,
      quote: c.customerQuote,
      quoteEnglish: paraphraseQuote(c.customerQuote),
      summary: c.summary,
      storyScore: c.storyScore,
      painSignals: c.painSignals.map(p => p.signal),
      agentActions: c.agentActions.map(a => a.action),
      circle: c.circle,
      phone: c.phone ? c.phone.replace(/\d{6}(\d{4})$/, '******$1') : '',
      recordingUrl: c.recordingUrl || '',
      hasRecording: c.hasRecording || false,
      department: c.department || '',
    }));

  // Build user-level stats
  const userStats = buildUserStats(classified);

  // Build escalation data
  const escalationData = buildEscalationData(classified);

  // Build region data
  const regionData = buildRegionData(classified);

  // Build deep sentiment analytics
  const sentimentDeepDive = buildSentimentDeepDive(classified);
  console.log(`Sentiment: ${sentimentDeepDive.frustratedPct}% frustrated/angry, ${sentimentDeepDive.satisfiedPct}% satisfied, ${sentimentDeepDive.escalationPatternCount} escalation patterns`);

  // Build advanced insights (L2, signals, dept health, escalations, dispositions, action plan)
  console.log("Building advanced insights...");
  const advancedInsights = buildAdvancedInsights(classified);
  console.log(`Advanced: ${advancedInsights.l2Data.length} L2 categories, ${advancedInsights.breakingSignals.length} breaking signals, ${advancedInsights.deptHealth.length} depts, ${advancedInsights.dispositions.length} dispositions, ${advancedInsights.actionPlan.length} action items`);

  // Build tech issues data
  const techIssues = buildTechIssues(classified);
  console.log(`Tech Issues: ${techIssues.totalTechCalls} calls, ${techIssues.byProduct.length} products affected`);

  // Build enhanced escalation data
  const enhancedEscalations = buildEnhancedEscalations(classified);
  console.log(`Enhanced Escalations: ${enhancedEscalations.total} total, ${enhancedEscalations.reasons.length} reasons`);

  // Build user intent segmentation
  const intentSegmentation = buildIntentSegmentation(classified);
  console.log(`Intent: ${intentSegmentation.postConversion.total} post-conversion stuck, ${intentSegmentation.preConversion.total} pre-conversion stuck`);

  // Build language breakdown (customer preferred language from transcripts)
  const languageBreakdown = buildLanguageBreakdown(classified);
  console.log(`Language: ${languageBreakdown.distribution.map(d => `${d.language}: ${d.count}`).join(', ')}`);

  const result = { calls: classified, stats, stories, userStats, escalationData, regionData, sentimentDeepDive, advancedInsights, techIssues, enhancedEscalations, intentSegmentation, languageBreakdown, generatedAt: new Date().toISOString() };
  fs.writeFileSync(DATA_PATH, JSON.stringify(result));
  console.log(`Saved to ${DATA_PATH} (${stories.length} stories, ${userStats.uniqueCallers} unique callers)`);
  return result;
}

function buildUserStats(calls) {
  const users = {};
  for (const c of calls) {
    const phone = c.phone || 'unknown';
    if (!(phone in users)) users[phone] = { calls: 0, products: {}, totalDur: 0, angry: 0, escalations: 0 };
    users[phone].calls++;
    users[phone].products[c.product] = (users[phone].products[c.product] || 0) + 1;
    users[phone].totalDur += c.duration;
    if (c.sentimentAnalysis.sentiment === 'Angry') users[phone].angry++;
    if (c.agentActions.some(a => a.action.includes('Escalat') || a.action.includes('Ticket') || a.action.includes('RM'))) users[phone].escalations++;
  }

  const userList = Object.entries(users).sort((a, b) => b[1].calls - a[1].calls);
  const topCallers = userList.slice(0, 20).map(([phone, u]) => ({
    phone,
    calls: u.calls,
    totalMin: Math.round(u.totalDur / 60),
    products: Object.keys(u.products).join(', '),
    angry: u.angry,
    escalations: u.escalations,
  }));

  return {
    uniqueCallers: userList.length,
    singleCallers: userList.filter(([, u]) => u.calls === 1).length,
    repeatCallers: userList.filter(([, u]) => u.calls >= 2).length,
    frequentCallers: userList.filter(([, u]) => u.calls >= 3).length,
    heavyCallers: userList.filter(([, u]) => u.calls >= 5).length,
    topCallers,
  };
}

function buildEscalationData(calls) {
  const escalated = calls.filter(c => c.agentActions.some(a => a.action.includes('Escalat') || a.action.includes('Ticket')));
  const byProduct = {};
  const byCategory = {};

  for (const c of escalated) {
    byProduct[c.product] = (byProduct[c.product] || 0) + 1;
    const key = c.product + ' → ' + c.category;
    byCategory[key] = byCategory[key] || { product: c.product, category: c.category, count: 0 };
    byCategory[key].count++;
  }

  return {
    total: escalated.length,
    percentage: Math.round(escalated.length / calls.length * 100),
    byProduct: Object.entries(byProduct).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ product: k, count: v })),
    topCategories: Object.values(byCategory).sort((a, b) => b.count - a.count).slice(0, 15),
  };
}

function buildRegionData(calls) {
  const regions = {};
  for (const c of calls) {
    const r = c.circle || 'Unknown';
    if (!(r in regions)) regions[r] = { calls: 0, angry: 0, escalations: 0 };
    regions[r].calls++;
    if (c.sentimentAnalysis.sentiment === 'Angry') regions[r].angry++;
    if (c.agentActions.some(a => a.action.includes('Escalat'))) regions[r].escalations++;
  }
  return Object.entries(regions).sort((a, b) => b[1].calls - a[1].calls).slice(0, 15).map(([k, v]) => ({ region: k, ...v }));
}

// ─── Deep Sentiment Analytics ───
function buildSentimentDeepDive(calls) {
  const totalCalls = calls.length;

  // Sentiment distribution
  const sentimentDist = { Angry: 0, Frustrated: 0, Confused: 0, Neutral: 0, Satisfied: 0 };
  const frustrationReasons = {}; // why are people frustrated?
  const angerReasons = {};

  // Tonality breakdown
  const customerTones = {};
  const agentTones = {};
  let escalationPatternCount = 0;
  let toneShifts = { "calm→angry": 0, "angry→calm": 0 };

  // Helping / Not Helping aggregation
  const helpingFactors = {};
  const notHelpingFactors = {};
  let totalHelpScore = 0;
  let totalHurtScore = 0;

  // Per-product frustration
  const productFrustration = {};

  // Duration vs sentiment correlation
  const sentimentByDuration = {
    "< 2 min": { total: 0, angry: 0, frustrated: 0, satisfied: 0 },
    "2-5 min": { total: 0, angry: 0, frustrated: 0, satisfied: 0 },
    "5-10 min": { total: 0, angry: 0, frustrated: 0, satisfied: 0 },
    "10-20 min": { total: 0, angry: 0, frustrated: 0, satisfied: 0 },
    "> 20 min": { total: 0, angry: 0, frustrated: 0, satisfied: 0 },
  };

  // Frustrated call examples (top 20 by score)
  const frustCalls = [];

  for (const c of calls) {
    const sa = c.sentimentAnalysis || {};
    const sentiment = sa.sentiment || "Neutral";
    const signals = sa.signals || {};
    const tonality = sa.tonality || {};
    const ha = sa.helpingAnalysis || {};

    sentimentDist[sentiment]++;

    // Product frustration
    if (!productFrustration[c.product]) productFrustration[c.product] = { total: 0, frustrated: 0, angry: 0 };
    productFrustration[c.product].total++;
    if (sentiment === "Frustrated") productFrustration[c.product].frustrated++;
    if (sentiment === "Angry") productFrustration[c.product].angry++;

    // Duration vs sentiment
    const durMin = c.duration / 60;
    let bucket;
    if (durMin < 2) bucket = "< 2 min";
    else if (durMin < 5) bucket = "2-5 min";
    else if (durMin < 10) bucket = "5-10 min";
    else if (durMin < 20) bucket = "10-20 min";
    else bucket = "> 20 min";

    sentimentByDuration[bucket].total++;
    if (sentiment === "Angry") sentimentByDuration[bucket].angry++;
    if (sentiment === "Frustrated") sentimentByDuration[bucket].frustrated++;
    if (sentiment === "Satisfied") sentimentByDuration[bucket].satisfied++;

    // Tonality
    if (tonality.customerTone) {
      customerTones[tonality.customerTone] = (customerTones[tonality.customerTone] || 0) + 1;
    }
    if (tonality.agentTone) {
      agentTones[tonality.agentTone] = (agentTones[tonality.agentTone] || 0) + 1;
    }
    if (tonality.escalationPattern) escalationPatternCount++;
    if (tonality.toneShift) toneShifts[tonality.toneShift] = (toneShifts[tonality.toneShift] || 0) + 1;

    // Helping / Not Helping
    if (ha.helping) {
      for (const h of ha.helping) {
        helpingFactors[h.factor] = (helpingFactors[h.factor] || 0) + 1;
      }
    }
    if (ha.notHelping) {
      for (const nh of ha.notHelping) {
        notHelpingFactors[nh.factor] = (notHelpingFactors[nh.factor] || 0) + 1;
      }
    }
    totalHelpScore += (ha.helpScore || 0);
    totalHurtScore += (ha.hurtScore || 0);

    // Collect frustrated calls for examples
    if ((sentiment === "Frustrated" || sentiment === "Angry") && c.customerQuote) {
      frustCalls.push({
        call_sid: c.call_sid,
        product: c.product,
        category: c.category,
        sentiment,
        sentimentScore: sa.sentimentScore || 0,
        signals,
        tonality: {
          customerTone: tonality.customerTone,
          agentTone: tonality.agentTone,
          escalationPattern: tonality.escalationPattern,
          toneShift: tonality.toneShift,
        },
        helping: ha.helping || [],
        notHelping: ha.notHelping || [],
        duration: c.durationMin,
        quote: c.customerQuote,
        quoteEnglish: paraphraseQuote(c.customerQuote),
        summary: c.summary,
        circle: c.circle,
        phone: c.phone ? c.phone.replace(/\d{6}(\d{4})$/, '******$1') : '',
      });
    }

    // Frustration reasons (from category)
    if (sentiment === "Frustrated" || sentiment === "Angry") {
      const reason = `${c.product} → ${c.category}`;
      if (sentiment === "Frustrated") frustrationReasons[reason] = (frustrationReasons[reason] || 0) + 1;
      if (sentiment === "Angry") angerReasons[reason] = (angerReasons[reason] || 0) + 1;
    }
  }

  // Sort frustrated calls by score
  frustCalls.sort((a, b) => (b.sentimentScore + (b.tonality.escalationPattern ? 10 : 0)) - (a.sentimentScore + (a.tonality.escalationPattern ? 10 : 0)));

  const frustratedPct = Math.round((sentimentDist.Frustrated + sentimentDist.Angry) / totalCalls * 100);
  const angryPct = Math.round(sentimentDist.Angry / totalCalls * 100);
  const satisfiedPct = Math.round(sentimentDist.Satisfied / totalCalls * 100);

  return {
    totalCalls,
    sentimentDist,
    frustratedPct,
    angryPct,
    satisfiedPct,

    // Per-product frustration rates
    productFrustration: Object.entries(productFrustration)
      .map(([p, v]) => ({ product: p, total: v.total, frustrated: v.frustrated, angry: v.angry, pct: Math.round((v.frustrated + v.angry) / v.total * 100) }))
      .sort((a, b) => b.pct - a.pct),

    // Duration correlation
    sentimentByDuration,

    // Tonality
    customerTones: Object.entries(customerTones).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ tone: k, count: v, pct: Math.round(v / totalCalls * 100) })),
    agentTones: Object.entries(agentTones).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ tone: k, count: v, pct: Math.round(v / totalCalls * 100) })),
    escalationPatternCount,
    toneShifts,

    // Top frustration reasons
    topFrustrationReasons: Object.entries(frustrationReasons).sort((a, b) => b[1] - a[1]).slice(0, 15).map(([k, v]) => ({ reason: k, count: v })),
    topAngerReasons: Object.entries(angerReasons).sort((a, b) => b[1] - a[1]).slice(0, 15).map(([k, v]) => ({ reason: k, count: v })),

    // What's Helping / Not Helping
    helpingFactors: Object.entries(helpingFactors).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ factor: k, count: v, pct: Math.round(v / totalCalls * 100) })),
    notHelpingFactors: Object.entries(notHelpingFactors).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ factor: k, count: v, pct: Math.round(v / totalCalls * 100) })),
    avgHelpScore: Math.round(totalHelpScore / totalCalls * 100) / 100,
    avgHurtScore: Math.round(totalHurtScore / totalCalls * 100) / 100,

    // Top frustrated call examples
    topFrustratedCalls: frustCalls.slice(0, 30),
  };
}

// ─── Technical Issues / App Bugs ───
function buildTechIssues(calls) {
  const techCategories = [
    'App / Login / Technical Issue',
    'App Crash / Login Failed',
    'Video KYC Issues',
  ];
  const techCalls = calls.filter(c =>
    techCategories.some(tc => c.category.includes(tc) || c.category.includes('App') || c.category.includes('Technical') || c.category.includes('Login Failed')) ||
    c.painSignals.some(p => p.signal.includes('App') || p.signal.includes('Technical') || p.signal.includes('Platform'))
  );

  // ─── Transcript-based tech issue bucket classification ───
  // Priority-ordered: first match wins. Patterns match against actual transcript text.
  const ISSUE_BUCKETS = [
    { bucket: 'OTP / Verification Failure', pattern: /otp/i },
    { bucket: 'Password / MPIN Reset', pattern: /password|mpin|pin.*reset|forgot.*pin/i },
    { bucket: 'KYC / Document Issue', pattern: /(kyc|vkyc|video.*kyc|ckyc).{0,20}(fail|reject|pending|issue|stuck|error|problem|nahi|complete)|pan.{0,10}(reject|mismatch|verify|fail)|document.{0,10}(upload|reject|fail)|selfie/i },
    { bucket: 'App Update / Install', pattern: /app.{0,10}update|update.{0,10}app|play.store|app.*store|reinstall|uninstall|install.*app|new.*version|latest.*version|force.*update/i },
    { bucket: 'Payment / Transaction Error', pattern: /payment.{0,15}(fail|stuck|pending|nahi|error|issue|decline)|transaction.{0,10}fail|paisa.{0,10}(kat|deduct)|double.*debit|refund.*fail/i },
    { bucket: 'UPI / Mandate Issue', pattern: /upi.{0,15}(fail|error|issue|nahi|not)|mandate.{0,10}(fail|reject|issue)|nach.{0,10}(fail|reject)|autopay.{0,10}(fail|issue)/i },
    { bucket: 'Network / Connectivity', pattern: /(server|network|internet|connection).{0,10}(error|down|fail|issue|problem|slow)|something.*went.*wrong|try.*again.*later|load.*nahi|loading/i },
    { bucket: 'Account Access / Login', pattern: /login|sign.*in|log.*in|account.{0,10}(lock|access|block|suspend)/i },
    { bucket: 'App Crash / Not Working', pattern: /crash|band.*ho|not.*work|nahi.*chal|hang|stuck|blank|error/i },
  ];

  function classifyIssueBucket(call) {
    const txt = call.transcript || '';
    for (const b of ISSUE_BUCKETS) {
      if (b.pattern.test(txt)) return b.bucket;
    }
    return 'General Tech Issue';
  }

  const byProduct = {};
  const byIssueType = {};
  const bySeverity = { critical: 0, high: 0, medium: 0, low: 0 };
  const byIssueBucket = {};
  const sampleCalls = {}; // keyed by bucket name

  for (const c of techCalls) {
    // By product
    if (!byProduct[c.product]) byProduct[c.product] = { product: c.product, count: 0, angry: 0, frustrated: 0, resolved: 0, totalDur: 0 };
    byProduct[c.product].count++;
    byProduct[c.product].totalDur += c.duration || 0;
    const sent = c.sentimentAnalysis.sentiment;
    if (sent === 'Angry') byProduct[c.product].angry++;
    if (sent === 'Frustrated') byProduct[c.product].frustrated++;
    if (c.agentActions.some(a => a.action.includes('Resolved'))) byProduct[c.product].resolved++;

    // By issue type (category-level)
    const issueType = c.category;
    if (!byIssueType[issueType]) byIssueType[issueType] = { type: issueType, count: 0, products: {} };
    byIssueType[issueType].count++;
    byIssueType[issueType].products[c.product] = (byIssueType[issueType].products[c.product] || 0) + 1;

    // Severity from pain signals
    for (const p of c.painSignals) {
      bySeverity[p.severity] = (bySeverity[p.severity] || 0) + 1;
    }

    // ─── By Issue Bucket (transcript-based) ───
    const bucket = classifyIssueBucket(c);
    if (!byIssueBucket[bucket]) byIssueBucket[bucket] = { bucket, count: 0, angry: 0, frustrated: 0, resolved: 0, products: {}, sampleQuotes: [] };
    byIssueBucket[bucket].count++;
    if (sent === 'Angry') byIssueBucket[bucket].angry++;
    if (sent === 'Frustrated') byIssueBucket[bucket].frustrated++;
    if (c.agentActions.some(a => a.action.includes('Resolved'))) byIssueBucket[bucket].resolved++;
    byIssueBucket[bucket].products[c.product] = (byIssueBucket[bucket].products[c.product] || 0) + 1;
    if (byIssueBucket[bucket].sampleQuotes.length < 2 && c.customerQuote && c.customerQuote.length > 20) {
      byIssueBucket[bucket].sampleQuotes.push((c.customerQuote || '').slice(0, 100));
    }

    // Sample calls per bucket — 3 per bucket max
    if (!sampleCalls[bucket]) sampleCalls[bucket] = [];
    if (sampleCalls[bucket].length < 3) {
      sampleCalls[bucket].push({
        call_sid: c.call_sid,
        product: c.product,
        category: c.category,
        sentiment: sent,
        duration: c.durationMin,
        department: c.department,
        recordingUrl: c.recordingUrl,
        quote: (c.customerQuote || '').slice(0, 150),
        summary: c.summary,
        issueBucket: bucket,
      });
    }
  }

  // ─── Product × Issue Bucket matrix ───
  const productIssueMat = {};
  for (const c of techCalls) {
    const bucket = classifyIssueBucket(c);
    if (!productIssueMat[c.product]) productIssueMat[c.product] = {};
    productIssueMat[c.product][bucket] = (productIssueMat[c.product][bucket] || 0) + 1;
  }
  const allBucketNames = [...new Set(techCalls.map(c => classifyIssueBucket(c)))];
  const productIssueMatrix = Object.entries(productIssueMat)
    .sort((a, b) => Object.values(b[1]).reduce((s, v) => s + v, 0) - Object.values(a[1]).reduce((s, v) => s + v, 0))
    .map(([prod, buckets]) => ({
      product: prod,
      total: Object.values(buckets).reduce((s, v) => s + v, 0),
      buckets: allBucketNames.map(bn => ({ bucket: bn, count: buckets[bn] || 0 })),
    }));

  // ─── Product Education Gaps — what users don't understand per product ───
  const EDU_PATTERNS = [
    { gap: 'How to use app / navigate', pattern: /kaise.{0,15}(kare|use|open|access|chal)|how.{0,15}(to|do|can).{0,10}(use|navigate|find|access|open)|samajh.*nahi|understand.*not|confus/i },
    { gap: 'Where is my money / status', pattern: /paisa.*kahan|money.*where|where.*money|amount.*kahan|status.*kya|kya.*hua|what.*happen|track|where.*invest|kidhar.*gaya/i },
    { gap: 'Interest / returns confusion', pattern: /interest.*kaise|interest.*kitna|interest.*rate.*kya|return.*kitna|return.*kaise|yield|coupon.*rate|earnings.*kaise|profit.*kitna/i },
    { gap: 'Maturity / payout process', pattern: /maturity.*kab|maturity.*kaise|payout.*kab|payout.*kaise|redeem.*kaise|withdraw.*kaise|pre.*mature|jab.*mature|when.*mature|when.*payout/i },
    { gap: 'Tax / TDS confusion', pattern: /tds.*kaise|tds.*kitna|tax.*kaise|tax.*kitna|tax.*deduct|tds.*deduct|form.*16|26as|tax.*certificate|tds.*certificate/i },
    { gap: 'KYC process confusion', pattern: /kyc.*kaise|kyc.*kya|kyc.*karna|how.*kyc|why.*kyc|video.*kyc.*kaise|pan.*kaise|aadhaar.*kaise|document.*kaise|document.*kya/i },
    { gap: 'Account linking / bank', pattern: /bank.*add|bank.*change|bank.*link|account.*add|account.*link|account.*change|bank.*kaise.*jode|naya.*bank|new.*bank/i },
    { gap: 'Charges / fees confusion', pattern: /charge.*kya|charge.*kitna|fee.*kitna|fee.*kya|penalty.*kitna|penalty.*kya|hidden.*charge|extra.*charge|deduction.*kya|cut.*kya/i },
    { gap: 'Cancellation / refund process', pattern: /cancel.*kaise|cancel.*karna|refund.*kaise|refund.*kab|refund.*kitne.*din|cancel.*process|how.*cancel|how.*refund/i },
    { gap: 'Nominee / document update', pattern: /nominee.*kaise|nominee.*change|nominee.*add|document.*update|details.*change|name.*change|address.*change|mobile.*change|email.*change/i },
  ];

  const eduGaps = {};
  for (const c of calls) { // scan ALL calls, not just tech
    const txt = c.transcript || '';
    for (const ep of EDU_PATTERNS) {
      if (ep.pattern.test(txt)) {
        const key = c.product + '||' + ep.gap;
        if (!eduGaps[key]) eduGaps[key] = { product: c.product, gap: ep.gap, count: 0, frustrated: 0, angry: 0, sampleQuote: '' };
        eduGaps[key].count++;
        const sent = c.sentimentAnalysis.sentiment;
        if (sent === 'Frustrated') eduGaps[key].frustrated++;
        if (sent === 'Angry') eduGaps[key].angry++;
        if (!eduGaps[key].sampleQuote && c.customerQuote && c.customerQuote.length > 20) {
          eduGaps[key].sampleQuote = (c.customerQuote || '').slice(0, 100);
        }
      }
    }
  }

  const productEduGaps = {};
  for (const g of Object.values(eduGaps)) {
    if (!productEduGaps[g.product]) productEduGaps[g.product] = [];
    productEduGaps[g.product].push({
      gap: g.gap,
      count: g.count,
      frustrated: g.frustrated,
      angry: g.angry,
      frustPct: g.count > 0 ? Math.round((g.frustrated + g.angry) / g.count * 100) : 0,
      sampleQuote: g.sampleQuote,
    });
  }
  for (const prod of Object.keys(productEduGaps)) {
    productEduGaps[prod].sort((a, b) => b.count - a.count);
  }

  return {
    totalTechCalls: techCalls.length,
    pctOfTotal: Math.round(techCalls.length / calls.length * 100),
    byProduct: Object.values(byProduct).sort((a, b) => b.count - a.count).map(p => ({
      ...p,
      avgDur: Math.round(p.totalDur / p.count / 60 * 10) / 10,
      frustPct: Math.round((p.angry + p.frustrated) / p.count * 100),
      resolvePct: Math.round(p.resolved / p.count * 100),
    })),
    byIssueType: Object.values(byIssueType).sort((a, b) => b.count - a.count).map(t => ({
      ...t,
      products: Object.entries(t.products).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ product: k, count: v })),
    })),
    bySeverity,
    byIssueBucket: Object.values(byIssueBucket).sort((a, b) => b.count - a.count).map(b => ({
      ...b,
      frustPct: b.count > 0 ? Math.round((b.angry + b.frustrated) / b.count * 100) : 0,
      resolvePct: b.count > 0 ? Math.round(b.resolved / b.count * 100) : 0,
      products: Object.entries(b.products).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ product: k, count: v })),
    })),
    productIssueMatrix,
    allBucketNames,
    productEduGaps,
    sampleCalls: Object.values(sampleCalls).flat(),
    bucketSamples: sampleCalls,
  };
}

// ─── Enhanced Escalations ───
function buildEnhancedEscalations(calls) {
  const escalated = calls.filter(c => c.agentActions.some(a =>
    a.action.includes('Escalat') || a.action.includes('Ticket') || a.action.includes('RM')
  ));

  const byProduct = {};
  const byCategory = {};
  const byChannel = {};
  const reasons = {};
  const byResolution = { resolved: 0, unresolved: 0, transferred: 0 };
  const bySentiment = {};
  let totalDur = 0;

  for (const c of escalated) {
    totalDur += c.duration || 0;
    const sent = c.sentimentAnalysis.sentiment;
    bySentiment[sent] = (bySentiment[sent] || 0) + 1;

    // By product
    if (!byProduct[c.product]) byProduct[c.product] = { product: c.product, count: 0, resolved: 0, angry: 0 };
    byProduct[c.product].count++;
    if (sent === 'Angry') byProduct[c.product].angry++;
    if (c.agentActions.some(a => a.action.includes('Resolved'))) { byProduct[c.product].resolved++; byResolution.resolved++; }
    else if (c.agentActions.some(a => a.action.includes('Transfer'))) { byResolution.transferred++; }
    else { byResolution.unresolved++; }

    // By category
    const key = c.product + ' → ' + c.category;
    if (!byCategory[key]) byCategory[key] = { product: c.product, category: c.category, count: 0, avgDur: 0, totalDur: 0, angry: 0 };
    byCategory[key].count++;
    byCategory[key].totalDur += c.duration || 0;
    if (sent === 'Angry') byCategory[key].angry++;

    // By channel
    const ch = c.department || 'Unknown';
    if (!byChannel[ch]) byChannel[ch] = { channel: ch, count: 0, resolved: 0 };
    byChannel[ch].count++;
    if (c.agentActions.some(a => a.action.includes('Resolved'))) byChannel[ch].resolved++;

    // Escalation reasons (from agent actions)
    for (const a of c.agentActions) {
      if (a.action.includes('Escalat') || a.action.includes('Ticket') || a.action.includes('RM')) {
        const reason = a.reason || a.action;
        if (!reasons[reason]) reasons[reason] = { reason, count: 0, products: {} };
        reasons[reason].count++;
        reasons[reason].products[c.product] = (reasons[reason].products[c.product] || 0) + 1;
      }
    }
  }

  // Compute avg durations for categories
  for (const cat of Object.values(byCategory)) {
    cat.avgDur = Math.round(cat.totalDur / cat.count / 60 * 10) / 10;
  }

  // ── What frustrated users asked for / wanted as next steps ──
  const frustAsks = {};
  const nextSteps = {};
  for (const c of escalated) {
    const sent = c.sentimentAnalysis.sentiment;
    if (sent !== 'Angry' && sent !== 'Frustrated') continue;

    // Extract what the user wanted from category + summary
    const ask = c.category;
    if (!frustAsks[ask]) frustAsks[ask] = { ask, count: 0, products: {} };
    frustAsks[ask].count++;
    frustAsks[ask].products[c.product] = (frustAsks[ask].products[c.product] || 0) + 1;

    // Extract what agent offered as next step
    for (const a of c.agentActions) {
      const step = a.action;
      if (!nextSteps[step]) nextSteps[step] = { step, count: 0, products: {} };
      nextSteps[step].count++;
      nextSteps[step].products[c.product] = (nextSteps[step].products[c.product] || 0) + 1;
    }
  }

  const frustratedAsks = Object.values(frustAsks).sort((a, b) => b.count - a.count).slice(0, 20).map(a => ({
    ...a,
    products: Object.entries(a.products).sort((x, y) => y[1] - x[1]).map(([k, v]) => ({ product: k, count: v })),
  }));
  const frustratedNextSteps = Object.values(nextSteps).sort((a, b) => b.count - a.count).slice(0, 15).map(s => ({
    ...s,
    products: Object.entries(s.products).sort((x, y) => y[1] - x[1]).map(([k, v]) => ({ product: k, count: v })),
  }));

  const totalFrustratedEscalated = escalated.filter(c => c.sentimentAnalysis.sentiment === 'Angry' || c.sentimentAnalysis.sentiment === 'Frustrated').length;

  // Top escalated calls with recording
  const topEscCalls = escalated
    .filter(c => c.sentimentAnalysis.sentiment === 'Angry' || c.sentimentAnalysis.sentiment === 'Frustrated')
    .sort((a, b) => (b.sentimentAnalysis.sentimentScore || 0) - (a.sentimentAnalysis.sentimentScore || 0))
    .slice(0, 15)
    .map(c => ({
      call_sid: c.call_sid,
      product: c.product,
      category: c.category,
      sentiment: c.sentimentAnalysis.sentiment,
      duration: c.durationMin,
      department: c.department,
      recordingUrl: c.recordingUrl,
      quote: (c.customerQuote || '').slice(0, 120),
      summary: c.summary,
    }));

  return {
    total: escalated.length,
    percentage: Math.round(escalated.length / calls.length * 100),
    avgDuration: Math.round(totalDur / (escalated.length || 1) / 60 * 10) / 10,
    byProduct: Object.values(byProduct).sort((a, b) => b.count - a.count).map(p => ({
      ...p,
      resolvePct: Math.round(p.resolved / p.count * 100),
      pctOfDept: Math.round(p.count / calls.filter(c => c.product === p.product).length * 100),
    })),
    topCategories: Object.values(byCategory).sort((a, b) => b.count - a.count).slice(0, 15),
    byChannel: Object.values(byChannel).sort((a, b) => b.count - a.count).map(ch => ({
      ...ch,
      resolvePct: Math.round(ch.resolved / ch.count * 100),
    })),
    reasons: Object.values(reasons).sort((a, b) => b.count - a.count).slice(0, 15).map(r => ({
      ...r,
      products: Object.entries(r.products).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ product: k, count: v })),
    })),
    byResolution,
    bySentiment,
    topEscCalls,
    frustratedAsks,
    frustratedNextSteps,
    totalFrustratedEscalated,
  };
}

// ─── User Intent Segmentation ───
function buildIntentSegmentation(calls) {
  // Post-conversion categories (user already invested/bought something, now stuck)
  const postConversionPatterns = [
    /matured|maturity|payout|withdrawal|redeem|refund|coupon|interest.*not.*received|money.*not.*received|pre.*close|pre.*mature|auto.*renewal|nominee|bank.*change|demat|migration|transfer.*out|cancel.*after|statement|receipt|tds|tax|certificate/i,
  ];
  // Pre-conversion categories (user trying to buy/invest, getting stuck)
  const preConversionPatterns = [
    /booking.*fail|invest|purchase|buy|apply|book.*fd|book.*bond|activation|delivery|limit|upgrade|kyc|onboard|sign.*up|register|unable.*to.*book|payment.*fail|how.*to|rate|compare|which|plan/i,
  ];

  const postConvCalls = [];
  const preConvCalls = [];
  const otherCalls = [];

  for (const c of calls) {
    const catLower = c.category.toLowerCase();
    const summaryLower = (c.summary || '').toLowerCase();
    const combined = catLower + ' ' + summaryLower;

    if (postConversionPatterns.some(p => p.test(combined))) {
      postConvCalls.push(c);
    } else if (preConversionPatterns.some(p => p.test(combined))) {
      preConvCalls.push(c);
    } else {
      otherCalls.push(c);
    }
  }

  function buildSegment(segCalls, label) {
    const byProduct = {};
    const byCategory = {};
    const byChannel = {};
    const bySentiment = {};
    let angry = 0, frustrated = 0, escalated = 0;

    for (const c of segCalls) {
      const sent = c.sentimentAnalysis.sentiment;
      bySentiment[sent] = (bySentiment[sent] || 0) + 1;
      if (sent === 'Angry') angry++;
      if (sent === 'Frustrated') frustrated++;
      if (c.agentActions.some(a => a.action.includes('Escalat') || a.action.includes('Ticket'))) escalated++;

      byProduct[c.product] = (byProduct[c.product] || 0) + 1;
      const key = c.product + ' → ' + c.category;
      byCategory[key] = (byCategory[key] || 0) + 1;
      const ch = c.department || 'Unknown';
      byChannel[ch] = (byChannel[ch] || 0) + 1;
    }

    // Top calls in this segment
    const topCalls = segCalls
      .filter(c => c.customerQuote && c.customerQuote.length > 10)
      .sort((a, b) => b.storyScore - a.storyScore)
      .slice(0, 10)
      .map(c => ({
        call_sid: c.call_sid,
        product: c.product,
        category: c.category,
        sentiment: c.sentimentAnalysis.sentiment,
        duration: c.durationMin,
        department: c.department,
        recordingUrl: c.recordingUrl,
        quote: (c.customerQuote || '').slice(0, 120),
        summary: c.summary,
      }));

    return {
      label,
      total: segCalls.length,
      pct: Math.round(segCalls.length / calls.length * 100),
      angry,
      frustrated,
      escalated,
      frustPct: Math.round((angry + frustrated) / (segCalls.length || 1) * 100),
      escPct: Math.round(escalated / (segCalls.length || 1) * 100),
      bySentiment,
      byProduct: Object.entries(byProduct).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ product: k, count: v })),
      topCategories: Object.entries(byCategory).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([k, v]) => ({ category: k, count: v })),
      byChannel: Object.entries(byChannel).sort((a, b) => b[1] - a[1]).map(([k, v]) => ({ channel: k, count: v })),
      topCalls,
    };
  }

  return {
    postConversion: buildSegment(postConvCalls, 'Post-Conversion (Already Invested, Stuck)'),
    preConversion: buildSegment(preConvCalls, 'Pre-Conversion (Trying to Invest, Stuck)'),
    other: buildSegment(otherCalls, 'General Inquiries'),
  };
}

function buildLanguageBreakdown(calls) {
  const langCounts = {};
  const langByProduct = {};
  const langBySentiment = {};
  const langDurations = {};
  const langFrustrated = {};
  const sampleCalls = {};

  for (const c of calls) {
    const lang = c.customerLanguage || 'Unknown';
    langCounts[lang] = (langCounts[lang] || 0) + 1;

    // By product
    if (!langByProduct[c.product]) langByProduct[c.product] = {};
    langByProduct[c.product][lang] = (langByProduct[c.product][lang] || 0) + 1;

    // By sentiment
    const sent = c.sentimentAnalysis?.sentiment || 'Neutral';
    if (!langBySentiment[lang]) langBySentiment[lang] = {};
    langBySentiment[lang][sent] = (langBySentiment[lang][sent] || 0) + 1;

    // Avg duration per language
    if (!langDurations[lang]) langDurations[lang] = { total: 0, count: 0 };
    langDurations[lang].total += c.duration || 0;
    langDurations[lang].count++;

    // Frustration rate per language
    if (!langFrustrated[lang]) langFrustrated[lang] = { frustrated: 0, total: 0 };
    langFrustrated[lang].total++;
    if (sent === 'Angry' || sent === 'Frustrated') langFrustrated[lang].frustrated++;

    // Sample calls per language (3 max, prefer ones with longer transcripts)
    if (!sampleCalls[lang]) sampleCalls[lang] = [];
    if (sampleCalls[lang].length < 3 && c.transcript.length > 100) {
      sampleCalls[lang].push({
        call_sid: c.call_sid,
        product: c.product,
        summary: c.summary,
        sentiment: c.sentimentAnalysis?.sentiment,
        durationMin: c.durationMin,
        customerQuote: c.customerQuote?.slice(0, 150) || '',
        recordingUrl: c.recordingUrl || '',
        hasRecording: c.hasRecording || false,
      });
    }
  }

  // Build language distribution sorted by count
  const distribution = Object.entries(langCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([lang, count]) => ({
      language: lang,
      count,
      percentage: Math.round(count / calls.length * 100),
      avgDurationMin: langDurations[lang] ? Math.round(langDurations[lang].total / langDurations[lang].count / 60 * 10) / 10 : 0,
      frustrationRate: langFrustrated[lang] ? Math.round(langFrustrated[lang].frustrated / langFrustrated[lang].total * 100) : 0,
      sentiments: langBySentiment[lang] || {},
    }));

  // Product × Language matrix
  const productLangMatrix = Object.entries(langByProduct)
    .sort((a, b) => {
      const totalA = Object.values(a[1]).reduce((s, v) => s + v, 0);
      const totalB = Object.values(b[1]).reduce((s, v) => s + v, 0);
      return totalB - totalA;
    })
    .map(([product, langs]) => ({ product, ...langs }));

  const allLanguages = Object.keys(langCounts).sort((a, b) => (langCounts[b] || 0) - (langCounts[a] || 0));

  return {
    totalCalls: calls.length,
    distribution,
    productLangMatrix,
    allLanguages,
    sampleCalls,
  };
}

function buildStats(calls) {
  const products = {};
  const sentiments = { Angry: 0, Frustrated: 0, Confused: 0, Neutral: 0, Satisfied: 0 };

  for (const c of calls) {
    // Product stats
    if (!products[c.product]) products[c.product] = { total: 0, avgDur: 0, totalDur: 0, subcategories: {}, agentActions: {}, sentiments: { Angry: 0, Frustrated: 0, Confused: 0, Neutral: 0, Satisfied: 0 } };
    products[c.product].total++;
    products[c.product].totalDur += c.duration;

    // Sentiment
    sentiments[c.sentimentAnalysis.sentiment]++;
    products[c.product].sentiments[c.sentimentAnalysis.sentiment]++;

    // Subcategory stats
    if (!products[c.product].subcategories[c.category])
      products[c.product].subcategories[c.category] = { count: 0, avgDur: 0, totalDur: 0, reason: c.categoryReason, sentiments: { Angry: 0, Frustrated: 0, Confused: 0, Neutral: 0, Satisfied: 0 } };
    products[c.product].subcategories[c.category].count++;
    products[c.product].subcategories[c.category].totalDur += c.duration;
    products[c.product].subcategories[c.category].sentiments[c.sentimentAnalysis.sentiment]++;

    // Agent action stats
    for (const a of c.agentActions) {
      if (!products[c.product].agentActions[a.action]) products[c.product].agentActions[a.action] = 0;
      products[c.product].agentActions[a.action]++;
    }
  }

  // Compute averages
  for (const p of Object.values(products)) {
    p.avgDur = Math.round(p.totalDur / p.total / 60 * 10) / 10;
    for (const s of Object.values(p.subcategories)) {
      s.avgDur = Math.round(s.totalDur / s.count / 60 * 10) / 10;
    }
  }

  // Top categories
  const categories = {};
  for (const c of calls) {
    const key = `${c.product} > ${c.category}`;
    if (!categories[key]) categories[key] = { product: c.product, category: c.category, count: 0, reason: c.categoryReason };
    categories[key].count++;
  }

  return {
    totalCalls: calls.length,
    products,
    sentiments,
    topCategories: Object.values(categories).sort((a, b) => b.count - a.count).slice(0, 30),
  };
}

// ─── Serve ───
let data;

try {
  // Always reclassify with updated engine
  data = loadAndClassify();
} catch (e) {
  console.error("Classification failed:", e.message);
  if (fs.existsSync(DATA_PATH)) {
    data = JSON.parse(fs.readFileSync(DATA_PATH, "utf-8"));
  } else {
    process.exit(1);
  }
}

const MIME = {
  ".html": "text/html", ".js": "application/javascript", ".css": "text/css",
  ".json": "application/json", ".png": "image/png", ".svg": "image/svg+xml",
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET");
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

  // ─── API ───
  if (url.pathname === "/api/stats") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.stats));
  }

  if (url.pathname === "/api/stories") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.stories || []));
  }

  if (url.pathname === "/api/curated-stories") {
    res.writeHead(200, { "Content-Type": "application/json" });
    try {
      const curated = JSON.parse(fs.readFileSync(CURATED_PATH, "utf-8"));
      return res.end(JSON.stringify(curated));
    } catch(e) {
      return res.end(JSON.stringify([]));
    }
  }

  if (url.pathname === "/api/users") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.userStats || {}));
  }

  if (url.pathname === "/api/escalations") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.escalationData || {}));
  }

  if (url.pathname === "/api/regions") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.regionData || []));
  }

  if (url.pathname === "/api/sentiment-deep-dive") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.sentimentDeepDive || {}));
  }

  if (url.pathname === "/api/calls") {
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

    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ calls: paged, total, page, pages: Math.ceil(total / limit) }));
  }

  if (url.pathname.startsWith("/api/call/")) {
    const sid = url.pathname.split("/api/call/")[1];
    const call = data.calls.find(c => c.call_sid === sid);
    if (!call) {
      res.writeHead(404, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "Call not found" }));
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(call));
  }

  // ─── Tech Issues API ───
  if (url.pathname === "/api/tech-issues") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.techIssues || {}));
  }

  // ─── Enhanced Escalations API ───
  if (url.pathname === "/api/enhanced-escalations") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.enhancedEscalations || {}));
  }

  // ─── Intent Segmentation API ───
  if (url.pathname === "/api/intent-segmentation") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.intentSegmentation || {}));
  }

  // ─── Language Breakdown API ───
  if (url.pathname === "/api/language-breakdown") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.languageBreakdown || {}));
  }

  // ─── Advanced Insights API ───
  if (url.pathname === "/api/advanced-insights") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(data.advancedInsights || {}));
  }

  // ─── Claude AI Analysis (single call) ───
  if (url.pathname === "/api/claude-analyze" && req.method === "POST") {
    let body = "";
    req.on("data", chunk => body += chunk);
    req.on("end", async () => {
      try {
        const { call_sid } = JSON.parse(body);
        const call = data.calls.find(c => c.call_sid === call_sid);
        if (!call) {
          res.writeHead(404, { "Content-Type": "application/json" });
          return res.end(JSON.stringify({ error: "Call not found" }));
        }
        const analysis = await analyzeCallWithClaude(
          call.transcript,
          { product: call.product, durationMin: call.durationMin, category: call.category }
        );
        res.writeHead(200, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ call_sid, analysis }));
      } catch (e) {
        res.writeHead(500, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ─── Static files ───
  let filePath = path.join(__dirname, "..", "public", url.pathname === "/" ? "index.html" : url.pathname);
  const ext = path.extname(filePath);
  const contentType = MIME[ext] || "text/plain";

  fs.readFile(filePath, (err, content) => {
    if (err) { res.writeHead(404); return res.end("Not found"); }
    res.writeHead(200, { "Content-Type": contentType, "Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0" });
    res.end(content);
  });
});

server.listen(PORT, () => {
  console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`  Call Insights Dashboard v2`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`  ${data.calls.length} calls classified`);
  console.log(`  ${Object.keys(data.stats.products).length} products`);
  console.log(`  Sentiments: ${Object.entries(data.stats.sentiments).map(([k,v])=>`${k}: ${v}`).join(', ')}`);
  console.log(`  ${(data.stories || []).length} user stories extracted`);
  console.log(`  ${(data.userStats || {}).uniqueCallers || '?'} unique callers | ${(data.escalationData || {}).total || '?'} escalations`);
  const sdd = data.sentimentDeepDive || {};
  console.log(`  Frustrated/Angry: ${sdd.frustratedPct||'?'}% | Satisfied: ${sdd.satisfiedPct||'?'}% | Escalation patterns: ${sdd.escalationPatternCount||0}`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);
});
