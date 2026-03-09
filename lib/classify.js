/**
 * Call Classification Engine v2
 * Issue-based categories (what is the customer's ACTUAL problem?)
 * + Sentiment Analysis + User Story extraction
 */

// ─── Product Detection ───
function detectProduct(groupName, transcript) {
  const g = (groupName || "").toLowerCase();
  const t = (transcript || "").toLowerCase();

  if (/credit.?card|stable.?cc|cc_new/i.test(g)) return "Credit Card";
  if (/bond|stable.?bonds|bondsoutbound|relationship.?manager|rm.?module/i.test(g)) return "Bonds";
  if (/mutual.?fund/i.test(g)) return "Mutual Fund";
  if (/fd|fixed.?deposit/i.test(g)) return "FD";
  if (/vkyc/i.test(g)) return "KYC/Onboarding";

  // Transcript-based
  if (/credit card|cc limit|card activat|credit limit|emi convert|card block|card deliver|credit score/i.test(t)) return "Credit Card";
  if (/bond|debenture|ncd|coupon rate|maturity date|isin|sovereign gold/i.test(t)) return "Bonds";
  if (/mutual fund|sip |nav |redemption|amc|folio number/i.test(t)) return "Mutual Fund";
  if (/fixed deposit|fd book|fd rate|fd matur/i.test(t)) return "FD";
  if (/vkyc|video kyc/i.test(t)) return "KYC/Onboarding";

  return "General";
}

// ─── Issue Classification — ISSUE-FIRST, not channel-first ───
// Every rule describes the CUSTOMER'S PROBLEM, not "who they want to talk to"
const ISSUE_RULES = {
  "Credit Card": [
    { sub: "Card Activation / Delivery Status", patterns: [/card activat|card deliver|card nahi aay|card dispatch|card track|card kab aayega|physical card|pin generat|set pin/i], reason: "Customer checking when their credit card will arrive or how to activate it" },
    { sub: "Credit Limit Issue", patterns: [/credit limit|limit increase|limit badha|limit kam|limit enhance|limit reduce|available limit|total limit/i], reason: "Customer asking about credit limit — increase, decrease, or current available limit" },
    { sub: "Payment Failed / Money Deducted", patterns: [/payment fail|payment pending|paisa kat|transaction fail|upi fail|amount deduct|nahi hua credit|payment nahi|bank se kata|double debit|money stuck/i], reason: "Customer's payment failed or money was deducted but not credited — stuck transaction" },
    { sub: "Bill / Statement Confusion", patterns: [/bill|statement|billing|invoice|outstanding|due date|minimum due|total due|billing cycle|generate date/i], reason: "Customer confused about bill amount, due date, statement cycle, or outstanding balance" },
    { sub: "Rewards / Cashback Missing", patterns: [/reward|cashback|points|cash back|redeem point|offer|coupon|benefit/i], reason: "Customer asking about missing rewards, cashback not received, or how to redeem points" },
    { sub: "KYC / Verification Stuck", patterns: [/kyc|verification|aadhaar|pan card|video kyc|vkyc|document upload|selfie|re.?kyc/i], reason: "Customer's KYC is incomplete, stuck, or needs re-verification" },
    { sub: "Late Fee / Wrongful Charges", patterns: [/late fee|interest charge|penalty|finance charge|overdue charge|late payment|wrong charge|extra charge|hidden charge/i], reason: "Customer disputing late fees, interest charges, or unexpected charges on their card" },
    { sub: "Refund Not Received", patterns: [/refund|dispute|chargeback|wrong charge|unauthorized|double charge|merchant.*refund|return.*money/i], reason: "Customer waiting for a refund that hasn't come through or disputing a transaction" },
    { sub: "Card Blocked / Fraud Report", patterns: [/card block|block card|fraud|stolen|lost card|unauthorized transaction|suspicious|compromised/i], reason: "Customer reporting card fraud, unauthorized transactions, or requesting card block" },
    { sub: "EMI Conversion", patterns: [/emi|convert to emi|emi option|installment|no cost emi/i], reason: "Customer wants to convert a transaction to EMI or asking about EMI options" },
    { sub: "App / Login / Technical Issue", patterns: [/app.*issue|app.*problem|app.*error|login issue|otp nahi|app crash|technical|not loading|app open nahi|password|server/i], reason: "Customer unable to login, app crashing, OTP not received, or facing technical errors" },
  ],
  "Bonds": [
    { sub: "How to Buy / Investment Help", patterns: [/buy|purchase|kharid|invest.*bond|lena hai|kaise le|available bond|new bond|minimum invest|how to invest|paisa lagana|bond lena/i], reason: "Customer wants help purchasing bonds or understanding how to invest" },
    { sub: "Tax Document Needed (TDS/15G/Capital Gains)", patterns: [/tax|tds|form 15|capital gain|itr|income tax|15g|15h|p&l|profit.loss|tax certificate|26as|tax deduct/i], reason: "Customer needs tax-related documents — TDS certificate, Form 15G/H, capital gains statement for ITR filing" },
    { sub: "Statement / Holding Report Needed", patterns: [/statement|document|certificate|cas |demat|holding statement|account statement|bond statement|portfolio statement/i], reason: "Customer requesting bond holding statement, CAS, or demat statement" },
    { sub: "Interest / Coupon Not Received", patterns: [/payout|interest.*nahi|coupon.*nahi|interest credit|payment.*nahi|paisa nahi.*aay|interest kab|coupon kab|interest pending/i], reason: "Customer's interest/coupon payment is delayed or not received" },
    { sub: "Maturity — Where Is My Money?", patterns: [/matur|maturity.*money|maturity.*paisa|principal.*return|maturity.*date|bond.*expire|maturity amount|money.*after.*matur/i], reason: "Customer's bond has matured but money hasn't been credited, or asking about maturity process" },
    { sub: "Want to Exit / Sell Early", patterns: [/early exit|sell bond|exit bond|premature|liquid|sell karna|exit karna|withdraw|break bond|encash/i], reason: "Customer wants to exit bonds before maturity or sell on secondary market" },
    { sub: "Returns / Yield Confusion", patterns: [/return|yield|interest rate|kitna milega|kya return|rate of interest|how much.*earn|annual|percent/i], reason: "Customer confused about bond returns, yield calculation, or interest rate" },
    { sub: "App / Login / Technical Issue", patterns: [/app.*issue|app.*problem|app.*error|login issue|otp nahi|app crash|not loading|app open nahi|technical|server|glitch/i], reason: "Customer facing app or technical issues while trying to access bond portfolio" },
    { sub: "KYC / Verification Stuck", patterns: [/kyc|verification|aadhaar|pan|vkyc|document upload|ckyc/i], reason: "Customer's KYC is incomplete or stuck, blocking bond transactions" },
    { sub: "Want RM Callback / Assigned RM", patterns: [/rm.*callback|rm.*call.*back|want.*rm|assign.*rm|my rm|talk.*rm|rm.*number|rm se baat|rm nahi mil/i], reason: "Customer specifically requesting to be connected with their assigned Relationship Manager" },
  ],
  "FD": [
    { sub: "FD Booking Failed / Help Needed", patterns: [/book fd|fd book|create fd|new fd|fd kaise|how to book|fd open|booking fail|fd nahi|cannot book/i], reason: "Customer needs help booking FD or booking is failing" },
    { sub: "Which Bank / Rate Comparison", patterns: [/which bank|bank name|partner bank|bank rate|bank safe|rbl|unity|suryoday|shivalik|compare|best rate|highest rate/i], reason: "Customer comparing FD rates across partner banks or asking about bank safety" },
    { sub: "FD Matured — Money Not Received", patterns: [/matur|payout|fd expire|principal|fd kab mature|auto renew|money.*not.*received|paisa nahi aaya/i], reason: "Customer's FD has matured but money hasn't been credited" },
    { sub: "Interest Rate Query", patterns: [/interest rate|fd rate|kitna rate|rate of interest|best rate|current rate/i], reason: "Customer checking current FD interest rates" },
    { sub: "Want to Break FD Early", patterns: [/premature|early withdraw|break fd|fd todna|fd cancel|close fd/i], reason: "Customer wants to break FD before maturity — asking about penalty" },
    { sub: "KYC / Verification Stuck", patterns: [/kyc|verification|aadhaar|pan|vkyc|document/i], reason: "Customer's KYC blocking FD booking" },
    { sub: "App / Login / Technical Issue", patterns: [/app.*issue|app.*problem|app.*error|login|otp nahi|app crash|not loading/i], reason: "Customer facing technical issues with FD" },
  ],
  "Mutual Fund": [
    { sub: "Redemption — How to Withdraw", patterns: [/redeem|redemption|withdraw|sell mutual|paisa nikalna|fund sell|how to redeem|money back/i], reason: "Customer wants to redeem mutual fund units or withdraw money" },
    { sub: "SIP Start / Stop / Modify", patterns: [/sip|systematic investment|sip start|sip stop|sip modify|sip cancel|step up|auto debit|mandate/i], reason: "Customer asking about starting, stopping, modifying, or cancelling their SIP" },
    { sub: "Which Fund / Investment Advice", patterns: [/invest|which fund|recommend|best fund|fund suggest|allocat|where to invest|good fund/i], reason: "Customer seeking fund recommendations or investment advice" },
    { sub: "Portfolio Down / Performance Query", patterns: [/performance|return|nav|loss|profit|xirr|cagr|how much gain|portfolio.*down|negative|market/i], reason: "Customer worried about portfolio performance, losses, or checking returns" },
    { sub: "KYC / Verification Stuck", patterns: [/kyc|verification|aadhaar|pan|vkyc|ckyc/i], reason: "KYC incomplete, blocking mutual fund transactions" },
    { sub: "App / Login / Technical Issue", patterns: [/app.*issue|app.*problem|app.*error|login|otp nahi|app crash/i], reason: "Technical issues with mutual fund features" },
  ],
  "General": [
    { sub: "App Crash / Login Failed", patterns: [/app.*issue|app.*problem|app.*error|login|otp|app crash|not loading|technical|server error|bug/i], reason: "General app/login issues not specific to any product" },
    { sub: "Account Settings / Profile Change", patterns: [/account|profile|phone number change|email change|deactivat|delete account|update.*detail|nominee/i], reason: "Customer wants to change account settings, phone number, email, or nominee" },
    { sub: "KYC / Verification Stuck", patterns: [/kyc|verification|aadhaar|pan|vkyc|document/i], reason: "General KYC issues" },
    { sub: "How Does Stable Money Work?", patterns: [/stable money|what is|kya hai|information|details|how does|about|platform|company/i], reason: "New user asking general questions about how the platform works" },
    { sub: "Complaint / Want Escalation", patterns: [/complaint|escalat|senior|manager se baat|not happy|dissatisfied|grievance|higher authority/i], reason: "Customer filing a formal complaint or demanding escalation" },
  ],
  "KYC/Onboarding": [
    { sub: "Video KYC Keeps Failing", patterns: [/vkyc|video kyc|kyc fail|kyc disconnect|kyc pending|kyc reject|camera|video call/i], reason: "Video KYC process failing, disconnecting, or being rejected repeatedly" },
    { sub: "Document Upload Issue", patterns: [/document|upload|aadhaar|pan card|selfie|photo|id proof/i], reason: "Issues uploading KYC documents" },
    { sub: "KYC Status — How Long?", patterns: [/kyc status|kyc pending|kyc complete|kyc approved|kyc reject|how long|kitna time/i], reason: "Customer checking how long KYC approval will take" },
  ],
};

// ─── Enhanced Sentiment Analysis v3 — Deep Frustration + Tonality ───
function analyzeSentiment(transcript, durationSec) {
  const t = (transcript || "").toLowerCase();

  // Parse speaker segments for per-speaker analysis
  const segments = [];
  const segRegex = /\[Speaker (\d+)\]\s*([\s\S]*?)(?=\[Speaker \d+\]|$)/g;
  let match;
  while ((match = segRegex.exec(transcript || "")) !== null) {
    segments.push({ speaker: parseInt(match[1]), text: match[2].trim() });
  }
  const customerText = segments.filter(s => s.speaker === 1).map(s => s.text.toLowerCase()).join(" ");
  const agentText = segments.filter(s => s.speaker === 0).map(s => s.text.toLowerCase()).join(" ");

  const signals = {
    frustrated: 0,
    satisfied: 0,
    confused: 0,
    angry: 0,
    neutral: 0,
  };

  // ──────────────────────────────────────────────
  // FRUSTRATED SIGNALS (massively expanded)
  // ──────────────────────────────────────────────

  // Repeat calling / follow-up fatigue
  if (/kab hoga|kitni baar|pehle bhi|phir se call|again calling|bahut time|wait kar|ho kab raha/i.test(t)) signals.frustrated += 2;
  if (/baar baar|bar bar|daily call|roz call|kaafi din|bahut din|so many days|itne din/i.test(t)) signals.frustrated += 2;
  if (/already called|3 times|4 times|5 times|multiple times|kai baar|do baar|teen baar/i.test(t)) signals.frustrated += 3;
  if (/still pending|abhi tak|ab tak nahi|still not|yet to|abhi bhi|still waiting/i.test(t)) signals.frustrated += 2;

  // No response / ghosting
  if (/disappointed|not happy|koi jawab nahi|no response|no reply|reply nahi/i.test(t)) signals.frustrated += 2;
  if (/nobody called|kisi ne call nahi|no callback|callback nahi|koi nahi aaya|nobody reached/i.test(t)) signals.frustrated += 3;
  if (/email bhi kiya|mail bhi|whatsapp bhi|tried everything|sab kiya/i.test(t)) signals.frustrated += 2;

  // Helplessness / stuck
  if (/kya karu|what do i do|kuch nahi ho raha|nothing is happening|koi solution nahi|no solution/i.test(t)) signals.frustrated += 2;
  if (/haar gaya|give up|fed up|thak gaya|tired of|sick of|exhausted/i.test(t)) signals.frustrated += 3;
  if (/going nowhere|kahi nahi|stuck|atka hua|latka hua|pending from|since last/i.test(t)) signals.frustrated += 2;

  // Broken promises
  if (/you said|aapne kaha|bola tha|promised|promise kiya|assured|commitment/i.test(t)) signals.frustrated += 2;
  if (/24 hours|48 hours|2 days|3 days|one week|ek hafte|do din|teen din/i.test(t) && /nahi|not|still|abhi/i.test(t)) signals.frustrated += 2;

  // Money stuck / financial anxiety
  if (/paisa atka|money stuck|paisa nahi aaya|money not received|kab milega paisa|where is my money/i.test(t)) signals.frustrated += 3;
  if (/mera paisa|my money|hard earned|mehnat ka|savings|bachaya tha/i.test(t)) signals.frustrated += 2;
  if (/interest nahi|coupon nahi|payout nahi|maturity nahi|settlement nahi/i.test(t)) signals.frustrated += 2;

  // Long call = likely frustration (graduated)
  if (durationSec > 1200) signals.frustrated += 3; // 20+ min
  else if (durationSec > 900) signals.frustrated += 2; // 15+ min
  else if (durationSec > 600) signals.frustrated += 1; // 10+ min

  // ──────────────────────────────────────────────
  // ANGRY SIGNALS (expanded)
  // ──────────────────────────────────────────────

  // Direct anger / abuse
  if (/fraud|scam|dhoka|cheat|loot|bekar|worst|terrible|bakwaas|unacceptable/i.test(t)) signals.angry += 3;
  if (/pagal|mad|ridiculous|nonsense|useless|pathetic|hopeless|shameless|bewakoof/i.test(t)) signals.angry += 2;
  if (/gadha|idiot|stupid|fool|chutiya|bevkuf|nalayak|kamina/i.test(t)) signals.angry += 4;

  // Escalation demands
  if (/senior|manager se baat|complaint|grievance|higher authority|escalat/i.test(t)) signals.angry += 2;
  if (/ceo|md se baat|top management|head office|corporate office/i.test(t)) signals.angry += 3;

  // Legal / regulatory threats
  if (/rbi complaint|consumer forum|legal action|advocate|lawyer|court|police|cyber cell/i.test(t)) signals.angry += 4;
  if (/sebi|ombudsman|banking ombudsman|consumer court|district forum/i.test(t)) signals.angry += 3;
  if (/file case|case karunga|complaint karunga|report karunga|media|social media pe dalunga|twitter pe/i.test(t)) signals.angry += 3;

  // Trust breakdown
  if (/trust nahi|never again|kabhi nahi|worst experience|biggest mistake|regret|pachtawa/i.test(t)) signals.angry += 2;
  if (/uninstall|delete app|close account|sab nikalo|withdraw everything|all my money out/i.test(t)) signals.angry += 3;

  // ──────────────────────────────────────────────
  // SATISFIED SIGNALS (expanded)
  // ──────────────────────────────────────────────
  if (/thank you|thanks|dhanyawad|shukriya|helpful|great help|solved|fixed|done|sorted/i.test(t)) signals.satisfied += 2;
  if (/accha|very good|nice|perfect|ok.*done|understood|samajh gaya|clear ho gaya/i.test(t)) signals.satisfied += 1;
  if (/excellent service|bahut accha|amazing|wonderful|fantastic|great job|well done/i.test(t)) signals.satisfied += 3;
  if (/will recommend|refer karunga|friends ko bolunga|good experience|happy with/i.test(t)) signals.satisfied += 2;
  if (/resolved|issue solved|problem fixed|all good|sab theek|ho gaya|mil gaya/i.test(t)) signals.satisfied += 2;

  // ──────────────────────────────────────────────
  // CONFUSED SIGNALS (expanded)
  // ──────────────────────────────────────────────
  if (/samajh nahi|kaise kare|kya matlab|confused|unclear|pata nahi|nahi samjha|nahi pata/i.test(t)) signals.confused += 2;
  if (/how to|kaise|explain|batao|kya karna|what should I do/i.test(t)) signals.confused += 1;
  if (/kya hota hai|what is this|ye kya hai|i don't understand|nahi samajh|aap batao/i.test(t)) signals.confused += 2;
  if (/complicated|complex|mushkil|difficult to understand|jargon|technical/i.test(t)) signals.confused += 1;

  // ──────────────────────────────────────────────
  // TONALITY ANALYSIS (new!)
  // ──────────────────────────────────────────────
  const tonality = detectTonality(segments, customerText, agentText, durationSec);

  // Boost sentiment signals from tonality
  if (tonality.customerTone === "aggressive") signals.angry += 2;
  if (tonality.customerTone === "pleading") signals.frustrated += 2;
  if (tonality.customerTone === "sarcastic") signals.frustrated += 1;
  if (tonality.escalationPattern) signals.angry += 2;
  if (tonality.repeatedAsking >= 3) signals.frustrated += 2;

  // ──────────────────────────────────────────────
  // DETERMINE PRIMARY SENTIMENT (refined thresholds)
  // ──────────────────────────────────────────────
  const max = Math.max(signals.frustrated, signals.satisfied, signals.confused, signals.angry);
  let sentiment, sentimentScore;

  if (max === 0) {
    sentiment = "Neutral";
    sentimentScore = 50;
  } else if (signals.angry >= 3) {
    sentiment = "Angry";
    sentimentScore = Math.min(100, 65 + signals.angry * 4);
  } else if (signals.frustrated >= 2 && signals.frustrated >= signals.satisfied) {
    sentiment = "Frustrated";
    sentimentScore = Math.min(100, 55 + signals.frustrated * 4);
  } else if (signals.satisfied >= 2 && signals.satisfied > signals.frustrated) {
    sentiment = "Satisfied";
    sentimentScore = Math.min(100, 60 + signals.satisfied * 8);
  } else if (signals.confused >= 2) {
    sentiment = "Confused";
    sentimentScore = Math.min(100, 50 + signals.confused * 8);
  } else if (signals.frustrated >= 1 && signals.angry >= 1) {
    // Combined frustration+anger even if neither is high alone
    sentiment = "Frustrated";
    sentimentScore = Math.min(100, 55 + (signals.frustrated + signals.angry) * 4);
  } else {
    sentiment = "Neutral";
    sentimentScore = 50;
  }

  // ──────────────────────────────────────────────
  // WHAT'S HELPING / NOT HELPING
  // ──────────────────────────────────────────────
  const helpingAnalysis = analyzeWhatHelps(segments, customerText, agentText, sentiment, durationSec);

  return {
    sentiment,
    sentimentScore,
    signals,
    tonality,
    helpingAnalysis,
    sentimentColor: {
      Angry: "#EF4444",
      Frustrated: "#F59E0B",
      Confused: "#A78BFA",
      Neutral: "#94A3B8",
      Satisfied: "#22C55E",
    }[sentiment] || "#94A3B8",
  };
}

// ─── Tonality Detection (voice-from-text patterns) ───
function detectTonality(segments, customerText, agentText, durationSec) {
  const result = {
    customerTone: "neutral",     // aggressive, pleading, sarcastic, calm, neutral
    agentTone: "neutral",         // empathetic, defensive, robotic, helpful, neutral
    escalationPattern: false,     // did frustration escalate over time?
    repeatedAsking: 0,            // how many times customer asked same thing
    interruptionScore: 0,         // short speaker turns = interruptions
    silenceGaps: false,           // long gaps suggest hold / checking
    toneShift: null,              // did tone shift mid-call? e.g. "angry→calm" or "calm→angry"
  };

  // ── Customer Tone ──
  // Aggressive: direct confrontation, threats, insults
  if (/fraud|scam|dhoka|legal|court|rbi|complaint|police|cyber/i.test(customerText)) {
    result.customerTone = "aggressive";
  }
  // Pleading: desperation, helplessness, emotional appeal
  else if (/please.*help|meri.*madad|kya karu|haar gaya|thak gaya|mehnat ka paisa|bachon ka paisa|urgent.*need|bahut zaruri/i.test(customerText)) {
    result.customerTone = "pleading";
  }
  // Sarcastic: passive-aggressive, rhetorical questions
  else if (/oh.*great|wonderful|nice service|wah.*kya.*service|aise hi chalta hai|yahi hota hai|company hai ya|this is how you treat/i.test(customerText)) {
    result.customerTone = "sarcastic";
  }
  // Calm
  else if (/theek hai|ok|sure|haan|accha|right|understood|samajh gaya/i.test(customerText) && !/angry|frustrated/.test(customerText)) {
    result.customerTone = "calm";
  }

  // ── Agent Tone ──
  // Empathetic: acknowledging feelings, apologizing
  if (/i understand|main samajhta|sorry for|maafi|inconvenience|pareshani|i apologize|surely.*help|definitely.*resolve/i.test(agentText)) {
    result.agentTone = "empathetic";
  }
  // Defensive: blaming customer, deflecting
  else if (/your fault|aapki galti|you should have|aapko karna chahiye tha|not our|humara nahi|system.*hai/i.test(agentText)) {
    result.agentTone = "defensive";
  }
  // Robotic: scripted responses, no personalization
  else if (/please hold|ek minute|let me check|system mein|backend team|ticket raise|48.*hours|24.*hours/i.test(agentText) &&
           !/understand|sorry|help|samajhta/i.test(agentText)) {
    result.agentTone = "robotic";
  }
  // Helpful: proactive, offering solutions
  else if (/let me.*help|main kar deta|abhi.*resolve|immediately|priority|jaldi|right away|i will make sure/i.test(agentText)) {
    result.agentTone = "helpful";
  }

  // ── Escalation Pattern ──
  // Check if customer starts calm but becomes angry
  const customerSegs = segments.filter(s => s.speaker === 1);
  if (customerSegs.length >= 4) {
    const firstHalf = customerSegs.slice(0, Math.floor(customerSegs.length / 2)).map(s => s.text.toLowerCase()).join(" ");
    const secondHalf = customerSegs.slice(Math.floor(customerSegs.length / 2)).map(s => s.text.toLowerCase()).join(" ");

    const earlyAnger = (firstHalf.match(/angry|fraud|scam|complaint|worst|unacceptable|useless|terrible|pagal|bekar/gi) || []).length;
    const lateAnger = (secondHalf.match(/angry|fraud|scam|complaint|worst|unacceptable|useless|terrible|pagal|bekar/gi) || []).length;

    if (lateAnger > earlyAnger + 1) {
      result.escalationPattern = true;
      result.toneShift = "calm→angry";
    } else if (earlyAnger > lateAnger + 1) {
      result.toneShift = "angry→calm";
    }
  }

  // ── Repeated Asking ──
  // Detect customer asking the same question multiple times
  const customerQuestions = customerSegs
    .map(s => s.text)
    .filter(t => /\?|kab|when|where|kaha|status|update|hua kya/i.test(t));
  result.repeatedAsking = Math.max(0, customerQuestions.length - 2); // normal to ask 1-2 questions

  // ── Interruption Score ──
  // Many short turns = heated back-and-forth
  const shortTurns = segments.filter(s => s.text.length < 30).length;
  result.interruptionScore = Math.min(10, Math.round(shortTurns / Math.max(1, segments.length) * 10));

  return result;
}

// ─── What's Helping / What's Not Helping ───
function analyzeWhatHelps(segments, customerText, agentText, sentiment, durationSec) {
  const helping = [];
  const notHelping = [];

  // ── WHAT'S HELPING ──

  // Agent empathy
  if (/i understand|main samajhta|sorry for|maafi|i apologize|pareshani ke liye/i.test(agentText)) {
    helping.push({ factor: "Agent showed empathy", detail: "Agent acknowledged customer's frustration or inconvenience" });
  }

  // Immediate resolution
  if (/abhi kar deta|right now|immediately|abhi.*resolve|let me fix|main abhi/i.test(agentText)) {
    helping.push({ factor: "Immediate action", detail: "Agent took action on the call itself instead of deferring" });
  }

  // Clear explanation
  if (/let me explain|samjhata|actually what happened|issue ye hai|reason is|wajah ye hai/i.test(agentText)) {
    helping.push({ factor: "Clear explanation", detail: "Agent explained the root cause to the customer" });
  }

  // Proactive follow-up commitment
  if (/i will call you|main call karunga|update dunga|follow up|email bhejta|personally ensure/i.test(agentText)) {
    helping.push({ factor: "Proactive follow-up", detail: "Agent committed to personal follow-up instead of generic ticket" });
  }

  // Ownership language
  if (/i will take care|main dekh lunga|my responsibility|meri zimmedari|i personally|main khud/i.test(agentText)) {
    helping.push({ factor: "Agent took ownership", detail: "Agent took personal ownership of the issue" });
  }

  // Customer confirmed satisfaction
  if (/thank|theek hai.*samajh|ok.*fine|accha.*theek|got it.*thanks|ok understood|clear|samajh gaya/i.test(customerText)) {
    helping.push({ factor: "Customer acknowledged resolution", detail: "Customer expressed understanding or thanks" });
  }

  // ── WHAT'S NOT HELPING ──

  // Scripted responses / no personalization
  if (/please hold|ticket raise|48 hours|24 hours|backend team|let me check/i.test(agentText) &&
      !/sorry|understand|samajhta|immediately/i.test(agentText)) {
    notHelping.push({ factor: "Scripted/robotic responses", detail: "Agent using standard scripts without addressing the specific concern" });
  }

  // Passing the buck
  if (/backend team|technical team|different department|unka kaam|humara nahi|not my|i can only|sirf ticket/i.test(agentText)) {
    notHelping.push({ factor: "Passing responsibility", detail: "Agent deflecting to other teams without owning the resolution" });
  }

  // Repetitive hold
  const holdCount = (agentText.match(/hold|wait|ek minute|ruko|checking/gi) || []).length;
  if (holdCount >= 3) {
    notHelping.push({ factor: "Excessive holds", detail: `Agent put customer on hold ${holdCount} times — suggests lack of info access` });
  }

  // No acknowledgment of repeat calling
  if (/pehle bhi|already called|phir se|baar baar|3 times|4 times/i.test(customerText) &&
      !/sorry|apologize|maafi|i see.*previous|understand.*frustration/i.test(agentText)) {
    notHelping.push({ factor: "Ignored repeat calling history", detail: "Customer mentioned calling before but agent didn't acknowledge or escalate" });
  }

  // Generic timeline without specifics
  if (/24.*hours|48.*hours|2-3.*days|1-2.*days|shortly|jaldi|soon/i.test(agentText) &&
      !/specific|exact|by.*tomorrow|by.*evening|kal.*tak|aaj.*shaam/i.test(agentText)) {
    notHelping.push({ factor: "Vague timeline", detail: "Agent gave generic timeline (24-48 hrs) instead of specific commitment" });
  }

  // Customer had to explain issue multiple times
  const customerExplanations = segments.filter(s => s.speaker === 1 && s.text.length > 60).length;
  if (customerExplanations >= 4 && durationSec > 600) {
    notHelping.push({ factor: "Customer re-explained multiple times", detail: "Customer had to explain their issue repeatedly — agent didn't grasp it quickly" });
  }

  // No solution offered
  if (sentiment === "Angry" || sentiment === "Frustrated") {
    if (!/resolve|fix|solution|refund|credit|process|done|kar diya|ho jayega/i.test(agentText)) {
      notHelping.push({ factor: "No concrete solution", detail: "Agent didn't offer any concrete resolution or timeline" });
    }
  }

  // Abrupt ending without resolution
  if (durationSec < 120 && segments.length > 4) {
    if (!/bye|thank|theek|ok/i.test(segments.slice(-2).map(s=>s.text).join(" "))) {
      notHelping.push({ factor: "Abrupt call end", detail: "Call ended quickly without proper closure — possible disconnect" });
    }
  }

  return {
    helping,
    notHelping,
    helpScore: helping.length,
    hurtScore: notHelping.length,
    netScore: helping.length - notHelping.length,
  };
}

// ─── Agent Disposition ───
function detectAgentActions(transcript) {
  const t = (transcript || "").toLowerCase();
  const actions = [];

  if (/email.*share|email.*send|mail.*kar|email.*bhej|email pe|mail pe bhej|email karenge|email kar denge|mail send/i.test(t))
    actions.push({ action: "Will Send Email", reason: "Agent committed to sending information via email", icon: "📧" });
  if (/rm.*connect|rm se baat|rm.*call|relationship manager|rm team|rm ko bol|rm assign/i.test(t))
    actions.push({ action: "Transferring to RM", reason: "Agent escalated to Relationship Manager team", icon: "👤" });
  if (/app.*check|app mein|app pe|app se|playstore|app download|app open kar/i.test(t))
    actions.push({ action: "Asked to Check App", reason: "Agent directed customer to use mobile app", icon: "📱" });
  if (/callback|call back|call kar.*denge|wapas call|phir se call/i.test(t))
    actions.push({ action: "Promised Callback", reason: "Agent promised to call back with resolution", icon: "📞" });
  if (/escalat|senior|team.*check|backend.*team|technical team|raise.*ticket|ticket number/i.test(t))
    actions.push({ action: "Raised Ticket / Escalated", reason: "Agent escalated to internal team with ticket", icon: "🎫" });
  if (/done|ho gaya|resolve|fixed|complete|activated|processed|successful/i.test(t))
    actions.push({ action: "Resolved on Call", reason: "Issue was resolved during the call itself", icon: "✅" });
  if (/hold|wait|check kar|ek minute|ruko|let me check|system check/i.test(t))
    actions.push({ action: "Put on Hold to Check", reason: "Agent put customer on hold to investigate", icon: "⏸️" });

  if (actions.length === 0) actions.push({ action: "Information Provided", reason: "Agent provided general information or guidance", icon: "ℹ️" });
  return actions;
}

// ─── Pain Signals ───
function detectPainSignals(transcript, durationSec) {
  const t = (transcript || "").toLowerCase();
  const pains = [];

  if (/pehle bhi call|already called|phir se|baar baar|again calling|dobara|kitni baar/i.test(t))
    pains.push({ signal: "Repeat Caller", severity: "high", reason: "Customer mentions calling multiple times for same issue" });
  if (durationSec > 600)
    pains.push({ signal: "Long Call (10+ min)", severity: "medium", reason: `Call lasted ${Math.round(durationSec/60)} minutes — complex or unresolved` });
  if (/escalat|senior|manager|complaint|grievance|not happy|disappointed/i.test(t))
    pains.push({ signal: "Wants Escalation", severity: "high", reason: "Customer demanding to speak with senior or filing complaint" });
  if (/fraud|scam|cheat|dhoka|loot|trust nahi/i.test(t))
    pains.push({ signal: "Trust Issue", severity: "critical", reason: "Customer expressing loss of trust or fraud allegation" });
  if (/competitor|groww|zerodha|coin|other app|dusre platform|angel|upstox/i.test(t))
    pains.push({ signal: "Competitor Mention", severity: "medium", reason: "Customer comparing with competitor" });
  if (/cancel|close account|delete|uninstall|remove/i.test(t))
    pains.push({ signal: "Churn Risk", severity: "high", reason: "Customer considering leaving the platform" });

  return pains;
}

// ─── User Story Detection ───
function isCompellingStory(call) {
  const t = (call.transcript || "").toLowerCase();
  let score = 0;

  // Long, emotional calls are better stories
  if (call.duration > 300) score += 1;
  if (call.duration > 600) score += 2;
  if (call.duration > 900) score += 2;

  // Sentiment-based
  if (call.sentimentAnalysis?.sentiment === "Angry") score += 3;
  if (call.sentimentAnalysis?.sentiment === "Frustrated") score += 2;
  if (call.sentimentAnalysis?.sentimentScore > 70) score += 1;

  // Pain signals
  if (call.painSignals?.length >= 2) score += 2;
  if (call.painSignals?.length >= 3) score += 1;

  // Specific compelling patterns — crisis stories
  if (/fraud|scam|dhoka|fake|cheat/i.test(t)) score += 3;
  if (/pehle bhi call|baar baar|bar bar|kitni baar|daily call/i.test(t)) score += 2;
  if (/groww|zerodha|competitor|dusre|other app|angel|upstox/i.test(t)) score += 2;
  if (/senior|manager se baat|escalat|complaint|grievance/i.test(t)) score += 1;
  if (/mera paisa|money stuck|paisa kahan|where is my money|kahan gaya|deducted/i.test(t)) score += 2;
  if (/lakh|crore|\d{5,}/i.test(t)) score += 1; // Large amounts
  if (/rbi|legal|court|consumer forum|advocate/i.test(t)) score += 2;

  // Specific product-related drama
  if (/fd.*maturity.*nahi|fd.*money.*nahi|bond.*interest.*nahi/i.test(t)) score += 2;
  if (/card.*nahi.*aaya|card.*deliver/i.test(t) && call.duration > 300) score += 1;
  if (/refund.*pending|refund.*nahi/i.test(t) && call.duration > 300) score += 1;

  // Good resolution is also a story (positive stories)
  if (call.sentimentAnalysis?.sentiment === "Satisfied" && call.duration > 300) score += 2;
  if (/thank you so much|very helpful|great service|excellent|you are amazing/i.test(t)) score += 2;
  if (/problem solved|resolved|sorted|fixed/i.test(t) && call.duration > 180) score += 1;

  // Customer quote quality bonus
  if (call.customerQuote && call.customerQuote.length > 40) score += 1;
  if (call.customerQuote && call.customerQuote.length > 80) score += 1;

  return score;
}

function extractQuote(transcript) {
  // Find the most emotional CUSTOMER line (Speaker 1 only)
  const segments = [];
  const regex = /\[Speaker (\d+)\]\s*([\s\S]*?)(?=\[Speaker \d+\]|$)/g;
  let match;
  while ((match = regex.exec(transcript)) !== null) {
    segments.push({ speaker: parseInt(match[1]), text: match[2].trim() });
  }

  let bestQuote = "";
  let bestScore = 0;

  for (const seg of segments) {
    if (seg.speaker !== 1) continue; // customer only
    const text = seg.text;
    if (text.length < 25 || text.length > 300) continue;

    // Skip agent greetings that leak into Speaker 1 due to diarization errors
    if (/^(hello|welcome|good morning|good evening|good afternoon|how can|may i|this is|my name is|thank you for calling)/i.test(text)) continue;
    if (/^(hi sir|hi ma|namaste|namaskar|haan ji|ok sir|ok madam|ji sir|accha)$/i.test(text.trim())) continue;
    if (/welcome to|how can I help|my name is|afternoon sir|stable money/i.test(text) && text.length < 120) continue;
    // Skip very short filler responses
    if (text.length < 30 && /^(ok|yes|no|haan|nahi|accha|theek hai|right|sure)/i.test(text)) continue;

    let score = 0;
    // Strong emotional content — fraud / money problems
    if (/fraud|scam|dhoka|cheat|loot|fake/i.test(text)) score += 5;
    if (/paisa.*nahi|money.*not|kahan gaya|where is my|stuck|deduct|deducted|kataa/i.test(text)) score += 4;
    if (/kab hoga|kitni baar|phir se|pehle bhi|already|again|baar baar|bar bar/i.test(text)) score += 4;

    // Frustration / helplessness
    if (/nahi ho raha|nahi mil raha|koi jawab nahi|no response|no one|kuch nahi/i.test(text)) score += 3;
    if (/senior|manager|complaint|escalat|grievance/i.test(text)) score += 3;
    if (/wait.*long|bahut time|so many days|itne din|pending/i.test(text)) score += 3;

    // Competitor / churn
    if (/groww|zerodha|other app|dusre platform|angel|upstox|coin|better/i.test(text)) score += 3;
    if (/cancel|close|uninstall|leave|chhodna|hatana|delete/i.test(text)) score += 2;

    // Confusion / seeking help
    if (/samajh nahi|kaise kare|kya karna|confused|understand|explain/i.test(text)) score += 2;
    if (/help me|please help|koi toh|mujhe batao/i.test(text)) score += 2;

    // Positive resolution stories are also good
    if (/thank you so much|bahut accha|very helpful|great service|problem solved|resolved|happy/i.test(text)) score += 3;
    if (/recommend|will tell|refer|suggest/i.test(text)) score += 2;

    // Specific pain with detail
    if (/lakh|crore|thousand|hazar|\d{4,}|\₹|rupee|rs\s?\d/i.test(text)) score += 2;
    if (/\d+\s*(din|days?|month|week|mahine)/i.test(text)) score += 1;

    // Questions are interesting
    if (/\?/.test(text)) score += 1;
    // Prefer medium length — not too short, not too long
    if (text.length > 60 && text.length < 200) score += 1;
    // Longer substantive content
    if (text.length > 100 && text.length < 250) score += 1;

    if (score > bestScore) {
      bestScore = score;
      bestQuote = text;
    }
  }

  return bestQuote || "";
}

// ─── English paraphrase for Hindi/Hinglish quotes ───
function paraphraseQuote(quote) {
  if (!quote) return "";
  // If mostly English already, return as-is
  const hindiChars = (quote.match(/[\u0900-\u097F]/g) || []).length;
  const totalChars = quote.replace(/\s/g, "").length;
  if (hindiChars / totalChars < 0.1) return ""; // mostly English, no need

  // Pattern-based English context for common Hindi/Hinglish phrases
  const patterns = [
    [/fraud|dhoka|cheat/i, "Customer alleges fraud"],
    [/paisa.*nahi.*aay|money.*not.*received|nahi mila/i, "Money not received"],
    [/kab.*milega|kab.*aayega|when.*will/i, "Asking when they will receive it"],
    [/kitni baar|baar baar|pehle bhi/i, "Has called multiple times about this"],
    [/samajh.*nahi|confused|kaise/i, "Customer is confused and needs help"],
    [/groww|zerodha|competitor|dusre/i, "Comparing with competitor platforms"],
    [/cancel|close|band|hatana/i, "Wants to cancel or close"],
    [/mera paisa|paisa kahan|money stuck/i, "Asking where their money is"],
    [/thank|accha|helpful|great/i, "Customer expressing satisfaction"],
    [/interest|coupon|payout/i, "About interest/coupon payments"],
    [/tax|tds|15g/i, "About tax documents"],
    [/card.*nahi|card.*delivery|activate/i, "About card delivery or activation"],
    [/limit.*increase|limit.*badha/i, "Requesting credit limit increase"],
    [/senior|manager|escalat/i, "Demanding escalation to senior staff"],
  ];

  let context = [];
  for (const [pat, desc] of patterns) {
    if (pat.test(quote)) context.push(desc);
  }
  return context.length ? context.slice(0, 2).join(". ") + "." : "Customer explaining their issue.";
}

// ─── Transcript Detail Extractor ───
function extractTranscriptDetails(transcript) {
  const t = (transcript || "").toLowerCase();
  const details = {};

  // Extract amounts mentioned
  const amtMatch = t.match(/(?:rs\.?\s?|₹|rupee\s?)(\d[\d,]*(?:\.\d+)?)/i) || t.match(/(\d[\d,]+)\s*(?:lakh|crore|thousand|hazar)/i);
  if (amtMatch) details.amount = amtMatch[0].trim();

  // Extract time references
  const timeMatch = t.match(/(\d+)\s*(din|days?|months?|weeks?|mahine|hafta)/i);
  if (timeMatch) details.timePeriod = `${timeMatch[1]} ${timeMatch[2]}`;

  // Extract specific bank/partner names
  const bankMatch = t.match(/(rbl|unity|suryoday|shivalik|bajaj|hdfc|icici|sbi|axis|kotak|idfc|indusind|yes bank)/i);
  if (bankMatch) details.bank = bankMatch[1];

  // Extract specific competitor
  const compMatch = t.match(/(groww|zerodha|upstox|angel one|coin|kuvera|paytm money|et money)/i);
  if (compMatch) details.competitor = compMatch[1];

  // Extract card type / bond name references
  const cardMatch = t.match(/(platinum|gold|signature|select|premium|classic)/i);
  if (cardMatch) details.cardType = cardMatch[1];

  // Extract app-related specifics
  const appMatch = t.match(/(otp not|otp nahi|app crash|white screen|login fail|password reset|server error|page not load)/i);
  if (appMatch) details.techIssue = appMatch[0];

  // Was the call disconnected abruptly?
  if (t.length > 200 && !/thank|bye|okay|accha|noted/i.test(t.slice(-100))) details.abruptEnd = true;

  // Did customer mention a specific date?
  const dateMatch = t.match(/(\d{1,2})\s*(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)/i);
  if (dateMatch) details.dateRef = dateMatch[0];

  return details;
}

// ─── Summary Generator (human-readable, detail-rich) ───
function generateSummary(product, subcategory, agentActions, pains, sentiment, durationSec, transcript) {
  const durMin = Math.round(durationSec / 60 * 10) / 10;
  const t = (transcript || "").toLowerCase();
  const details = extractTranscriptDetails(transcript);

  // Build a natural language summary
  let opener = "";

  // Product-specific openers — richer context
  if (product === "Credit Card") {
    if (subcategory.includes("Activation")) {
      opener = details.cardType
        ? `Customer called about their ${details.cardType} credit card — checking activation or delivery status.`
        : "Customer called to check credit card delivery/activation status.";
    }
    else if (subcategory.includes("Limit")) {
      opener = details.amount
        ? `Customer has a credit limit concern (mentioned amount: ${details.amount}).`
        : "Customer called about a credit limit issue — requesting increase or checking available limit.";
    }
    else if (subcategory.includes("Payment Failed")) {
      opener = details.amount
        ? `Customer's payment of ${details.amount} failed or money was deducted but not credited.`
        : "Customer's payment failed or money was deducted but not credited — transaction stuck.";
    }
    else if (subcategory.includes("Bill")) opener = "Customer called with questions about their credit card billing — confused about statement amount, due date, or outstanding balance.";
    else if (subcategory.includes("Reward")) opener = "Customer called about missing rewards or cashback not reflecting in their account.";
    else if (subcategory.includes("Late Fee")) {
      opener = details.amount
        ? `Customer disputing charges of ${details.amount} — late fees or unexpected charges on their card.`
        : "Customer disputing late fees or unexpected charges on their credit card.";
    }
    else if (subcategory.includes("Refund")) {
      opener = details.timePeriod
        ? `Customer waiting for a refund — says it's been ${details.timePeriod} since the original transaction.`
        : "Customer following up on a pending refund that hasn't been processed yet.";
    }
    else if (subcategory.includes("Fraud")) opener = "Customer reported suspicious or unauthorized transactions on their credit card — possible fraud case.";
    else if (subcategory.includes("EMI")) opener = "Customer wants to convert a recent transaction to EMI or asking about available EMI options.";
    else if (subcategory.includes("KYC")) opener = "Customer's KYC verification is stuck, blocking card features or approval.";
    else if (subcategory.includes("App")) {
      opener = details.techIssue
        ? `Customer facing technical issue: ${details.techIssue}. Unable to use the app properly.`
        : "Customer facing app login or technical issues — unable to access account.";
    }
    else opener = `Customer called about ${subcategory}.`;
  } else if (product === "Bonds") {
    if (subcategory.includes("Buy")) {
      opener = details.amount
        ? `Customer wants to invest ${details.amount} in bonds — seeking guidance on available options.`
        : "Customer wants help buying bonds — asking about available options, minimum investment, or how to purchase.";
    }
    else if (subcategory.includes("Tax")) {
      opener = details.dateRef
        ? `Customer needs tax documents (TDS certificate, Form 15G/H, or capital gains statement) — referenced date ${details.dateRef} for ITR filing.`
        : "Customer needs tax documents — TDS certificate, Form 15G/H, or capital gains statement for ITR filing.";
    }
    else if (subcategory.includes("Statement")) opener = "Customer requesting their bond holding statement, CAS report, or demat account statement.";
    else if (subcategory.includes("Interest")) {
      opener = details.timePeriod
        ? `Customer's interest/coupon payment has been delayed for ${details.timePeriod} — payment not received.`
        : "Customer's bond interest or coupon payment is overdue — asking when it will be credited.";
    }
    else if (subcategory.includes("Maturity")) {
      opener = details.amount
        ? `Customer's bond matured (${details.amount} expected) but money hasn't been credited yet.`
        : "Customer's bond has matured but the principal amount hasn't been credited to their account.";
    }
    else if (subcategory.includes("Exit")) opener = "Customer wants to exit or sell their bonds before maturity — asking about early exit options and penalties.";
    else if (subcategory.includes("Return")) opener = "Customer confused about bond returns — asking about yield calculation, interest rate, or expected earnings.";
    else if (subcategory.includes("RM")) opener = "Customer specifically requesting to speak with their assigned Relationship Manager for bond portfolio discussion.";
    else opener = `Customer called about ${subcategory}.`;
  } else if (product === "FD") {
    if (subcategory.includes("Booking")) {
      opener = details.bank
        ? `Customer needs help booking a Fixed Deposit with ${details.bank}.`
        : details.amount
        ? `Customer wants to book an FD of ${details.amount} — needs booking assistance.`
        : "Customer needs help booking a Fixed Deposit — facing issues or needs guidance.";
    }
    else if (subcategory.includes("Bank")) {
      opener = details.bank
        ? `Customer comparing FD rates — specifically asking about ${details.bank}.`
        : "Customer comparing FD rates across partner banks — wants the best rate and safe bank.";
    }
    else if (subcategory.includes("Matured")) {
      opener = details.amount
        ? `Customer's FD matured (${details.amount} expected) but money not yet received.`
        : "Customer's FD has matured but payout hasn't been credited — following up on money.";
    }
    else if (subcategory.includes("Interest Rate")) opener = "Customer checking current FD interest rates before investing.";
    else if (subcategory.includes("Break")) opener = "Customer wants to break their FD early — asking about premature withdrawal penalty.";
    else opener = `Customer called about ${subcategory}.`;
  } else if (product === "Mutual Fund") {
    if (subcategory.includes("Redemption")) {
      opener = details.amount
        ? `Customer wants to redeem ${details.amount} from their mutual fund — asking about withdrawal process.`
        : "Customer wants to redeem/withdraw their mutual fund investment — needs process guidance.";
    }
    else if (subcategory.includes("SIP")) opener = "Customer called about their SIP — asking about starting, stopping, modifying amount, or mandate setup.";
    else if (subcategory.includes("Fund")) opener = "Customer seeking mutual fund recommendations — wants investment advice on which funds to pick.";
    else if (subcategory.includes("Portfolio")) opener = "Customer worried about portfolio performance — checking returns and considering changes.";
    else opener = `Customer called about ${subcategory}.`;
  } else if (product === "KYC/Onboarding") {
    if (subcategory.includes("Video")) opener = "Customer's Video KYC keeps failing or disconnecting — unable to complete verification.";
    else if (subcategory.includes("Document")) opener = "Customer having trouble uploading KYC documents — upload failing or document rejected.";
    else opener = `Customer called about ${subcategory}.`;
  } else {
    if (subcategory.includes("App")) {
      opener = details.techIssue
        ? `Customer facing technical issue with the app: ${details.techIssue}.`
        : "Customer unable to use the Stable Money app — login, crash, or technical error.";
    }
    else if (subcategory.includes("Complaint")) opener = "Customer called with a complaint — wants the issue escalated or resolved immediately.";
    else if (subcategory.includes("Short")) opener = "Very short call — likely dropped, auto-disconnected, or customer hung up quickly.";
    else opener = `Customer called about ${subcategory}.`;
  }

  // Agent response — more natural phrasing
  const actions = agentActions || [];
  const primary = actions[0]?.action || "Information Provided";
  const secondary = actions[1]?.action || "";
  let agentPart = "";

  if (primary === "Resolved on Call") {
    agentPart = secondary
      ? ` Agent resolved it on the call and also ${secondary.toLowerCase()}.`
      : " Agent successfully resolved the issue during the call.";
  }
  else if (primary === "Will Send Email") agentPart = " Agent confirmed they will send the required details via email.";
  else if (primary === "Transferring to RM") agentPart = " Agent transferred the call to the customer's Relationship Manager.";
  else if (primary === "Asked to Check App") agentPart = " Agent walked the customer through the app and asked them to check there.";
  else if (primary === "Promised Callback") agentPart = " Agent assured the customer they will receive a callback.";
  else if (primary === "Raised Ticket / Escalated") agentPart = " Agent escalated the issue and raised a support ticket.";
  else if (primary === "Put on Hold to Check") agentPart = " Agent put the customer on hold to investigate the issue.";
  else agentPart = ` Agent: ${primary.toLowerCase()}.`;

  // Specific context clues from transcript
  let contextPart = "";
  if (details.competitor) contextPart += ` Customer mentioned ${details.competitor} as comparison.`;
  if (details.bank && !opener.includes(details.bank)) contextPart += ` Related to ${details.bank}.`;
  if (details.abruptEnd && durMin < 2) contextPart += " Call ended abruptly.";

  // Pain context — more natural
  let painPart = "";
  if (pains.some(p => p.signal === "Repeat Caller")) painPart += " This is a repeat caller who has contacted support before for the same issue.";
  if (pains.some(p => p.signal === "Trust Issue")) painPart += " Customer expressed distrust or frustration with the platform.";
  if (pains.some(p => p.signal === "Churn Risk")) painPart += " Churn risk — customer considering leaving or cancelling.";
  if (pains.some(p => p.signal === "Competitor Mention") && !contextPart.includes("mentioned")) painPart += " Customer comparing with a competitor.";
  if (durMin > 15) painPart += ` Extended call lasting ${durMin} minutes — complex or difficult resolution.`;
  else if (durMin > 10) painPart += ` Long call (${durMin} min).`;

  // Outcome indicator
  let outcomePart = "";
  if (sentiment.sentiment === "Satisfied" && primary === "Resolved on Call") outcomePart = " Outcome: customer appeared satisfied.";
  else if (sentiment.sentiment === "Angry" && primary === "Raised Ticket / Escalated") outcomePart = " Outcome: customer was angry, issue escalated.";
  else if (sentiment.sentiment === "Angry") outcomePart = " Outcome: customer left the call angry — needs follow-up.";
  else if (sentiment.sentiment === "Frustrated") outcomePart = " Outcome: customer was frustrated — follow-up recommended.";

  return (opener + agentPart + contextPart + painPart + outcomePart).trim();
}

// ─── Main Classifier ───
function classifyCall(call) {
  const transcript = call.transcript_data?.transcript || "";
  const product = detectProduct(call.group_name, transcript);
  const rules = ISSUE_RULES[product] || ISSUE_RULES["General"];
  const durationSec = call.duration_seconds || 0;

  // Find ALL matching subcategories, pick best
  let bestMatch = null;
  let allMatches = [];

  for (const rule of rules) {
    for (const pattern of rule.patterns) {
      if (pattern.test(transcript)) {
        allMatches.push({ subcategory: rule.sub, reason: rule.reason });
        if (!bestMatch) bestMatch = { subcategory: rule.sub, reason: rule.reason };
        break;
      }
    }
  }

  // If no match, try cross-product rules
  if (!bestMatch) {
    for (const [prod, prodRules] of Object.entries(ISSUE_RULES)) {
      if (prod === product) continue;
      for (const rule of prodRules) {
        for (const pattern of rule.patterns) {
          if (pattern.test(transcript)) {
            bestMatch = { subcategory: rule.sub, reason: rule.reason + " (detected from transcript)" };
            allMatches.push(bestMatch);
            break;
          }
        }
        if (bestMatch) break;
      }
      if (bestMatch) break;
    }
  }

  if (!bestMatch) {
    // Analyze transcript length to determine if it's actually useful
    if (transcript.length < 100) {
      bestMatch = { subcategory: "Very Short / Dropped Call", reason: "Call transcript too short — likely dropped or auto-disconnected" };
    } else {
      bestMatch = { subcategory: "General Inquiry", reason: "Call about general platform usage — no specific issue pattern matched" };
    }
  }

  const agentActions = detectAgentActions(transcript);
  const painSignals = detectPainSignals(transcript, durationSec);
  const sentimentAnalysis = analyzeSentiment(transcript, durationSec);
  const summary = generateSummary(product, bestMatch.subcategory, agentActions, painSignals, sentimentAnalysis, durationSec, transcript);

  // ─── Customer language detection (Speaker 1 = customer) ───
  // Extract only customer segments from transcript
  const customerSegments = [];
  const speakerBlocks = transcript.split(/\[Speaker\s+(\d)\]/);
  // speakerBlocks: ['pre', '0', 'agent text', '1', 'customer text', '0', ...]
  for (let i = 1; i < speakerBlocks.length; i += 2) {
    if (speakerBlocks[i] === '1' && speakerBlocks[i + 1]) {
      customerSegments.push(speakerBlocks[i + 1].trim());
    }
  }
  // If no Speaker 1 segments found (single-channel audio), use full transcript as fallback
  const customerText = customerSegments.length > 0 && customerSegments.join(' ').trim().length > 20
    ? customerSegments.join(' ')
    : transcript;

  const custHindiChars = (customerText.match(/[\u0900-\u097F]/g) || []).length;
  const custEnglishWords = (customerText.match(/\b[a-zA-Z]{3,}\b/g) || []).length;
  const custTotalChars = customerText.length;
  const custHindiPct = custTotalChars > 0 ? custHindiChars / custTotalChars * 100 : 0;
  // Common Hindi words in Roman script (Hinglish indicator)
  const romanHindiWords = (customerText.match(/\b(kya|hai|nahi|kaise|karna|hoga|kab|abhi|paisa|mera|mere|aap|yeh|woh|mujhe|kar|raha|hain|tha|bhi|toh|aur|se|ko|ki|ka|ho|le|de|ek|ya|par|ke|na|pe|jo|jab|tab|ye|wo|mat|bas|sab|kuch|bahut|accha|theek|sahi|galat|bol|bolo|baat|pehle|baad|samajh|pata|dekh|chala|gaya|aaya|laga|hua|hota|karo|kiya|diya|liya|milta|milega|ayega|jayega|hoga|chahiye|zaroor|bilkul)\b/gi) || []).length;

  let customerLanguage;
  if (custTotalChars < 30) {
    customerLanguage = 'Unknown';
  } else if (custHindiPct > 30) {
    // Heavy Devanagari + some English = Hinglish, pure Devanagari = Hindi
    customerLanguage = custEnglishWords > 5 ? 'Hinglish' : 'Hindi';
  } else if (custHindiPct > 5 && romanHindiWords > 3) {
    customerLanguage = 'Hinglish';
  } else if (romanHindiWords > 8 && custEnglishWords > 3) {
    customerLanguage = 'Hinglish';
  } else if (romanHindiWords > 5) {
    customerLanguage = 'Hinglish';
  } else if (custHindiPct > 15) {
    customerLanguage = 'Hindi';
  } else {
    customerLanguage = 'English';
  }

  const result = {
    call_sid: call.call_sid,
    product,
    category: bestMatch.subcategory,
    categoryReason: bestMatch.reason,
    allCategories: allMatches,
    agentActions,
    painSignals,
    sentimentAnalysis,
    summary,
    duration: durationSec,
    durationMin: Math.round(durationSec / 60 * 10) / 10,
    phone: call.from_number,
    circle: call.from_circle,
    department: call.group_name,
    recordingUrl: call.recording_url,
    hasRecording: call.has_recording,
    transcript: transcript,
    customerLanguage: customerLanguage,
    startTime: call.start_time,
    direction: call.direction,
    disconnectedBy: call.disconnected_by,
  };

  // Story scoring
  result.storyScore = isCompellingStory(result);
  result.customerQuote = extractQuote(transcript);

  return result;
}

module.exports = { classifyCall, detectProduct, ISSUE_RULES, analyzeSentiment, paraphraseQuote };

