/**
 * Browser-Side Privacy-First Email Ingestion Client (`GET /api/inbox/client.js`).
 *
 * Served dynamically by `scrape_server` with `filter_config.json` and public OAuth
 * Client IDs baked in before serving (`EMBEDDED_CONFIG`). Executes entirely inside
 * the student's browser tab so raw private emails never leave their device.
 */

export const EMBEDDED_CONFIG = /*__EMBEDDED_CONFIG_START__*/ {
  version: 0,
  updated_at: null,
  filter_config: {},
  oauth: {
    gmail_client_id: "",
    outlook_client_id: "",
  },
} /*__EMBEDDED_CONFIG_END__*/;

export const BASELINE_EXCLUDED_KEYWORDS = [
  "mật khẩu",
  "password",
  "otp",
  "mã xác nhận",
  "đóng học phí",
  "nợ học phí",
  "thanh toán học phí",
  "tiền học phí",
  "thông báo học phí",
  "học phí học kỳ",
  "miễn giảm học phí",
  "biên lai",
  "bảng điểm cá nhân",
  "kết quả học tập",
  "cảnh báo học vụ",
  "kỷ luật",
];

export const BASELINE_NON_EVENT_KEYWORDS = [
  "xin chúc mừng",
  "chúc mừng",
  "recap",
  "tổng kết",
  "vinh danh",
  "tri ân ban chấp hành",
  "đã chính thức khép lại",
  "hành trình khép lại",
  "gửi lời cảm ơn",
  "hội nghị mở rộng",
  "đăng ký ở ký túc xá",
  "lễ trao bằng tốt nghiệp",
  "kết quả tuyển dụng",
  "diễn ra thành công",
  "các diễn giả đã mang đến",
];

export const BASELINE_ACTIVE_REGISTRATION_CUES = [
  "mở đơn",
  "link đăng ký",
  "form đăng ký",
  "đăng ký tham gia",
  "đăng ký tham dự",
  "mở cổng đăng ký",
  "forms.gle",
  "chính thức mở",
];

const UNSAFE_SENDER_DOMAINS = new Set([
  "*",
  ".",
  "gmail.com",
  "yahoo.com",
  "outlook.com",
  "hotmail.com",
]);

const STATIC_ASSET_EXT_RE = /\.(png|jpe?g|gif|svg|ico|webp|css|js)$/i;
const TRACKING_NOISE = ["pixel", "beacon", "spacer", "1x1", "emoji", "rsrc.php", ".gif", ".svg", ".ico"];

function normalizeNfc(text) {
  return String(text || "").normalize("NFC");
}

function escapeRegExp(str) {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function unescapeHtmlEntities(str) {
  const named = {
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&#39;": "'",
    "&apos;": "'",
    "&nbsp;": " ",
  };
  return String(str || "")
    .replace(/&(amp|lt|gt|quot|#39|apos|nbsp);/gi, (m) => named[m.toLowerCase()] || m)
    .replace(/&#(\d+);/g, (_, code) => String.fromCodePoint(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCodePoint(parseInt(hex, 16)));
}

function mergeOrderedUnique(...lists) {
  const seen = new Set();
  const out = [];
  for (const seq of lists) {
    for (const item of seq || []) {
      const val = String(item || "").trim();
      if (!val) continue;
      const key = normalizeNfc(val).toLowerCase();
      if (!seen.has(key)) {
        seen.add(key);
        out.push(val);
      }
    }
  }
  return out;
}

export function stripHtml(htmlText) {
  let text = String(htmlText || "").replace(/<(script|style)[^>]*>[\s\S]*?<\/\1>/gi, " ");
  text = text.replace(
    /<a\b[^>]*?\bhref\s*=\s*(["'])(https?:\/\/[^"'>\s]+)\1[^>]*>([\s\S]*?)<\/a>/gi,
    (_, _q, rawUrl, rawLabel) => {
      const url = unescapeHtmlEntities(rawUrl).trim();
      const label = unescapeHtmlEntities(String(rawLabel || "").replace(/<[^>]+>/g, " "))
        .replace(/\s+/g, " ")
        .trim();
      if (!label) return url;
      const normUrl = url.toLowerCase().replace(/\/+$/, "");
      const normLabel = label.toLowerCase().replace(/\/+$/, "");
      if (normLabel.includes(normUrl)) return label;
      return `${label} (${url})`;
    }
  );
  text = text.replace(/<br\s*\/?>/gi, "\n");
  text = text.replace(/<\/(?:p|div|li|tr)>/gi, "\n");
  text = text.replace(/<[^>]+>/g, " ");
  text = unescapeHtmlEntities(text);
  text = text.replace(/[ \t]+/g, " ");
  text = text.replace(/\n\s*\n+/g, "\n\n");
  return text.trim();
}

export function extractHtmlMediaUrls(htmlText) {
  if (!htmlText) return [];
  const cleaned = String(htmlText).replace(/<(script|style)[^>]*>[\s\S]*?<\/\1>/gi, " ");
  const urls = [];
  const seen = new Set();
  const imgRe = /<img\b[^>]*?\bsrc\s*=\s*(["'])(https?:\/\/[^"'>\s]+)\1[^>]*>/gi;
  let match;
  while ((match = imgRe.exec(cleaned)) !== null) {
    const tag = match[0];
    const url = unescapeHtmlEntities(match[2]).trim();
    if (!url || seen.has(url)) continue;
    if (/\b(?:width|height)\s*=\s*["']?[01](?:px)?["']?(?:\s|>|\/)/i.test(tag)) continue;
    const lowerUrl = url.toLowerCase();
    if (TRACKING_NOISE.some((n) => lowerUrl.includes(n))) continue;
    seen.add(url);
    urls.push(url);
  }
  return urls;
}

export function hasRegistrationLink(text) {
  if (!text) return false;
  const re = /https?:\/\/[^\s<>"')]+/gi;
  let match;
  while ((match = re.exec(String(text))) !== null) {
    const url = match[0].replace(/[.,;:!?]+$/, "");
    const lowerPath = url.split(/[?#]/, 1)[0].toLowerCase();
    if (url.toLowerCase().includes("unsubscribe")) continue;
    if (STATIC_ASSET_EXT_RE.test(lowerPath)) continue;
    return true;
  }
  return false;
}

export function appendQrLinksToText(bodyText, qrLinks) {
  const text = String(bodyText || "");
  const lowerText = text.toLowerCase();
  const additions = [];
  const seen = new Set();
  for (const rawLink of qrLinks || []) {
    const clean = String(rawLink || "").trim();
    if (!/^https?:\/\//i.test(clean)) continue;
    const key = clean.toLowerCase();
    if (lowerText.includes(key) || seen.has(key)) continue;
    seen.add(key);
    additions.push(`QR Link: ${clean}`);
  }
  if (!additions.length) return text;
  const prefix = text.replace(/\s+$/, "");
  return prefix ? `${prefix}\n\n${additions.join("\n")}` : additions.join("\n");
}

export function extractEmailAddress(fromHeaderOrAddr) {
  const raw = String(fromHeaderOrAddr || "").trim();
  const bracketMatch = raw.match(/<([^<>\s]+@[^<>\s]+)>/);
  if (bracketMatch) return bracketMatch[1].toLowerCase().trim();
  const plainMatch = raw.match(/([^\s<>]+@[^\s<>]+)/);
  return (plainMatch ? plainMatch[1] : raw).toLowerCase().trim();
}

export function sanitizeAllowedSenders(allowedSenders) {
  const out = [];
  for (const s of allowedSenders || []) {
    const clean = String(s || "").trim().toLowerCase();
    if (!clean || UNSAFE_SENDER_DOMAINS.has(clean)) continue;
    out.push(clean);
  }
  return out;
}

export function isSenderAllowed(fromHeaderOrAddr, allowedSenders) {
  const safeAllowlist = sanitizeAllowedSenders(allowedSenders);
  if (!safeAllowlist.length) return true;
  const emailAddr = extractEmailAddress(fromHeaderOrAddr);
  if (!emailAddr || !emailAddr.includes("@")) return false;
  const domain = emailAddr.split("@", 2)[1];
  for (const allowed of safeAllowlist) {
    if (allowed.includes("@")) {
      if (emailAddr === allowed) return true;
    } else if (domain === allowed || domain.endsWith(`.${allowed}`)) {
      return true;
    }
  }
  return false;
}

export function findMatchedKeywords(text, keywords) {
  if (!keywords || !keywords.length || !text) return [];
  const normText = normalizeNfc(text).toLowerCase();
  const matched = [];
  for (const kw of keywords) {
    const kwNorm = normalizeNfc(kw).trim().toLowerCase();
    if (!kwNorm) continue;
    const pattern = new RegExp(
      `(?<![\\p{L}\\p{N}_])${escapeRegExp(kwNorm)}(?![\\p{L}\\p{N}_])`,
      "u"
    );
    if (pattern.test(normText)) matched.push(kw);
  }
  return matched;
}

export function resolveEffectiveConfig(overrideConfig = null) {
  const base = (EMBEDDED_CONFIG && EMBEDDED_CONFIG.filter_config) || {};
  const merged = { ...base, ...(overrideConfig || {}) };
  return {
    allowed_senders: sanitizeAllowedSenders(merged.allowed_senders || []),
    keywords: mergeOrderedUnique(merged.keywords || []),
    excluded_keywords: mergeOrderedUnique(BASELINE_EXCLUDED_KEYWORDS, merged.excluded_keywords || []),
    non_event_keywords:
      merged.non_event_keywords !== undefined
        ? mergeOrderedUnique(merged.non_event_keywords)
        : [...BASELINE_NON_EVENT_KEYWORDS],
    active_registration_cues:
      merged.active_registration_cues !== undefined
        ? mergeOrderedUnique(merged.active_registration_cues)
        : [...BASELINE_ACTIVE_REGISTRATION_CUES],
    newer_than_days: Number(merged.newer_than_days || 14),
    max_results: Number(merged.max_results || 20),
  };
}

export function passesPrivacyAndKeywordGate(fromAddr, subject, bodyText, config = null) {
  const cfg = config ? resolveEffectiveConfig(config) : resolveEffectiveConfig();
  if (!isSenderAllowed(fromAddr, cfg.allowed_senders)) {
    return { passed: false, reason: "sender_not_allowed", matched_keywords: [] };
  }
  const combinedText = `${subject || ""}\n${bodyText || ""}`;

  const excludedHits = findMatchedKeywords(combinedText, cfg.excluded_keywords);
  if (excludedHits.length) {
    return { passed: false, reason: `excluded_private_keyword:${excludedHits[0]}`, matched_keywords: [] };
  }

  if (cfg.non_event_keywords.length) {
    const nonEventHits = findMatchedKeywords(combinedText, cfg.non_event_keywords);
    if (nonEventHits.length) {
      const overrideHits = cfg.active_registration_cues.length
        ? findMatchedKeywords(combinedText, cfg.active_registration_cues)
        : [];
      if (!overrideHits.length) {
        return { passed: false, reason: `excluded_non_event_signal:${nonEventHits[0]}`, matched_keywords: [] };
      }
    }
  }

  const matchedKw = findMatchedKeywords(combinedText, cfg.keywords);
  if (!matchedKw.length) {
    return { passed: false, reason: "no_drl_keyword_match", matched_keywords: [] };
  }
  return { passed: true, reason: "passed", matched_keywords: matchedKw };
}

export async function computeContentHash(fromAddr, subject, bodyText) {
  const emailAddr = extractEmailAddress(fromAddr);
  const normSubj = (normalizeNfc(subject).toLowerCase().match(/\S+/g) || []).join(" ");
  const normBody = (normalizeNfc(bodyText).toLowerCase().match(/\S+/g) || []).join(" ");
  const rawKey = `${emailAddr}|${normSubj}|${normBody}`;
  const bytes = new TextEncoder().encode(rawKey);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  const hex = Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return { content_hash: hex, hash_source: rawKey };
}

function collapseSenderDomains(allowedSenders) {
  const clean = sanitizeAllowedSenders(allowedSenders);
  const domains = Array.from(
    new Set(clean.map((s) => (s.includes("@") ? s.split("@", 2)[1] : s)))
  ).sort((a, b) => a.length - b.length);
  const roots = [];
  for (const d of domains) {
    if (!roots.some((r) => d === r || d.endsWith(`.${r}`))) roots.push(d);
  }
  return roots;
}

export function buildGmailQuery(config = null) {
  const cfg = resolveEffectiveConfig(config);
  const parts = [];
  if (cfg.allowed_senders.length) {
    parts.push(`(${cfg.allowed_senders.map((s) => `from:${s}`).join(" OR ")})`);
  }
  if (cfg.keywords.length) {
    parts.push(`(${cfg.keywords.map((kw) => `"${kw}"`).join(" OR ")})`);
  }
  if (cfg.excluded_keywords.length) {
    parts.push(cfg.excluded_keywords.map((kw) => `-"${kw}"`).join(" "));
  }
  if (cfg.newer_than_days > 0) {
    parts.push(`newer_than:${Math.trunc(cfg.newer_than_days)}d`);
  }
  return parts.join(" ");
}

export function buildOutlookSearchQuery(config = null) {
  const cfg = resolveEffectiveConfig(config);
  const clauses = [];
  const roots = collapseSenderDomains(cfg.allowed_senders);
  if (roots.length === 1) {
    clauses.push(`from:${roots[0]}`);
  } else if (roots.length > 1) {
    clauses.push(`(${roots.slice(0, 5).map((r) => `from:${r}`).join(" OR ")})`);
  }
  if (cfg.newer_than_days > 0) {
    const cutoff = new Date(Date.now() - Math.trunc(cfg.newer_than_days) * 86400 * 1000)
      .toISOString()
      .slice(0, 10);
    clauses.push(`received>=${cutoff}`);
  }
  return clauses.length ? `"${clauses.join(" AND ")}"` : "";
}

export async function decodeQrFromImageBlob(blob, customDecoder = null) {
  if (!blob) return null;
  if (typeof customDecoder === "function") {
    const link = await customDecoder(blob);
    return link && /^https?:\/\//i.test(String(link).trim()) ? String(link).trim() : null;
  }
  if (typeof globalThis.BarcodeDetector !== "function" || typeof globalThis.createImageBitmap !== "function") {
    return null;
  }
  try {
    const detector = new globalThis.BarcodeDetector({ formats: ["qr_code"] });
    const bitmap = await globalThis.createImageBitmap(blob);
    try {
      const codes = await detector.detect(bitmap);
      for (const code of codes || []) {
        const val = String(code.rawValue || "").trim();
        if (/^https?:\/\//i.test(val)) return val;
      }
    } finally {
      if (typeof bitmap.close === "function") bitmap.close();
    }
  } catch {
    return null;
  }
  return null;
}

function loadScriptOnce(src, globalCheck) {
  if (globalCheck()) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const existing = Array.from(document.scripts || []).find((s) => s.src === src);
    if (existing) {
      existing.addEventListener("load", () => resolve(), { once: true });
      existing.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), { once: true });
      return;
    }
    const script = document.createElement("script");
    script.src = src;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error(`Failed to load ${src}`));
    document.head.appendChild(script);
  });
}

export async function requestGmailToken(clientId) {
  const cid = String(clientId || (EMBEDDED_CONFIG.oauth && EMBEDDED_CONFIG.oauth.gmail_client_id) || "").trim();
  if (!cid) throw new Error("Missing Gmail OAuth Web Client ID (GMAIL_WEB_CLIENT_ID).");
  await loadScriptOnce("https://accounts.google.com/gsi/client", () => Boolean(window.google?.accounts?.oauth2));
  return new Promise((resolve, reject) => {
    const client = window.google.accounts.oauth2.initTokenClient({
      client_id: cid,
      scope: "https://www.googleapis.com/auth/gmail.readonly",
      callback: (resp) => {
        if (resp?.error) reject(new Error(resp.error_description || resp.error));
        else if (resp?.access_token) resolve(resp.access_token);
        else reject(new Error("Gmail authorization did not return an access token."));
      },
    });
    client.requestAccessToken({ prompt: "consent" });
  });
}

export async function requestOutlookToken(clientId, redirectUri) {
  const cid = String(clientId || (EMBEDDED_CONFIG.oauth && EMBEDDED_CONFIG.oauth.outlook_client_id) || "").trim();
  if (!cid) throw new Error("Missing Outlook SPA Client ID (OUTLOOK_WEB_CLIENT_ID).");
  await loadScriptOnce(
    "https://alcdn.msauth.net/browser/2.39.0/js/msal-browser.min.js",
    () => Boolean(window.msal?.PublicClientApplication)
  );
  const pca = new window.msal.PublicClientApplication({
    auth: {
      clientId: cid,
      authority: "https://login.microsoftonline.com/common",
      redirectUri: redirectUri || window.location.origin,
    },
    cache: { cacheLocation: "sessionStorage" },
  });
  if (typeof pca.initialize === "function") await pca.initialize();
  const result = await pca.loginPopup({ scopes: ["https://graph.microsoft.com/Mail.Read"] });
  if (!result?.accessToken) throw new Error("Outlook loginPopup did not return an access token.");
  return result.accessToken;
}

function decodeBase64UrlUtf8(b64Url) {
  const b64 = String(b64Url || "").replace(/-/g, "+").replace(/_/g, "/");
  const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
  const bin = atob(padded);
  const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
  return new TextDecoder("utf-8").decode(bytes);
}

function decodeBase64ToBytes(b64Str, urlSafe = false) {
  const norm = urlSafe ? String(b64Str).replace(/-/g, "+").replace(/_/g, "/") : String(b64Str);
  const padded = norm + "=".repeat((4 - (norm.length % 4)) % 4);
  const bin = atob(padded);
  return Uint8Array.from(bin, (c) => c.charCodeAt(0));
}

function extractGmailBodyAndImages(payload) {
  let plainText = "";
  let rawHtml = "";
  const mediaUrls = [];
  const attachmentIds = [];
  const seenMedia = new Set();
  const seenAtt = new Set();

  function walk(part) {
    if (!part) return;
    const mime = String(part.mimeType || "").toLowerCase();
    const filename = String(part.filename || "").toLowerCase();
    const bodyData = part.body?.data;
    const attId = part.body?.attachmentId;

    if (attId && (mime.startsWith("image/") || /\.(png|jpe?g|webp)$/i.test(filename)) && !seenAtt.has(attId)) {
      seenAtt.add(attId);
      attachmentIds.push(attId);
    }
    if (bodyData) {
      const decoded = decodeBase64UrlUtf8(bodyData);
      if (mime === "text/plain" && !plainText) plainText = decoded;
      if (mime === "text/html") {
        if (!rawHtml) rawHtml = decoded;
        for (const u of extractHtmlMediaUrls(decoded)) {
          if (!seenMedia.has(u)) {
            seenMedia.add(u);
            mediaUrls.push(u);
          }
        }
      }
    }
    for (const child of part.parts || []) walk(child);
  }

  walk(payload);
  let text = plainText;
  if (!text && rawHtml) text = stripHtml(rawHtml);
  else if (plainText && !/http/i.test(plainText) && rawHtml && /href/i.test(rawHtml)) {
    text = stripHtml(rawHtml);
  }
  return { text, mediaUrls, attachmentIds };
}

function defaultApiBase() {
  try {
    return new URL(import.meta.url).origin;
  } catch {
    return "";
  }
}

async function fetchJsonOrThrow(url, options = {}) {
  const res = await fetch(url, options);
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(body?.detail || `HTTP ${res.status} from ${url}`);
  }
  return body;
}

async function runBrowserSyncPipeline({
  provider,
  rawMessages,
  fetchAttachmentsFn,
  config,
  apiBase,
  ingestToken,
  dryRun,
  qrDecoder,
  onProgress,
}) {
  const progress = typeof onProgress === "function" ? onProgress : () => {};
  const verifiedByHash = new Map();
  let qrDecodedCount = 0;

  progress(`Filtering ${rawMessages.length} ${provider} message(s) locally...`);
  for (const mail of rawMessages) {
    const gate = passesPrivacyAndKeywordGate(mail.from, mail.subject, mail.text, config);
    if (!gate.passed) continue;

    let bodyText = mail.text;
    if (mail.hasImageAttachments && !hasRegistrationLink(bodyText)) {
      progress(`Checking poster attachments locally for QR link: ${mail.subject || "(no subject)"}`);
      const blobs = await fetchAttachmentsFn(mail, 3);
      const qrLinks = [];
      for (const blob of blobs) {
        const link = await decodeQrFromImageBlob(blob, qrDecoder);
        if (link && !qrLinks.includes(link)) qrLinks.push(link);
      }
      if (qrLinks.length) {
        qrDecodedCount += qrLinks.length;
        bodyText = appendQrLinksToText(bodyText, qrLinks);
      }
    }

    const { content_hash } = await computeContentHash(mail.from, mail.subject, bodyText);
    if (!verifiedByHash.has(content_hash)) {
      verifiedByHash.set(content_hash, {
        message_id: mail.id || content_hash,
        content_hash,
        source: provider,
        subject: mail.subject || "",
        sender: mail.from || "",
        received_at: mail.date || "",
        content: bodyText,
        media_urls: mail.mediaUrls || [],
      });
    }
  }

  const verifiedList = Array.from(verifiedByHash.values());
  const baseReturn = {
    provider,
    scanned: rawMessages.length,
    passed_gate: verifiedList.length,
    skipped_local: rawMessages.length - verifiedList.length,
    qr_decoded: qrDecodedCount,
    already_on_server: 0,
    uploaded: 0,
    created: 0,
    updated: 0,
    duplicates: 0,
    ignored: 0,
    dry_run: Boolean(dryRun),
    emails: verifiedList,
    server_response: null,
  };

  if (!verifiedList.length || dryRun) return baseReturn;

  const base = String(apiBase !== undefined && apiBase !== null ? apiBase : defaultApiBase()).replace(/\/+$/, "");
  const headers = { "Content-Type": "application/json" };
  if (ingestToken) headers["X-Ingest-Token"] = ingestToken;

  progress("Running pre-flight hash check on server...");
  let unseenEmails = verifiedList;
  try {
    const checkData = await fetchJsonOrThrow(`${base}/api/inbox/check`, {
      method: "POST",
      credentials: "include",
      headers,
      body: JSON.stringify({ hashes: verifiedList.map((e) => e.content_hash) }),
    });
    const known = new Set(checkData.known || []);
    unseenEmails = verifiedList.filter((e) => !known.has(e.content_hash));
    baseReturn.already_on_server = verifiedList.length - unseenEmails.length;
  } catch {
    unseenEmails = verifiedList;
  }

  if (!unseenEmails.length) return baseReturn;

  progress(`Uploading ${unseenEmails.length} unseen event email(s)...`);
  const batchRes = await fetchJsonOrThrow(`${base}/api/inbox/batch`, {
    method: "POST",
    credentials: "include",
    headers,
    body: JSON.stringify({ emails: unseenEmails }),
  });

  return {
    ...baseReturn,
    uploaded: unseenEmails.length,
    created: batchRes.created || 0,
    updated: batchRes.updated || 0,
    duplicates: batchRes.duplicates || 0,
    ignored: batchRes.ignored || 0,
    server_response: batchRes,
  };
}

export async function syncGmail(options = {}) {
  const {
    apiBase,
    accessToken,
    clientId,
    ingestToken = "",
    config = null,
    dryRun = false,
    qrDecoder = null,
    onProgress = null,
  } = options;
  const progress = typeof onProgress === "function" ? onProgress : () => {};
  const effectiveCfg = resolveEffectiveConfig(config);
  const token = accessToken || (await requestGmailToken(clientId));
  const q = buildGmailQuery(effectiveCfg);

  progress("Fetching candidate Gmail messages...");
  const listUrl = `https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=${effectiveCfg.max_results}&q=${encodeURIComponent(q)}`;
  const listData = await fetchJsonOrThrow(listUrl, {
    headers: { Authorization: `Bearer ${token}` },
  });

  const rawMessages = [];
  for (const item of listData.messages || []) {
    const msg = await fetchJsonOrThrow(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${encodeURIComponent(item.id)}?format=full`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const payload = msg.payload || {};
    const hdrMap = Object.fromEntries(
      (payload.headers || []).map((h) => [String(h.name || "").toLowerCase(), h.value || ""])
    );
    const { text, mediaUrls, attachmentIds } = extractGmailBodyAndImages(payload);
    rawMessages.push({
      id: msg.id,
      from: hdrMap.from || "",
      subject: hdrMap.subject || "",
      date: hdrMap.date || "",
      text: text || msg.snippet || "",
      mediaUrls,
      attachmentIds,
      hasImageAttachments: attachmentIds.length > 0,
    });
  }

  return runBrowserSyncPipeline({
    provider: "gmail",
    rawMessages,
    config: effectiveCfg,
    apiBase,
    ingestToken,
    dryRun,
    qrDecoder,
    onProgress,
    fetchAttachmentsFn: async (mail, maxImages) => {
      const blobs = [];
      for (const attId of (mail.attachmentIds || []).slice(0, maxImages)) {
        try {
          const att = await fetchJsonOrThrow(
            `https://gmail.googleapis.com/gmail/v1/users/me/messages/${encodeURIComponent(mail.id)}/attachments/${encodeURIComponent(attId)}`,
            { headers: { Authorization: `Bearer ${token}` } }
          );
          if (att.data) {
            const bytes = decodeBase64ToBytes(att.data, true);
            if (bytes.length && bytes.length <= 10 * 1024 * 1024) blobs.push(new Blob([bytes]));
          }
        } catch {
          // Ignore individual attachment failures
        }
      }
      return blobs;
    },
  });
}

export async function syncOutlook(options = {}) {
  const {
    apiBase,
    accessToken,
    clientId,
    redirectUri,
    ingestToken = "",
    config = null,
    dryRun = false,
    qrDecoder = null,
    onProgress = null,
  } = options;
  const progress = typeof onProgress === "function" ? onProgress : () => {};
  const effectiveCfg = resolveEffectiveConfig(config);
  const token = accessToken || (await requestOutlookToken(clientId, redirectUri));
  const searchQuery = buildOutlookSearchQuery(effectiveCfg);
  const effectiveTop = Math.min(Math.max(effectiveCfg.max_results || 20, 50), 100);

  progress("Fetching HCMUS Outlook messages...");
  const params = new URLSearchParams({
    $top: String(effectiveTop),
    $select: "id,subject,from,receivedDateTime,bodyPreview,body,hasAttachments",
  });
  if (searchQuery) params.set("$search", searchQuery);

  const listData = await fetchJsonOrThrow(`https://graph.microsoft.com/v1.0/me/messages?${params.toString()}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      ConsistencyLevel: "eventual",
    },
  });

  const rawMessages = (listData.value || []).map((msg) => {
    const sender = msg.from?.emailAddress || {};
    const fromFormatted = sender.name ? `${sender.name} <${sender.address || ""}>` : sender.address || "";
    const bodyObj = msg.body || {};
    const rawContent = bodyObj.content || msg.bodyPreview || "";
    const isHtml = String(bodyObj.contentType || "").toLowerCase() === "html";
    return {
      id: msg.id || "",
      from: fromFormatted,
      subject: msg.subject || "",
      date: msg.receivedDateTime || "",
      text: isHtml ? stripHtml(rawContent) : rawContent,
      mediaUrls: isHtml ? extractHtmlMediaUrls(rawContent) : [],
      hasImageAttachments: Boolean(msg.hasAttachments) || /cid:/i.test(rawContent),
    };
  });

  return runBrowserSyncPipeline({
    provider: "outlook",
    rawMessages,
    config: effectiveCfg,
    apiBase,
    ingestToken,
    dryRun,
    qrDecoder,
    onProgress,
    fetchAttachmentsFn: async (mail, maxImages) => {
      const blobs = [];
      try {
        const attData = await fetchJsonOrThrow(
          `https://graph.microsoft.com/v1.0/me/messages/${encodeURIComponent(mail.id)}/attachments`,
          { headers: { Authorization: `Bearer ${token}` } }
        );
        for (const att of attData.value || []) {
          if (att["@odata.type"] !== "#microsoft.graph.fileAttachment" || !att.contentBytes) continue;
          const ctype = String(att.contentType || "").toLowerCase();
          const name = String(att.name || "").toLowerCase();
          if (!ctype.startsWith("image/") && !/\.(png|jpe?g|webp)$/i.test(name)) continue;
          const bytes = decodeBase64ToBytes(att.contentBytes, false);
          if (bytes.length && bytes.length <= 10 * 1024 * 1024) {
            blobs.push(new Blob([bytes], { type: ctype || "image/png" }));
            if (blobs.length >= maxImages) break;
          }
        }
      } catch {
        // Ignore individual attachment failures
      }
      return blobs;
    },
  });
}
