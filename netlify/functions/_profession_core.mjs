import { Pool } from "pg";
import https from "https";
import http from "http";
import zlib from "zlib";
import { load as cheerioLoad } from "cheerio";
import { loadFilters } from "./load_filters.mjs";
import { logFetchError } from "./_error-logger.mjs";

let _filters = [];

/* ---------------------
   DB connection
--------------------- */
const connectionString = process.env.NETLIFY_DATABASE_URL;
if (!connectionString) throw new Error("NETLIFY_DATABASE_URL is not set");

const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false },
});

/* ---------------------
   Helper functions
--------------------- */
function normalizeText(s) {
  return String(s ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function normalizeWhitespace(s) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}

function normalizeUrl(raw) {
  try {
    const u = new URL(raw);
    u.hash = "";
    [
      "utm_source", "utm_medium", "utm_campaign", "utm_term",
      "utm_content", "fbclid", "gclid", "sessionId",
    ].forEach((p) => u.searchParams.delete(p));
    return u.toString().replace(/\?$/, "");
  } catch {
    return raw;
  }
}

function dedupeByUrl(items) {
  const seen = new Set();
  return items.filter((x) => {
    if (!x.url) return false;
    const u = normalizeUrl(x.url);
    if (seen.has(u)) return false;
    seen.add(u);
    x.url = u;
    return true;
  });
}

function titleNotBlacklisted(title, desc) {
  const combined = normalizeText(`${title ?? ""} ${desc ?? ""}`);
  return !_filters.some((kw) => combined.includes(normalizeText(kw)));
}

function looksLikeJobUrl(url) {
  if (!url) return false;
  try {
    const u = new URL(url);
    if (!u.hostname.includes("profession.hu")) return false;
    if (u.pathname.startsWith("/allas/")) return true;
    return false;
  } catch {
    return false;
  }
}

/* ---------------------
   Fetch helper
--------------------- */
function fetchText(url, redirectLeft = 5) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = u.protocol === "https:" ? https : http;

    const req = lib.request(
      u,
      {
        method: "GET",
        headers: {
          "User-Agent": "JobWatcher/1.0",
          Accept: "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
          "Accept-Language": "hu-HU,hu;q=0.9,en;q=0.8",
          "Accept-Encoding": "gzip,deflate,br",
        },
        timeout: 50000,
      },
      (res) => {
        const code = res.statusCode || 0;

        if ([301, 302, 303, 307, 308].includes(code)) {
          const loc = res.headers.location;
          if (!loc) return reject(new Error(`HTTP ${code} (no Location) for ${url}`));
          if (redirectLeft <= 0) return reject(new Error(`Too many redirects for ${url}`));
          const nextUrl = new URL(loc, url).toString();
          res.resume();
          return resolve(fetchText(nextUrl, redirectLeft - 1));
        }

        const enc = String(res.headers["content-encoding"] || "").toLowerCase();
        let stream = res;
        if (enc.includes("gzip")) stream = res.pipe(zlib.createGunzip());
        else if (enc.includes("deflate")) stream = res.pipe(zlib.createInflate());
        else if (enc.includes("br")) stream = res.pipe(zlib.createBrotliDecompress());

        let data = "";
        stream.setEncoding("utf8");
        stream.on("data", (chunk) => (data += chunk));
        stream.on("end", () => {
          if (code >= 200 && code < 300) resolve(data);
          else reject(new Error(`HTTP ${code} for ${url}`));
        });
        stream.on("error", reject);
      }
    );

    req.on("timeout", () => req.destroy(new Error(`Timeout for ${url}`)));
    req.on("error", reject);
    req.end();
  });
}

/* ---------------------
   HTML extraction
--------------------- */
const CTA_TITLES = new Set([
  "megnézem", "megnezem", "részletek", "reszletek",
  "tovább", "tovabb", "bővebben", "bovebben",
  "jelentkezem", "jelentkezés", "jelentkezes",
  "apply", "details", "view", "open", "more",
]);

function isCtaTitle(s) {
  const n = normalizeText(s);
  return !n || n.length < 4 || CTA_TITLES.has(n);
}

function extractCandidates(html, baseUrl) {
  const $ = cheerioLoad(html);
  const items = [];

  $("a[href]").each((_, el) => {
    const href = $(el).attr("href");
    if (!href) return;

    let url;
    try {
      url = new URL(href, baseUrl).toString();
    } catch {
      return;
    }
    if (!/^https?:\/\//i.test(url)) return;
    if (/\.(jpg|jpeg|png|gif|svg|webp|pdf|zip|rar|7z)(\?|#|$)/i.test(url)) return;

    let card = $(el).closest("article, li, .job-list-item, .job, .position, .listing, .card, .item");
    if (!card.length) card = $(el).closest("div");

    const linkText = normalizeWhitespace($(el).text());
    const headingText =
      normalizeWhitespace(card.find("h1,h2,h3,h4,h5,h6").first().text()) ||
      normalizeWhitespace($(el).parent().find("h1,h2,h3,h4,h5,h6").first().text());

    let title = linkText;
    if (headingText && (isCtaTitle(linkText) || headingText.length > linkText.length + 3)) {
      title = headingText;
    }

    title = normalizeWhitespace(title);
    if (!title || title.length < 4) return;

    const desc =
      normalizeWhitespace(card.find("p").first().text()) ||
      normalizeWhitespace(card.find(".description, .job-desc, .job-description").first().text()) ||
      null;

    items.push({
      title: title.slice(0, 300),
      url,
      description: desc ? desc.slice(0, 800) : null,
    });
  });

  return dedupeByUrl(items);
}

/* ---------------------
   DB upsert
--------------------- */
async function upsertJob(client, source, item) {
  const canonicalUrl = normalizeUrl(item.url);

  await client.query(
    `INSERT INTO marketing_job_posts
      (source, title, url, first_seen)
     VALUES ($1,$2,$3,NOW())
     ON CONFLICT (source, url) WHERE url IS NOT NULL
     DO NOTHING;`,
    [source, item.title, canonicalUrl]
  );
}

/* ---------------------
   Main exported function
--------------------- */
export async function processProfessionSources(SOURCES, callerName, request) {
  _filters = await loadFilters();

  const url = new URL(request.url);
  const debug = url.searchParams.get("debug") === "1";
  const write = url.searchParams.get("write") === "1";

  const client = (!debug || write) ? await pool.connect() : null;

  const stats = {
    ok: true,
    callerName,
    ranAt: new Date().toISOString(),
    debug: !!debug,
    write: !debug || write,
    totalSources: SOURCES.length,
    portals: [],
  };

  try {
    for (const p of SOURCES) {
      const source = p.key;

      let html = null;
      try {
        html = await fetchText(p.url);
      } catch (err) {
        if (/HTTP\s+[45]\d{2}/i.test(err.message)) {
          await logFetchError(callerName, { url: p.url, message: err.message });
        }
        stats.portals.push({ source, label: p.label, url: p.url, ok: false, error: err.message });
        continue;
      }

      const merged = extractCandidates(html, p.url)
        .filter((c) => looksLikeJobUrl(c.url));

      const matchedList = merged.filter((c) => titleNotBlacklisted(c.title, c.description));

      stats.portals.push({
        source,
        label: p.label,
        url: p.url,
        ok: true,
        matched: matchedList.length,
      });

      if (client) {
        for (const item of matchedList) {
          await upsertJob(client, source, item);
        }
      }
    }
  } finally {
    if (client) client.release();
  }

  if (!debug) {
    return new Response("Cron done", { status: 200 });
  }

  return new Response(JSON.stringify(stats), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}
