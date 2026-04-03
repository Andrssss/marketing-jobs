const { Pool } = require("pg");

const connectionString = process.env.NETLIFY_DATABASE_URL;
if (!connectionString) {
  console.error("NETLIFY_DATABASE_URL nincs beállítva.");
  throw new Error("NETLIFY_DATABASE_URL environment variable is not set.");
}

const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false },
});

const ALLOWED_ORIGIN =
  process.env.ALLOWED_ORIGIN || "https://marketing-jobs.netlify.app";
const MAX_LIMIT = 2000;

function jsonResponse(statusCode, body, extraHeaders = {}) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
      ...extraHeaders,
    },
    body: JSON.stringify(body),
  };
}

function parseLimit(rawLimit) {
  const parsed = Number.parseInt(rawLimit || "100", 10);

  if (!Number.isFinite(parsed)) {
    return 100;
  }

  return Math.min(Math.max(parsed, 1), MAX_LIMIT);
}

exports.handler = async (event) => {
  let client;
  try {
    const method = event.httpMethod;
    const path = event.path || "";

    if (method === "OPTIONS") {
      return {
        statusCode: 204,
        headers: {
          "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
          "Access-Control-Allow-Methods": "GET,OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
        body: "",
      };
    }

    if (method !== "GET") {
      return jsonResponse(405, { error: "Nem támogatott HTTP metódus." });
    }

    const qs = event.queryStringParameters || {};
    const timeRangeRaw = String(qs.timeRange || "").toLowerCase();
    const timeRange =
      timeRangeRaw === "24h" || timeRangeRaw === "1d"
        ? "24h"
        : timeRangeRaw === "7d" || timeRangeRaw === "1w"
        ? "7d"
        : null;
    const limit = parseLimit(qs.limit);
    const source = qs.source || null;
    client = await pool.connect();

    // GET /marketing-jobs/sources
    if (
      path.endsWith("/marketing-jobs/sources") ||
      path.endsWith("/marketing-jobs/sources/")
    ) {
      const { rows } = await client.query(
        `SELECT source, COUNT(*)::int AS count
         FROM marketing_job_posts
         GROUP BY source
         ORDER BY count DESC`
      );

      return jsonResponse(200, rows, {
        "Cache-Control": "public, max-age=60",
      });
    }

    // GET /marketing-jobs (list)
    let nextParam = 1;

    let timeWhere = "";
    if (timeRange === "24h") {
      timeWhere = `AND first_seen >= NOW() - INTERVAL '24 hours'`;
    } else if (timeRange === "7d") {
      timeWhere = `AND first_seen >= NOW() - INTERVAL '7 days'`;
    }

    let sourceWhere = "";
    const params = [];
    if (source) {
      sourceWhere = `AND source = $${nextParam}`;
      params.push(source);
      nextParam++;
    }

    params.push(limit);
    const limitParam = `$${nextParam}`;

    const query = `
      SELECT source, title, url,
             first_seen AS "firstSeen",
             experience
      FROM marketing_job_posts
      WHERE TRUE
        ${timeWhere}
        ${sourceWhere}
      ORDER BY first_seen DESC, id DESC
      LIMIT ${limitParam}
    `;

    const { rows } = await client.query(query, params);
    return jsonResponse(200, rows, {
      "Cache-Control": "public, max-age=60",
    });
  } catch (err) {
    console.error("Function error:", err);
    return jsonResponse(500, { error: "Szerver hiba", details: err.message });
  } finally {
    client?.release();
  }
};
