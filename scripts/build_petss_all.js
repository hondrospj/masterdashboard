#!/usr/bin/env node

/**
 * Build PETSS mean storm-tide TWL forecasts for all stations in the East region.
 *
 * Output:
 *   data/petss_forecasts_all_mllw.json
 *
 * What this script does:
 *   - fetches the latest available PETSS MEAN storm-tide text product
 *   - parses station blocks robustly
 *   - reads ONLY:
 *       column 0 = TIME (YYYYMMDDHHMM)
 *       column 5 = TWL
 *   - drops malformed times
 *   - drops 9999.000 missing values
 *   - drops absurd values outside a very wide sanity range
 *
 * This is designed specifically to prevent bad parses like:
 *   2026-18-13T07:00:00Z
 *   fcst: 32
 */

const fs = require("fs");
const path = require("path");

const OUT_PATH = path.join(__dirname, "..", "data", "petss_forecasts_all_mllw.json");

const PRODUCT_REGION = "east";
const PRODUCT_NAME = "mean"; // IMPORTANT: use ensemble mean, not e90/e10
const PRODUCT_KIND = "stormtide";
const BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/petss/prod";

const FETCH_TIMEOUT_MS = 25000;
const FETCH_RETRIES = 3;
const RETRY_WAIT_MS = 2500;

// Wide sanity bounds for East Coast TWL in feet MLLW.
// These are only a last-resort guard against bad parsing.
const MIN_REASONABLE_TWL_FT = -20;
const MAX_REASONABLE_TWL_FT = 25;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function utcDateString(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

function isoFromCycle(dateStr, cycle) {
  const y = Number(dateStr.slice(0, 4));
  const m = Number(dateStr.slice(4, 6));
  const d = Number(dateStr.slice(6, 8));
  const h = Number(cycle);
  return new Date(Date.UTC(y, m - 1, d, h, 0, 0)).toISOString();
}

function buildUrl(dateStr, cycle) {
  return `${BASE_URL}/petss.${dateStr}/petss.t${cycle}z.${PRODUCT_NAME}.${PRODUCT_KIND}.${PRODUCT_REGION}.txt`;
}

function buildCandidateRuns() {
  const now = new Date();
  const days = [];
  for (let back = 0; back <= 2; back++) {
    const d = new Date(Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate() - back,
      0, 0, 0
    ));
    days.push(utcDateString(d));
  }

  const cycles = ["18", "12", "06", "00"];
  const out = [];
  for (const day of days) {
    for (const cycle of cycles) {
      out.push({
        dateStr: day,
        cycle,
        url: buildUrl(day, cycle)
      });
    }
  }
  return out;
}

async function fetchText(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "User-Agent": "petss-builder/1.0"
      }
    });

    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
    }

    return await res.text();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchTextWithRetry(url) {
  let lastErr = null;

  for (let attempt = 1; attempt <= FETCH_RETRIES; attempt++) {
    try {
      return await fetchText(url);
    } catch (err) {
      lastErr = err;
      if (attempt < FETCH_RETRIES) {
        console.warn(`Fetch failed (${attempt}/${FETCH_RETRIES}) for ${url}: ${err.message}`);
        await sleep(RETRY_WAIT_MS * attempt);
      }
    }
  }

  throw lastErr;
}

async function fetchLatestAvailableProduct() {
  const candidates = buildCandidateRuns();

  for (const c of candidates) {
    try {
      console.log(`Trying ${c.url}`);
      const text = await fetchTextWithRetry(c.url);

      if (typeof text === "string" && text.includes("TIME") && text.includes("TWL")) {
        return {
          dateStr: c.dateStr,
          cycle: c.cycle,
          url: c.url,
          text
        };
      }
    } catch (err) {
      console.warn(`Skipping ${c.url}: ${err.message}`);
    }
  }

  throw new Error("Could not fetch any recent PETSS mean storm-tide text product.");
}

function parsePetssTimeUTC(raw) {
  const s = String(raw || "").trim();
  if (!/^\d{12}$/.test(s)) return null;

  const year = Number(s.slice(0, 4));
  const month = Number(s.slice(4, 6));
  const day = Number(s.slice(6, 8));
  const hour = Number(s.slice(8, 10));
  const minute = Number(s.slice(10, 12));

  if (
    month < 1 || month > 12 ||
    day < 1 || day > 31 ||
    hour < 0 || hour > 23 ||
    minute < 0 || minute > 59
  ) {
    return null;
  }

  const ms = Date.UTC(year, month - 1, day, hour, minute, 0);
  const d = new Date(ms);

  // strict validation to reject impossible dates
  if (
    d.getUTCFullYear() !== year ||
    d.getUTCMonth() !== month - 1 ||
    d.getUTCDate() !== day ||
    d.getUTCHours() !== hour ||
    d.getUTCMinutes() !== minute
  ) {
    return null;
  }

  return d.toISOString();
}

function parseMaybeNumber(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return null;

  const v = Number(s);
  if (!Number.isFinite(v)) return null;
  if (v === 9999 || v === 9999.0 || v === 9999.000) return null;

  return v;
}

function isReasonableTwl(v) {
  return Number.isFinite(v) && v >= MIN_REASONABLE_TWL_FT && v <= MAX_REASONABLE_TWL_FT;
}

function detectStationId(line) {
  const s = String(line || "").trim();

  // Avoid matching TIME rows
  if (/^\d{12}\s*,/.test(s)) return null;

  const patterns = [
    /\b(?:station|stn|gage|gauge|site|nos)\s*[:#-]?\s*(\d{7})\b/i,
    /^\s*(\d{7})\b/,
    /\b(\d{7})\b/
  ];

  for (const re of patterns) {
    const m = s.match(re);
    if (m) return m[1];
  }

  return null;
}

function isHeaderLine(line) {
  const s = String(line || "").trim().toUpperCase();
  return s.includes("TIME") && s.includes("TWL");
}

function isDataLine(line) {
  return /^\s*\d{12}\s*,/.test(String(line || ""));
}

function parseDataRow(line) {
  const cols = String(line)
    .split(",")
    .map(s => s.trim());

  // Expected:
  // TIME,TIDE,OB,SURGE,BIAS,TWL,SURGE90p,TWL90p,SURGE10p,TWL10p
  if (cols.length < 6) return null;

  const timeIso = parsePetssTimeUTC(cols[0]);
  const twl = parseMaybeNumber(cols[5]);

  if (!timeIso) return null;
  if (twl == null) return null;
  if (!isReasonableTwl(twl)) return null;

  return {
    t: timeIso,
    fcst: Math.round(twl * 1000) / 1000
  };
}

function parsePetssText(text) {
  const lines = String(text || "")
    .replace(/\r/g, "")
    .split("\n");

  const stations = {};
  let currentStation = null;
  let headerSeenForStation = false;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    const stationId = detectStationId(line);
    if (stationId) {
      currentStation = stationId;
      headerSeenForStation = false;
      if (!stations[currentStation]) {
        stations[currentStation] = { points: [] };
      }
      continue;
    }

    if (isHeaderLine(line)) {
      if (currentStation) {
        headerSeenForStation = true;
      }
      continue;
    }

    if (!currentStation || !headerSeenForStation) continue;
    if (!isDataLine(line)) continue;

    const row = parseDataRow(line);
    if (!row) continue;

    stations[currentStation].points.push(row);
  }

  // sort and dedupe
  for (const id of Object.keys(stations)) {
    const seen = new Set();

    stations[id].points = stations[id].points
      .filter(p => p && p.t && Number.isFinite(p.fcst))
      .sort((a, b) => new Date(a.t).getTime() - new Date(b.t).getTime())
      .filter(p => {
        const key = `${p.t}|${p.fcst}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });

    if (!stations[id].points.length) {
      delete stations[id];
    }
  }

  return stations;
}

function buildOutput({ dateStr, cycle, url, stations }) {
  return {
    issued_utc: new Date().toISOString(),
    model_time_utc: isoFromCycle(dateStr, cycle),
    source_url: url,
    source_date_utc: dateStr,
    source_cycle_utc: cycle,
    source_product: `${PRODUCT_NAME}.${PRODUCT_KIND}.${PRODUCT_REGION}.txt`,
    datum: "MLLW",
    value_column: "TWL",
    stations
  };
}

async function main() {
  try {
    const run = await fetchLatestAvailableProduct();
    const stations = parsePetssText(run.text);

    const stationCount = Object.keys(stations).length;
    if (!stationCount) {
      throw new Error("Parsed zero stations from PETSS text product.");
    }

    const out = buildOutput({
      dateStr: run.dateStr,
      cycle: run.cycle,
      url: run.url,
      stations
    });

    fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
    fs.writeFileSync(OUT_PATH, JSON.stringify(out, null, 2) + "\n", "utf8");

    let pointCount = 0;
    for (const id of Object.keys(stations)) {
      pointCount += stations[id].points.length;
    }

    console.log(`Wrote ${OUT_PATH}`);
    console.log(`Stations: ${stationCount}`);
    console.log(`Points: ${pointCount}`);
    console.log(`Source: ${run.url}`);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
