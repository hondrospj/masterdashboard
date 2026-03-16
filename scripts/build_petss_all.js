#!/usr/bin/env node

/**
 * Build PETSS TWL forecasts from NOMADS csv.tar.gz products.
 *
 * Why this version:
 * - The *.mean.stormtide.east.txt product is NOT a CSV table.
 * - The directory also publishes petss.tXXz.csv.tar.gz.
 * - We extract CSVs, locate files/rows containing TIME + TWL, and parse them robustly.
 *
 * Output:
 *   data/petss_forecasts_all_mllw.json
 */

const fs = require("fs");
const path = require("path");
const os = require("os");
const https = require("https");
const { execFileSync } = require("child_process");

const OUT_PATH = path.join(__dirname, "..", "data", "petss_forecasts_all_mllw.json");

const BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/petss/prod";
const REGION = "east";
const FETCH_TIMEOUT_MS = 30000;
const RETRIES = 3;

// broad sanity limits, just to block corrupted parses
const MIN_TWL_FT = -20;
const MAX_TWL_FT = 25;

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
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

function buildCandidateRuns() {
  const now = new Date();
  const cycles = ["18", "12", "06", "00"];
  const out = [];

  for (let back = 0; back <= 2; back++) {
    const d = new Date(Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate() - back,
      0, 0, 0
    ));
    const dateStr = utcDateString(d);

    for (const cycle of cycles) {
      out.push({
        dateStr,
        cycle,
        csvTarUrl: `${BASE_URL}/petss.${dateStr}/petss.t${cycle}z.csv.tar.gz`,
        txtUrl: `${BASE_URL}/petss.${dateStr}/petss.t${cycle}z.mean.stormtide.${REGION}.txt`
      });
    }
  }

  return out;
}

function downloadToFile(url, outPath, timeoutMs = FETCH_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      { headers: { "User-Agent": "petss-builder/2.0" } },
      (res) => {
        if (res.statusCode !== 200) {
          res.resume();
          reject(new Error(`HTTP ${res.statusCode} for ${url}`));
          return;
        }

        const file = fs.createWriteStream(outPath);
        res.pipe(file);

        file.on("finish", () => {
          file.close(() => resolve());
        });

        file.on("error", (err) => {
          try { fs.unlinkSync(outPath); } catch {}
          reject(err);
        });
      }
    );

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`Timeout fetching ${url}`));
    });

    req.on("error", reject);
  });
}

async function downloadWithRetry(url, outPath) {
  let lastErr = null;
  for (let i = 1; i <= RETRIES; i++) {
    try {
      await downloadToFile(url, outPath);
      return;
    } catch (err) {
      lastErr = err;
      if (i < RETRIES) {
        console.warn(`Download failed (${i}/${RETRIES}) for ${url}: ${err.message}`);
        await sleep(1500 * i);
      }
    }
  }
  throw lastErr;
}

function fileExistsAndNonEmpty(p) {
  try {
    const st = fs.statSync(p);
    return st.isFile() && st.size > 0;
  } catch {
    return false;
  }
}

function walkFiles(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walkFiles(p));
    else if (entry.isFile()) out.push(p);
  }
  return out;
}

function splitCsvLine(line) {
  // basic CSV splitter with quote handling
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }

    cur += ch;
  }

  out.push(cur);
  return out.map(s => s.trim());
}

function detectDelimiter(line) {
  if (line.includes(",")) return ",";
  if (line.includes("\t")) return "\t";
  return ",";
}

function splitLine(line, delimiter) {
  if (delimiter === ",") return splitCsvLine(line);
  return line.split("\t").map(s => s.trim());
}

function normalizeHeaderName(s) {
  return String(s || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "")
    .replace(/[^A-Z0-9]/g, "");
}

function findHeaderIndex(headers, options) {
  const normalized = headers.map(normalizeHeaderName);
  for (const opt of options) {
    const want = normalizeHeaderName(opt);
    const idx = normalized.indexOf(want);
    if (idx >= 0) return idx;
  }
  return -1;
}

function parsePetssTimeUTC(raw) {
  const s = String(raw || "").trim();

  if (/^\d{12}$/.test(s)) {
    const year = Number(s.slice(0, 4));
    const month = Number(s.slice(4, 6));
    const day = Number(s.slice(6, 8));
    const hour = Number(s.slice(8, 10));
    const minute = Number(s.slice(10, 12));

    const ms = Date.UTC(year, month - 1, day, hour, minute, 0);
    const d = new Date(ms);

    if (
      d.getUTCFullYear() === year &&
      d.getUTCMonth() === month - 1 &&
      d.getUTCDate() === day &&
      d.getUTCHours() === hour &&
      d.getUTCMinutes() === minute
    ) {
      return d.toISOString();
    }
    return null;
  }

  // fallback for values like 2026-03-11 12:00 or ISO strings
  const isoLike = s.replace(" ", "T");
  const d = new Date(isoLike.endsWith("Z") ? isoLike : `${isoLike}Z`);
  return Number.isFinite(d.getTime()) ? d.toISOString() : null;
}

function parseMaybeNumber(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return null;

  const v = Number(s);
  if (!Number.isFinite(v)) return null;

  // PETSS missing sentinels
  if (v === 9999 || v === 9999.0 || v === 9999.000) return null;
  if (v === -400 || v === -400.0) return null;

  return v;
}

function isReasonableTwl(v) {
  return Number.isFinite(v) && v >= MIN_TWL_FT && v <= MAX_TWL_FT;
}

function stationIdFromText(s) {
  const m = String(s || "").match(/\b\d{7}\b/);
  return m ? m[0] : null;
}

function detectStationIdForFile(filePath, headers, row) {
  const fileName = path.basename(filePath);

  const headerCandidates = [
    "NOS_ID", "NOSID", "STATION", "STATION_ID", "STATIONID",
    "SITE", "SITE_ID", "GAUGE", "GAUGE_ID", "ID"
  ];

  for (const name of headerCandidates) {
    const idx = findHeaderIndex(headers, [name]);
    if (idx >= 0 && idx < row.length) {
      const sid = stationIdFromText(row[idx]);
      if (sid) return sid;
    }
  }

  const fromFile = stationIdFromText(fileName);
  if (fromFile) return fromFile;

  return null;
}

function parseCsvFileForPoints(filePath) {
  const text = fs.readFileSync(filePath, "utf8").replace(/\r/g, "");
  const lines = text.split("\n").filter(Boolean);
  if (lines.length < 2) return null;

  const delimiter = detectDelimiter(lines[0]);
  const headers = splitLine(lines[0], delimiter);

  const timeIdx = findHeaderIndex(headers, ["TIME", "DATETIME", "VALIDTIME", "DATE_TIME", "T"]);
  const twlIdx  = findHeaderIndex(headers, ["TWL", "STORMTIDE", "TOTALWATERLEVEL", "TOTAL_WATER_LEVEL"]);

  if (timeIdx < 0 || twlIdx < 0) return null;

  const pointsByStation = new Map();

  for (let i = 1; i < lines.length; i++) {
    const row = splitLine(lines[i], delimiter);
    if (row.length <= Math.max(timeIdx, twlIdx)) continue;

    const t = parsePetssTimeUTC(row[timeIdx]);
    const twl = parseMaybeNumber(row[twlIdx]);

    if (!t || twl == null || !isReasonableTwl(twl)) continue;

    const sid = detectStationIdForFile(filePath, headers, row);
    if (!sid) continue;

    if (!pointsByStation.has(sid)) pointsByStation.set(sid, []);
    pointsByStation.get(sid).push({
      t,
      fcst: Math.round(twl * 1000) / 1000
    });
  }

  if (!pointsByStation.size) return null;
  return pointsByStation;
}

function mergeStationMaps(maps) {
  const stations = {};

  for (const mp of maps) {
    if (!mp) continue;

    for (const [sid, pts] of mp.entries()) {
      if (!stations[sid]) stations[sid] = { points: [] };
      stations[sid].points.push(...pts);
    }
  }

  for (const sid of Object.keys(stations)) {
    const seen = new Set();
    stations[sid].points = stations[sid].points
      .filter(p => p && p.t && Number.isFinite(p.fcst))
      .sort((a, b) => new Date(a.t).getTime() - new Date(b.t).getTime())
      .filter(p => {
        const k = `${p.t}|${p.fcst}`;
        if (seen.has(k)) return false;
        seen.add(k);
        return true;
      });

    if (!stations[sid].points.length) delete stations[sid];
  }

  return stations;
}

function buildOutput({ dateStr, cycle, csvTarUrl, stations }) {
  return {
    issued_utc: new Date().toISOString(),
    model_time_utc: isoFromCycle(dateStr, cycle),
    source_url: csvTarUrl,
    source_date_utc: dateStr,
    source_cycle_utc: cycle,
    source_product: `petss.t${cycle}z.csv.tar.gz`,
    datum: "MLLW",
    value_column: "TWL",
    region: REGION,
    stations
  };
}

async function fetchLatestAvailableCsvRun() {
  const candidates = buildCandidateRuns();

  for (const c of candidates) {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "petss-"));
    const tarPath = path.join(tmpDir, "petss.tar.gz");
    const extractDir = path.join(tmpDir, "x");
    fs.mkdirSync(extractDir);

    try {
      console.log(`Trying ${c.csvTarUrl}`);
      await downloadWithRetry(c.csvTarUrl, tarPath);

      if (!fileExistsAndNonEmpty(tarPath)) {
        throw new Error("Downloaded archive is empty.");
      }

      execFileSync("tar", ["-xzf", tarPath, "-C", extractDir], { stdio: "pipe" });

      const files = walkFiles(extractDir).filter(f => f.toLowerCase().endsWith(".csv"));
      if (!files.length) {
        throw new Error("Archive extracted but no CSV files were found.");
      }

      const parsedMaps = files.map(parseCsvFileForPoints).filter(Boolean);
      const stations = mergeStationMaps(parsedMaps);

      if (!Object.keys(stations).length) {
        throw new Error("CSV archive found, but no station TIME/TWL rows were parsed.");
      }

      return {
        dateStr: c.dateStr,
        cycle: c.cycle,
        csvTarUrl: c.csvTarUrl,
        stations
      };
    } catch (err) {
      console.warn(`Skipping ${c.csvTarUrl}: ${err.message}`);
    } finally {
      try {
        fs.rmSync(tmpDir, { recursive: true, force: true });
      } catch {}
    }
  }

  throw new Error("Could not fetch and parse any recent PETSS csv.tar.gz run.");
}

async function main() {
  try {
    const run = await fetchLatestAvailableCsvRun();
    const out = buildOutput(run);

    fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
    fs.writeFileSync(OUT_PATH, JSON.stringify(out, null, 2) + "\n", "utf8");

    const stationCount = Object.keys(out.stations).length;
    const pointCount = Object.values(out.stations)
      .reduce((sum, s) => sum + (Array.isArray(s.points) ? s.points.length : 0), 0);

    console.log(`Wrote ${OUT_PATH}`);
    console.log(`Stations: ${stationCount}`);
    console.log(`Points: ${pointCount}`);
    console.log(`Source: ${run.csvTarUrl}`);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
