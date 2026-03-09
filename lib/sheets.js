/**
 * Google Sheets Data Fetcher
 * Reads raw call data from the "Transcripts" tab and transforms
 * it into the object shape that classifyCall() expects.
 */
const { google } = require("googleapis");

const SHEET_ID = process.env.GOOGLE_SHEET_ID || "1TVfdnmAx9qagVnt55k9r1DCzmg7c1TIb5_ghdwub58s";
const SHEET_TAB = process.env.GOOGLE_SHEET_TAB || "Transcripts";

async function fetchFromSheets() {
  const apiKey = process.env.GOOGLE_API_KEY;
  if (!apiKey) throw new Error("GOOGLE_API_KEY environment variable not set");

  const sheets = google.sheets({ version: "v4", auth: apiKey });

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_TAB}!A:U`,
  });

  const rows = res.data.values;
  if (!rows || rows.length < 2) return [];

  const headers = rows[0];
  const dataRows = rows.slice(1);

  return dataRows.map((row) => rowToCallObject(row, headers));
}

function rowToCallObject(row, headers) {
  const obj = {};
  headers.forEach((h, i) => {
    obj[h] = row[i] || "";
  });

  return {
    call_sid: obj.call_sid || "",
    date: obj.date || "",
    start_time: obj.start_time || "",
    end_time: obj.end_time || "",
    from_number: obj.from_number || "",
    to_number: obj.to_number || "",
    exotel_number: obj.exotel_number || "",
    direction: obj.direction || "",
    status: obj.status || "",
    duration_seconds: parseInt(obj.duration_seconds) || 0,
    conversation_duration_seconds: parseInt(obj.conversation_duration_seconds) || 0,
    duration_minutes: parseFloat(obj.duration_minutes) || 0,
    app_name: obj.app_name || "",
    from_circle: obj.from_circle || "",
    to_circle: obj.to_circle || "",
    disconnected_by: obj.disconnected_by || "",
    group_name: obj.group_name || "",
    recording_url: obj.recording_url || "",
    has_recording: !!(obj.recording_url),
    transcript_data: {
      transcript: obj.transcript || "",
    },
  };
}

module.exports = { fetchFromSheets };
