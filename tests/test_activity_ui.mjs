import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resourceSession } from "../docs/static/app/modules/energy/session.js";
import { isF1SessionLive } from "../docs/static/app/modules/f1/activity.js";

const data = JSON.parse(readFileSync(new URL("../sources/market_sessions.json", import.meta.url)));
const calendar = {calendars: data.calendars, expires_at: "2028-01-01T00:00:00Z"};
function check(key, time, expected) {
  assert.equal(resourceSession(key, calendar, Date.parse(time)).state, expected, `${key} ${time}`);
}
check("gold", "2026-11-24T18:00:00Z", "open");
check("gold", "2026-11-24T22:30:00Z", "break");
check("gold", "2026-07-08T21:30:00Z", "break"); // Chicago DST.
check("gold", "2026-11-28T18:00:00Z", "closed");
check("gold", "2026-11-26T18:00:00Z", "unknown"); // Futures holiday hours are not stock hours.
check("corn", "2026-11-24T14:00:00Z", "break");
check("corn", "2026-11-24T15:00:00Z", "open");
check("corn", "2026-11-24T20:00:00Z", "break");
check("corn", "2026-11-27T21:00:00Z", "closed");
check("china", "2026-11-24T01:30:00Z", "open");
check("china", "2026-11-24T02:20:00Z", "break");
check("china", "2026-11-24T04:00:00Z", "break");
check("china", "2026-11-24T13:30:00Z", "open");
check("china", "2026-11-27T16:30:00Z", "open"); // Friday night extends into Saturday.
check("china", "2026-11-22T16:30:00Z", "closed"); // No Sunday night session.
check("brent", "2026-11-24T12:00:00Z", "open");
check("brent", "2026-11-24T23:30:00Z", "break");
check("murban", "2026-11-24T12:00:00Z", "assessment");
assert.equal(resourceSession("gold", {}, Date.parse("2026-11-24T12:00:00Z")).state, "unknown");
const now = Date.parse("2026-11-24T12:00:00Z");
const live = {mode: "live", fetched_at: new Date(now - 60000).toISOString()};
assert.equal(isF1SessionLive(live, now), true);
for (const sample of [{...live, mode: "between_rounds"}, {...live, mode: "scheduled"}, {...live, error: "offline"},
  {...live, session: {status: "finished"}}, {...live, fetched_at: "2026-11-23T00:00:00Z"}, {mode: "live"}]) {
  assert.equal(isF1SessionLive(sample, now), false);
}
console.log("Resource sessions: weekly hours, breaks, DST, holiday uncertainty, SC night sessions passed.");
console.log("F1 activity: live-only, finished, stale and unavailable data passed.");
