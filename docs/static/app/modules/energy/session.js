// Regular futures hours only. Stock holidays are a conservative uncertainty guard,
// never treated as proof that a futures venue is closed. Prices are not used as clocks.
const metals = new Set(["gold", "silver", "copper", "wti", "natural_gas", "gasoline"]);
const grains = new Set(["corn", "soybean", "wheat"]);
const unknown = {state: "unknown", label: "时段待确认", detail: "特殊假期或日历覆盖不足，未推断交易状态"};
const state = (value, label, detail) => ({state: value, label, detail});

function localParts(now, timeZone) {
  const p = Object.fromEntries(new Intl.DateTimeFormat("en-CA", {
    timeZone, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hourCycle: "h23",
  }).formatToParts(new Date(now)).map(x => [x.type, x.value]));
  return {date: `${p.year}-${p.month}-${p.day}`, minute: Number(p.hour) * 60 + Number(p.minute)};
}
function addDays(date, days) {
  return new Date(Date.parse(date + "T12:00:00Z") + days * 86400000).toISOString().slice(0, 10);
}
const weekday = date => new Date(date + "T12:00:00Z").getUTCDay();
function calendar(payload, name, date, now) {
  const data = payload?.calendars?.[name];
  return data && Array.isArray(data.sessions) && Date.parse(payload.expires_at) > now
    && data.valid_from <= date && date <= data.valid_until ? data : null;
}
function usSpecialDay(payload, date, now) {
  const c = calendar(payload, "XNYS", date, now);
  const row = c?.sessions.find(x => x.date === date);
  return !row || Date.parse(row.close) - Date.parse(row.open) < 6.4 * 3600000
    || /-(?:12-24|12-31|01-01)$/.test(date);
}

function shanghaiCrude(payload, now) {
  const {date, minute} = localParts(now, "Asia/Shanghai");
  const previous = addDays(date, -1);
  const c = calendar(payload, "XSHG", date, now);
  if (!c) return unknown;
  const isDay = d => c.sessions.some(x => x.date === d);
  const nightBase = minute < 150 ? previous : date;
  if (minute >= 1260 || minute < 150) {
    if (!isDay(nightBase)) return state("closed", "休市", "非夜盘开市日");
    const next = c.sessions.find(x => x.date > nightBase);
    if (!next || (Date.parse(next.date) - Date.parse(nightBase)) / 86400000 > 3) return unknown;
    return state("open", "交易中", "上海原油常规夜盘21:00–02:30；以交易所临时安排为准");
  }
  if (!isDay(date)) return state("closed", "休市", "周末或交易所假日");
  if ((minute >= 540 && minute < 615) || (minute >= 630 && minute < 690) || (minute >= 810 && minute < 900)) {
    return state("open", "交易中", "上海原油常规日盘");
  }
  if ((minute >= 615 && minute < 630) || (minute >= 690 && minute < 810)) return state("break", "盘间休息", "上海原油日盘间休息");
  return state("closed", "休市", "常规日盘与夜盘之间");
}

export function resourceSession(key, payload, now = Date.now()) {
  if (key === "murban") return state("assessment", "现货评估", "Platts现货评估价，不按期货开休市着色");
  if (key === "china") return shanghaiCrude(payload, now);
  if (metals.has(key) || grains.has(key)) {
    const {date, minute} = localParts(now, "America/Chicago");
    const day = weekday(date), grain = grains.has(key);
    const evening = grain ? 1140 : 1020;
    const fridayEnd = grain ? 800 : 960;
    if (day === 6 || (day === 0 && minute < evening) || (day === 5 && minute >= fridayEnd)) return state("closed", "休市", "期货常规周末休市");
    const tradeDate = minute >= evening ? addDays(date, 1) : date;
    if (usSpecialDay(payload, tradeDate, now)) return unknown;
    const open = grain ? (minute < 465 || minute >= 1140 || (minute >= 510 && minute < 800))
      : (minute < 960 || minute >= 1020);
    return open ? state("open", "交易中", grain ? "CBOT常规时段：芝加哥19:00–07:45、08:30–13:20" : "CME常规时段：芝加哥17:00至次日16:00")
      : state("break", "盘间休息", grain ? "谷物合约盘间休息" : "CME每日维护休息时段");
  }
  if (key === "brent") {
    const {date, minute} = localParts(now, "Europe/London"), day = weekday(date);
    if (day === 6 || (day === 0 && minute < 1380) || (day === 5 && minute >= 1380)) return state("closed", "休市", "ICE布伦特常规周末休市");
    const tradeDate = day === 0 ? addDays(date, 1) : date;
    if (usSpecialDay(payload, tradeDate, now) || /-12-(?:2[4-8]|31)$|-01-0[12]$/.test(tradeDate)) return unknown;
    // Conservative guard for UK bank-holiday Mondays whose exchange hours may vary.
    const n = Number(date.slice(-2)), month = Number(date.slice(5, 7));
    if (day === 1 && ((month === 5 && (n <= 7 || n >= 25)) || (month === 8 && n >= 25))) return unknown;
    const open = day === 0 || (day === 1 && minute < 1380) || (minute >= 60 && minute < 1380);
    return open ? state("open", "交易中", "ICE布伦特常规时段；周日伦敦23:00开盘，其余交易日01:00–23:00")
      : state("break", "盘间休息", "ICE布伦特每日休息时段");
  }
  return unknown;
}
