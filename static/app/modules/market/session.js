export function marketSession(key, payload, now = Date.now()) {
  const unknown = {state: "unknown", label: "状态待确认", detail: "交易日历不可用或已超出有效范围"};
  const calendar = payload?.calendars?.[payload?.mapping?.[key]];
  if (!calendar || !Array.isArray(calendar.sessions) || !Number.isFinite(Date.parse(payload.expires_at)) || Date.parse(payload.expires_at) <= now) return unknown;
  const fields = Object.fromEntries(new Intl.DateTimeFormat("en-CA", {
    timeZone: calendar.timezone, year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(new Date(now)).map(part => [part.type, part.value]));
  const date = `${fields.year}-${fields.month}-${fields.day}`;
  if (date < calendar.valid_from || date > calendar.valid_until) return unknown;
  const row = calendar.sessions.find(item => item.date === date);
  const next = calendar.sessions.find(item => Date.parse(item.open) > now);
  const nextText = next ? `；下次开市 ${new Date(next.open).toLocaleString()}` : "";
  if (!row) return {state: "closed", label: "休市", detail: `周末或交易所假日${nextText}`};
  if (now < Date.parse(row.open) || now >= Date.parse(row.close)) {
    return {state: "closed", label: "休市", detail: `常规交易时段外${nextText}`};
  }
  if (row.break_start && now >= Date.parse(row.break_start) && now < Date.parse(row.break_end)) {
    return {state: "break", label: "午间休市", detail: `午休；恢复交易 ${new Date(row.break_end).toLocaleTimeString()}`};
  }
  if (row.auction_start && now >= Date.parse(row.auction_start)) {
    return {state: "auction", label: "收市竞价", detail: "按交易所计划时段，实际竞价可能提前结束"};
  }
  return {state: "open", label: "交易中", detail: "按交易日历的常规时段；报价可能延迟，临时停市需以交易所公告为准"};
}
