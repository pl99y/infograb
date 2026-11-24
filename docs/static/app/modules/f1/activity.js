export function isF1SessionLive(payload, now = Date.now()) {
  if (!payload || payload.error || String(payload.mode).toLowerCase() !== "live") return false;
  const status = String(payload.session?.status || payload.session?.status_text || payload.session_status || "");
  if (/finished|completed|cancelled|canceled|ended|not.started/i.test(status)) return false;
  const updated = [payload.updated_at, payload.fetched_at, payload.generated_at, payload.session?.updated_at]
    .map(value => Date.parse(value)).filter(Number.isFinite);
  if (!updated.length) return false;
  const age = now - Math.max(...updated);
  return age >= -300000 && age <= 45 * 60000;
}
