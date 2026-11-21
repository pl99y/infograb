import { formatAbsoluteLocalDateTime, formatRelativeLocalTime } from "./time.js?v=46";

let datasets = {};

export async function refreshExportMeta() {
  try {
    const response = await fetch("./data/export_meta.json", {
      cache: "no-store",
      signal: AbortSignal.timeout(8000),
    });
    if (!response.ok) return;
    const meta = await response.json();
    datasets = Object.assign({}, ...Object.values(meta?.profiles || {}).map(profile => profile.datasets));
  } catch {
    // Keep the last metadata received when the connection is temporarily unavailable.
  }
}

const STATUS_LABELS = {
  success: "已更新",
  partial: "部分更新",
  failed: "采集失败",
  skipped: "采集跳过",
  unverified: "等待首次确认",
};

export function getFreshness(filenames, fallbackAt) {
  const rows = filenames.map(name => datasets[name]);
  const statuses = rows.map(row => row?.status || "unverified");
  const successTimes = rows.map(row => row?.last_success_at).filter(Boolean).sort();
  const lastSuccess = successTimes.length === rows.length ? successTimes[0] : null;
  const status = statuses.every(value => value === statuses[0])
    ? statuses[0]
    : statuses.some(value => value === "failed" || value === "partial") ? "partial" : "unverified";
  const label = STATUS_LABELS[status] || STATUS_LABELS.unverified;
  const age = lastSuccess
    ? (status === "success" ? "" : " · 上次成功 ") + formatRelativeLocalTime(lastSuccess)
    : fallbackAt ? " · 快照 " + formatRelativeLocalTime(fallbackAt) : "";
  const title = rows.some(Boolean)
    ? filenames.map((name, index) => {
        const row = rows[index];
        const statusLabel = STATUS_LABELS[statuses[index]] || STATUS_LABELS.unverified;
        const attempt = formatAbsoluteLocalDateTime(row?.last_attempt_at);
        const success = formatAbsoluteLocalDateTime(row?.last_success_at);
        return name + " · " + statusLabel
          + (attempt ? " · 尝试 " + attempt : "") + (success ? " · 成功 " + success : "");
      }).join("\n")
    : formatAbsoluteLocalDateTime(fallbackAt);

  return { text: label + (status === "success" && age ? " " : "") + age, title, failed: status === "failed" };
}

export function renderFreshness(element, filenames, fallbackAt) {
  if (!element) return;
  const freshness = getFreshness(filenames, fallbackAt);
  element.textContent = "（" + freshness.text + "）";
  element.title = freshness.title;
}
