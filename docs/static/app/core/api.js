export function createApi() {
  async function fetchJson(path) {
    const response = await fetch(path, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${path}`);
    }
    return response.json();
  }

  function mapGet(url) {
    const clean = String(url || "");
    if (clean.startsWith("/api/energy/latest")) return "./data/energy_latest.json";
    if (clean.startsWith("/api/market-snapshots/latest")) return "./data/market_latest.json";
    if (clean.startsWith("/api/iflow/latest")) return "./data/iflow_latest.json";
    if (clean.startsWith("/api/aviation/alerts")) return "./data/aviation_alerts.json";
    if (clean.startsWith("/api/aviation/disruptions")) return "./data/aviation_disruptions.json";
    if (clean.startsWith("/api/weather/alerts")) return "./data/weather_alerts.json";
    if (clean.startsWith("/api/disaster/ongoing")) return "./data/disaster_ongoing.json";
    if (clean.startsWith("/api/f1/live")) return "./data/f1_live.json";
    if (clean.startsWith("/api/f1/news")) return "./data/f1_news.json";
    if (clean.startsWith("/api/telegram")) return "./data/telegram.json";
    throw new Error(`Static Pages API mapping missing for GET ${clean}`);
  }

  return {
    async get(url) {
      return fetchJson(mapGet(url));
    },
    async post(url) {
      throw new Error(`POST not available on static Pages: ${url}`);
    },
  };
}
