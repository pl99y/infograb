export function createApi() {
  async function fetchJson(path) {
    const response = await fetch(path, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${path}`);
    }

    const data = await response.json();
    return {
      data,
      meta: {
        lastModified: response.headers.get("last-modified") || "",
        etag: response.headers.get("etag") || "",
      },
    };
  }

  function mapGet(url) {
    const clean = String(url || "");
    if (clean.startsWith("/api/energy/latest")) return "./data/energy_latest.json";
    if (clean.startsWith("/api/market-snapshots/latest")) return "./data/market_latest.json";
    if (clean.startsWith("/api/aviation/alerts")) return "./data/aviation_alerts.json";
    if (clean.startsWith("/api/aviation/disruptions")) return "./data/aviation_disruptions.json";
    if (clean.startsWith("/api/weather/alerts")) return "./data/weather_alerts.json";
    if (clean.startsWith("/api/disaster/ongoing")) return "./data/disaster_ongoing.json";
    if (clean.startsWith("/api/public-health/latest")) return "./data/public_health_latest.json";
    if (clean.startsWith("/api/space-weather/launches")) return "./data/space_launches_latest.json";
    if (clean.startsWith("/api/space-weather/alerts")) return "./data/space_weather_alerts.json";
    if (clean.startsWith("/api/space-weather/forecast")) return "./data/space_weather_forecast.json";
    if (clean.startsWith("/api/mnd-pla/dashboard")) return "./data/mnd_pla_dashboard.json";
    if (clean.startsWith("/api/f1/live")) return "./data/f1_live.json";
    if (clean.startsWith("/api/f1/news")) return "./data/f1_news.json";
    if (clean.startsWith("/api/telegram")) return "./data/telegram.json";
    if (clean.startsWith("/api/news-timeline/latest")) return "./data/news_timeline_latest.json";
    if (clean.startsWith("/api/news-hotsearch/latest")) return "./data/news_hotsearch_latest.json";
    throw new Error(`Static Pages API mapping missing for GET ${clean}`);
  }

  return {
    async get(url) {
      const { data } = await fetchJson(mapGet(url));
      return data;
    },
    async getWithMeta(url) {
      return fetchJson(mapGet(url));
    },
  };
}
