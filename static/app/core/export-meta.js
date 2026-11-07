let exportMetaPromise = null;

function normalizeIso(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  if (Number.isNaN(ts)) return null;
  return new Date(ts).toISOString();
}

async function loadExportMeta() {
  if (!exportMetaPromise) {
    exportMetaPromise = fetch("./data/export_meta.json", { cache: "no-store" })
      .then(async (response) => {
        if (!response.ok) return null;
        try {
          return await response.json();
        } catch (_) {
          return null;
        }
      })
      .catch(() => null);
  }
  return exportMetaPromise;
}

export async function getExportProfileGeneratedAt(profile) {
  const meta = await loadExportMeta();
  return normalizeIso(meta?.profiles?.[profile]?.generated_at || null);
}

export function invalidateExportMetaCache() {
  exportMetaPromise = null;
}
