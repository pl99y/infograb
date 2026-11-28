// Match named locations, not an issuing agency or an incidental word in a summary.
const chinaCodes = new Set(["CN", "CHN", "HK", "HKG", "MO", "MAC", "TW", "TWN"]);
const chinaLocationEn = /\b(?:china(?![\s-]+sea\b)|hong[\s-]*kong|maca[ou]|taiwan(?![\s-]+strait\b)|beijing|tianjin|shanghai|chongqing|hebei|shanxi|liaoning|jilin|heilongjiang|jiangsu|zhejiang|anhui|fujian|jiangxi|shandong|henan|hubei|hunan|guangdong|hainan|sichuan|guizhou|yunnan|shaanxi|gansu|qinghai|inner[\s-]+mongolia|guangxi|tibet|xizang|ningxia|xinjiang)\b/i;
const chinaLocationZh = /中国(?!沿?海域|海|南海|东海)|香港|澳[门門]|[台臺][湾灣](?!海[峡峽])|北京|天津|上海|重[庆慶]|河北|山西|[辽遼][宁寧]|吉林|黑[龙龍]江|江[苏蘇]|浙江|安徽|福建|江西|山[东東]|河南|湖北|湖南|广[东東]|廣東|海南|四川|[贵貴]州|云南|雲南|[陕陝]西|甘[肃肅]|青海|[内內]蒙古|[广廣]西|西藏|[宁寧]夏|新疆/;

export function isChinaCode(code) {
  return chinaCodes.has(String(code ?? "").trim().toUpperCase());
}

export function mentionsChina(location) {
  return typeof location === "string" && (chinaLocationEn.test(location) || chinaLocationZh.test(location));
}

export function chinaFirst(items, matchesChina) {
  const china = [];
  const other = [];
  for (const item of items) {
    (matchesChina(item) ? china : other).push(item);
  }
  return china.concat(other);
}
