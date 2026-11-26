A static information dashboard powered by GitHub Actions and GitHub Pages.

It collects data from multiple sources, processes and translates part of the content, then exports JSON files to `docs/data/` for the frontend to render.

## Stack

- Python
- GitHub Actions
- GitHub Pages
- Gemini API

## Structure

```text
collectors/   data fetching
parsers/      parsing logic
services/     shared processing utilities
scripts/      export scripts
docs/         static frontend + exported JSON

A low-cost imitation of a well-known project in this space
— partly born out of frustration with how commercial it has become.


## 行情与顶部焦点

- 指数状态采用 `sources/market_sessions.json` 的交易所日历，前端每30秒复核，独立于报价是否更新。状态是计划时段，临时停市仍以交易所公告为准。
- 日历由 `scripts/build_market_calendar.py` 生成；生成环境需要 Python >=3.10 和 exchange-calendars==4.13.3，生产环境仅读取 JSON。当前上证覆盖至2026年底，其余三市场覆盖至2027年底；超期显示状态待确认，年度假期公布后应重建日历。
- 大宗商品为黄金、白银、铜、玉米、大豆、小麦的连续期货报价。金银单位为美元/金衡盎司，铜为美元/磅，谷物为美分/蒲式耳。检测到合约变更的首轮不显示跨合约涨跌。
- 当前关注与热搜摘要共用一次3.1 Flash-Lite请求；无效模型编号或新增数字会退回来源标题。AI明确返回无合适要事时留空；接口失败才按候选优先级回退，普通国际地方新闻不自动补位。只存一份当前结果，45分钟后停止显示。
- 当前关注候选来自原有各板块，加上 BBC World、UN News、Al Jazeera、France 24 四个固定RSS（不是模型自行联网搜索）。RSS串行抓取，各最多12条，不归档正文；国际新闻优先战争外交、能源运输中断、重大灾害、公共卫生及广泛经济影响，娱乐体育评论降权。合并后总候选最多40条，其中新闻最多16条、同一新闻来源最多4条，并保留其他板块代表。不同媒体的同一事件由现有AI请求合并考虑，明确转载按原通讯社计证据，保留跨来源报道供核验。
- 顶栏警报默认后台试判（`INFOGRAB_ALERT_COLORS=0`）。当前判断及最近48次变化可在 `current_focus_v1` 缓存核验，记录最多保留两天。确认试判质量后才设置为1启用变色；严重级别需要官方及独立新闻来源，Telegram不能单独触发。

- 资源行情的交易状态在浏览器按品种常规时段计算，每30秒复核。金属/美国能源、谷物、ICE布伦特、上海原油分别处理跨夜与休息时段；美国股市假日/半日市仅作期货特殊时段的保守待确认提示，不直接当成期货休市。现货评估价不着交易色。
- 市场/资源交易中用淡蓝底，盘间休息用淡琥珀底，休市用淡灰底；这与红绿涨跌分开。F1仅新鲜LIVE数据启用淡紫底，45分钟以上的旧LIVE不会保留高亮。
