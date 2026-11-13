import unittest, tempfile, json, sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from parsers.disaster_parser import _parse_wmo_items
from parsers.space_weather_parser import parse_swpc_forecast_payloads, SpaceWeatherParseError
from parsers.mnd_pla_parser import extract_intrusion_info, summarize_mnd_pla_records
from parsers.public_health_parser import parse_promed_latest_html
from collectors.news_timeline_fetcher import parse_zaobao_article
from services.telegram.ingest import clean_telegram_text
from collectors.public_health_fetcher import fetch_public_health_payloads, PublicHealthFetcher
from services.f1.rounds import resolve_round_strategy
from scripts.export_status import hotsearch_status
from parsers.energy_parser import parse_oilprice_last_json_quote, EnergyParseError
from services.aviation.pipeline import _mark_inactive_adsbfi_alerts, ADSBFI_SOURCE_NAME
from parsers.aviation_parser import parse_adsbfi_squawk_alerts, AviationParseError
from storage import initialize_database, get_connection

class Regressions(unittest.TestCase):
 def test_wmo_active_and_expired(self):
  now=datetime.now(timezone.utc)
  def row(i,age,expiry):return dict(id=i,s=4,headline='Heat',sent=(now-timedelta(hours=age)).isoformat(),expires=(now+timedelta(hours=expiry)).isoformat())
  raw={'wmo_all':{'ok':True,'data':{'items':[row('active',30,5),row('expired',3,-1)]}}}
  result=_parse_wmo_items(raw,now.isoformat(),window_hours=12)
  self.assertEqual([r['dedupe_key'] for r in result],['active'])
 def test_aviation_failed_source_not_resolved(self):
  with tempfile.TemporaryDirectory() as d:
   db=str(Path(d)/'test.db');initialize_database(db);con=get_connection(db)
   now=datetime.now(timezone.utc).isoformat()
   for sq in ['7700','7600']:
    con.execute('INSERT INTO airport_alerts (dedupe_key,callsign,squawk_code,source_name,source_url,fetched_at,extra_json) VALUES (?,?,?,?,?,?,?)',(sq,'TEST',sq,ADSBFI_SOURCE_NAME,'https://example.com',now,json.dumps({'is_active':True})))
   _mark_inactive_adsbfi_alerts(con,active_dedupe_keys=set(),checked_at=now,successful_squawks={'7600'})
   rows={r['squawk_code']:json.loads(r['extra_json']) for r in con.execute('SELECT * FROM airport_alerts')}
   self.assertTrue(rows['7700']['is_active']);self.assertFalse(rows['7600']['is_active']);con.close()
 def test_aviation_bad_shape(self):
  with self.assertRaises(AviationParseError):parse_adsbfi_squawk_alerts({},'https://example.com',squawk='7700',fetched_at='2026-11-13T00:00:00Z')
 def test_space(self):
  forecast='''A. NOAA Geomagnetic Activity Observation and Forecast
The greatest observed 3 hr Kp over the past 24 hours was 5.67.
The greatest expected 3 hr Kp for Nov 13 is 5.67 (G1).
B. NOAA Solar Radiation Activity Observation and Forecast
Solar radiation, as observed by NOAA GOES-19 over the past 24 hours, was low.
S1 or greater  1%  1%  1%
C. NOAA Radio Blackout Activity and Forecast
R1-R2  10%  10%  10%
'''
  result=parse_swpc_forecast_payloads(forecast,'Active 10/20/30')
  self.assertIn('5.67 (G1).',result['geomagnetic']['expected_summary'])
  self.assertIn('GOES-19',result['solar_radiation']['observed_summary'])
  with self.assertRaises(SpaceWeatherParseError):parse_swpc_forecast_payloads('<html>error</html>','error')
 def test_mnd_unknown(self):
  count,areas=extract_intrusion_info('偵獲共機4架次，其中3架次進入北部空域。')
  self.assertEqual(count,3);self.assertTrue(areas)
  self.assertIsNone(summarize_mnd_pla_records([{'published_date':'2026-11-13'}])['aircraft_total_sum'])
 def test_promed_link(self):
  rows=parse_promed_latest_html('<table><tbody><tr><td>Fri Nov 13 2026</td><td><a href="/post/123">Outbreak</a></td></tr></tbody></table>')
  self.assertEqual(rows[0]['item_url'],'https://www.promedmail.org/post/123')
 def test_health_isolation(self):
  with patch.object(PublicHealthFetcher,'fetch_text',side_effect=RuntimeError('offline')),patch.object(PublicHealthFetcher,'fetch_json',return_value={'value':[{}]}) as fetch:
   result=fetch_public_health_payloads();fetch.assert_called_once();self.assertIn('promed',result['errors']);self.assertNotIn('who',result['errors'])
 def test_news_dates(self):
  for body in ['<script type="application/ld+json">{"@graph":[{"datePublished":"2026-11-13T09:00:00+08:00"}]}</script>','<time datetime="2026-11-13T09:00:00+08:00"></time>']:
   self.assertEqual(parse_zaobao_article('<h1>News</h1>'+body)[1],'2026-11-13T01:00:00+00:00')
 def test_hotsearch_digest(self):
  self.assertEqual(hotsearch_status({'sources':[{'ok':True,'items_count':20}], 'ai_digest':{'ok':False}}),'partial')
 def test_f1_invalid_schedule(self):
  with patch('services.f1.rounds._fetch_text',return_value='<html>template changed</html>'):
   with self.assertRaises(ValueError):resolve_round_strategy(year=2026)
 def test_telegram_link(self):
  self.assertIn('https://t.me/channel/12345',clean_telegram_text('Source https://t.me/channel/12345'))
 def test_murban(self):
  meta={'4863':{'blend_name':'Murban Crude','source':'Platts','app_label':'Physical','last_price':[{'price':'93.47','dateadded':'1794463200'}]}}
  quote=parse_oilprice_last_json_quote(quote_key='murban',name='Murban',json_text='{}',source_url='https://oilprice.com',metadata_json_text=json.dumps(meta))
  self.assertEqual(quote.price,93.47);self.assertTrue(quote.timestamp_text)
  with self.assertRaises(EnergyParseError):parse_oilprice_last_json_quote(quote_key='murban',name='Murban',json_text='{}',source_url='https://oilprice.com',metadata_json_text='{}')
 def test_telegram_source_coverage(self):
  from services.telegram.queries import get_recent_posts
  with tempfile.TemporaryDirectory() as d:
   db=str(Path(d)/'test.db');initialize_database(db);con=get_connection(db)
   try:
    for source_id in range(1,8):
     con.execute("INSERT INTO sources(id,name,source_type,url) VALUES (?,?,?,?)",(source_id,str(source_id),'telegram',f'https://t.me/source{source_id}'))
     for n in range(60 if source_id==1 else 3):
      stamp=f'2026-11-{13 if source_id==1 else 12}T00:00:00+00:00'
      con.execute("INSERT INTO items(source_id,post_url,published_at,fetched_at) VALUES (?,?,?,?)",(source_id,f'https://t.me/source{source_id}/{n}',stamp,stamp))
    con.commit()
   finally:con.close()
   rows=get_recent_posts(db_path=db)
   self.assertEqual(len(rows),50);self.assertEqual(len({r['source_name'] for r in rows}),7)
 def test_market_percent_uses_daily_change(self):
  from collectors.market_snapshot_fetcher import YahooMarketSnapshotAdapter, MarketSnapshotSpec
  from unittest.mock import Mock
  response=Mock();response.json.return_value={'chart':{'result':[{'meta':{'regularMarketPrice':3634.353,'chartPreviousClose':3680.64,'regularMarketChangePercent':-0.587}}]}}
  session=Mock();session.get.return_value=response
  item=YahooMarketSnapshotAdapter(session=session).fetch_one(MarketSnapshotSpec('sse','SSE','000001.SS','yahoo_finance'))
  self.assertEqual(item['change_percent'],-0.59)
 def test_mnd_query_url(self):
  from parsers.mnd_pla_parser import parse_mnd_pla_list_page
  rows=parse_mnd_pla_list_page('<a href="/news/plaact/123/?from=list">115.11.13 中共解放軍臺海周邊海、空域動態</a>', list_url='https://www.mnd.gov.tw/news/plaact')
  self.assertEqual(len(rows),1);self.assertTrue(rows[0].detail_url.endswith('/123'))
if __name__=='__main__':unittest.main()

