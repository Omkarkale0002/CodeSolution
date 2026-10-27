import csv
import gzip
import json
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from logiwhale.ingestion import load_pings, load_ports, PING_FIELDS
from logiwhale.transform import analyze, port_for
from logiwhale.pipeline import run

PORTS = [dict(port_id='P', port_name='Port', lat_min=0,lat_max=1,lon_min=0,lon_max=1,daily_berth_cost=10)]

def ping(hour, inside=False, speed=10, draft=10, vessel='V'):
    return dict(vessel_id=vessel,timestamp=datetime(2026,5,31,22,tzinfo=timezone.utc)+timedelta(hours=hour),latitude=0.5 if inside else 2,longitude=0.5,current_draft_meters=draft,speed_knots=speed)

class AnalyticsTests(unittest.TestCase):
    def test_anchor_transit_berth_and_month(self):
        out=analyze([ping(0,speed=1),ping(1,speed=0),ping(2,True,6),ping(3,True,0),ping(4,True,0)], PORTS)
        self.assertEqual(len(out['port_visits']),1)
        self.assertEqual(out['monthly_port_delays'][0]['average_delay_hours'],3)
        self.assertEqual(out['monthly_port_delays'][0]['calendar_month'],'2026-06')
        self.assertEqual(out['port_visits'][0]['entry_to_berth_hours'],1)
        self.assertEqual(out['monthly_port_delays'][0]['average_entry_to_berth_hours'],1)
    def test_missing_anchor_and_empty(self):
        out=analyze([ping(0,True,0)], PORTS)
        self.assertIsNone(out['monthly_port_delays'][0]['average_delay_hours'])
        self.assertEqual(analyze([],PORTS)['port_visits'],[])
    def test_gap_resets_state(self):
        self.assertIsNone(analyze([ping(0,speed=0),ping(8,True,0)],PORTS)['port_visits'][0]['delay_hours'])
    def test_resume_sailing_cancels_anchor(self):
        out=analyze([ping(0,speed=0),ping(1),ping(2,True,0)],PORTS)
        self.assertEqual(out['port_visits'][0]['status'],'missing_anchor')
    def test_repeat_visit(self):
        out=analyze([ping(0,True,0),ping(1,speed=0),ping(2,True,0)],PORTS)
        self.assertEqual(len(out['port_visits']),2)
    def test_boundaries_and_overlap(self):
        self.assertEqual(port_for(dict(latitude=0,longitude=1),PORTS),'P')
        with self.assertRaises(ValueError): port_for(dict(latitude=0,longitude=1),PORTS*2)
    def test_anomalies_both_outside_gap_and_unique_ranking(self):
        out=analyze([ping(0),ping(1,draft=13),ping(2,draft=8),ping(3,True,draft=20),ping(4,draft=10),ping(12,draft=20)],PORTS)
        self.assertEqual(len(out['draft_anomalies']),2)
        self.assertEqual(len(out['top10_vessel_anomalies']),1)
        self.assertEqual(out['top10_vessel_anomalies'][0]['absolute_change_meters'],5)
    def test_vessels_do_not_share_state(self):
        out=analyze([ping(0,speed=0),ping(1,True,0,vessel='W')],PORTS)
        self.assertIsNone(out['port_visits'][0]['delay_hours'])
    def test_top10_cap(self):
        rows=[]
        for i in range(12): rows.extend([ping(0,vessel=str(i)),ping(1,draft=13,vessel=str(i))])
        self.assertEqual(len(analyze(rows,PORTS)['top10_vessel_anomalies']),10)

class InputAndRunTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
    def tearDown(self): self.temp.cleanup()
    def write_pings(self, values):
        path=self.root/'vessel_pings.csv.gz'
        with gzip.open(path,'wt',newline='') as f:
            w=csv.DictWriter(f,fieldnames=PING_FIELDS);w.writeheader()
            for p in values:
                w.writerow(dict(p,timestamp=p['timestamp'].isoformat() if isinstance(p['timestamp'],datetime) else p['timestamp']))
        return path
    def test_sort_duplicates_invalid_and_conflicts(self):
        p=ping(0); q=ping(1)
        path=self.write_pings([q,p,p,dict(ping(2),longitude=''),dict(ping(3),timestamp='bad'),dict(ping(4),speed_knots='nan'),dict(ping(5),latitude=100),dict(q,speed_knots=4)])
        clean,bad=load_pings(path)
        self.assertEqual(len(clean),1);self.assertEqual(len(bad),7)
    def test_bad_reference(self):
        f=self.root/'ports.csv';f.write_text('bad\n')
        with self.assertRaises(ValueError):load_ports(f)
    def test_rerun_failure_retains_pointer_and_lock(self):
        self.write_pings([ping(0,speed=0),ping(1,True,0)])
        (self.root/'port_geofences.csv').write_text('port_id,port_name,lat_min,lat_max,lon_min,lon_max,daily_berth_cost\nP,Port,0,1,0,1,10\n')
        out=self.root/'output';cfg=self.root/'config.json'
        cfg.write_text(json.dumps(dict(dataset_directory=str(self.root),output_directory=str(out),thresholds={})))
        first=run(cfg); pointer=(out/'CURRENT').read_text(); second=run(cfg)
        self.assertEqual(first,second);self.assertNotEqual(pointer,(out/'CURRENT').read_text())
        pointer=(out/'CURRENT').read_text();self.write_pings([])
        with self.assertRaises(ValueError):run(cfg)
        self.assertEqual(pointer,(out/'CURRENT').read_text())
        (out/'.running').touch()
        with self.assertRaises(RuntimeError):run(cfg)
