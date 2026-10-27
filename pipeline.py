"""File-configured batch entry point; atomic publication and overlap protection."""
import csv
import json
import os
import shutil
import tempfile
from pathlib import Path
from .ingestion import load_pings, load_ports
from .transform import analyze

SCHEMAS = {
    'classified_pings': ['vessel_id','timestamp','latitude','longitude','current_draft_meters','speed_knots','port_id'],
    'port_visits': ['vessel_id','port_id','anchor_timestamp','berth_timestamp','delay_hours','entry_timestamp','entry_to_berth_hours','status'],
    'monthly_port_delays': ['port_id','calendar_month','berth_events','complete_visits','missing_anchor_visits','average_delay_hours','entry_proxy_visits','average_entry_to_berth_hours'],
    'draft_anomalies': ['vessel_id','timestamp','previous_timestamp','draft_change_meters','absolute_change_meters','elapsed_hours'],
    'top10_vessel_anomalies': ['rank','vessel_id','timestamp','previous_timestamp','draft_change_meters','absolute_change_meters','elapsed_hours']}


def run(config_path='config.json'):
    config = json.loads(Path(config_path).read_text())
    output = Path(config['output_directory'])
    output.mkdir(parents=True, exist_ok=True)
    lock = output / '.running'
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError('another run is active; inspect stale lock after a crash') from exc
    os.close(fd)
    stage = Path(tempfile.mkdtemp(prefix='.stage-', dir=output))
    try:
        ports = load_ports(Path(config['dataset_directory']) / 'port_geofences.csv')
        pings, rejected = load_pings(Path(config['dataset_directory']) / 'vessel_pings.csv.gz')
        if not pings:
            raise ValueError('no valid telemetry; previous published run retained')
        result = analyze(pings, ports, **config['thresholds'])
        for name, values in result.items():
            with (stage / f'{name}.csv').open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=SCHEMAS[name])
                writer.writeheader()
                writer.writerows(values)
        (stage/'quarantine.json').write_text(json.dumps(rejected, default=str, indent=2))
        metrics = {'accepted_rows':len(pings), 'rejected_rows':len(rejected), 'vessels':len({p['vessel_id'] for p in pings}), 'ports':len(ports), 'outputs':{k:len(v) for k,v in result.items()}, 'thresholds':config['thresholds']}
        (stage/'quality_report.json').write_text(json.dumps(metrics, indent=2))
        # Immutable run directory; CURRENT is replaced atomically only after all outputs succeed.
        final = output / stage.name.replace('.stage-', 'run-')
        stage.rename(final)
        pointer = output / '.CURRENT.tmp'
        pointer.write_text(final.name + '\n')
        os.replace(pointer, output/'CURRENT')
        return metrics
    finally:
        if stage.exists():
            shutil.rmtree(stage)
        lock.unlink(missing_ok=True)
