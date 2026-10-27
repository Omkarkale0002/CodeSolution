"""Strict ingestion: quarantine bad telemetry, fail on invalid reference data."""
import csv
import gzip
import math
from datetime import datetime

PING_FIELDS = ['vessel_id', 'timestamp', 'latitude', 'longitude', 'current_draft_meters', 'speed_knots']
PORT_FIELDS = ['port_id', 'port_name', 'lat_min', 'lat_max', 'lon_min', 'lon_max', 'daily_berth_cost']


def rows(path, fields):
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt', newline='', encoding='utf-8-sig') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != fields:
            raise ValueError(f'{path}: expected columns {fields}, got {reader.fieldnames}')
        yield from enumerate(reader, 2)


def number(value, low, high):
    value = float(value)
    if not math.isfinite(value) or not low <= value <= high:
        raise ValueError(f'number outside [{low}, {high}]')
    return value


def load_ports(path):
    ports = []
    for line, raw in rows(path, PORT_FIELDS):
        try:
            p = dict(raw)
            if not p['port_id'] or not p['port_name'] or any(x['port_id'] == p['port_id'] for x in ports):
                raise ValueError('empty or duplicate port identity')
            for name in ['lat_min', 'lat_max', 'lon_min', 'lon_max']:
                p[name] = number(p[name], -90 if name.startswith('lat') else -180, 90 if name.startswith('lat') else 180)
            if p['lat_min'] > p['lat_max'] or p['lon_min'] > p['lon_max']:
                raise ValueError('reversed boundary')
            p['daily_berth_cost'] = int(p['daily_berth_cost'])
            if p['daily_berth_cost'] < 0:
                raise ValueError('negative berth cost')
            ports.append(p)
        except (ValueError, TypeError) as exc:
            raise ValueError(f'invalid port at line {line}: {exc}') from exc
    if not ports:
        raise ValueError('empty port reference')
    return sorted(ports, key=lambda p: p['port_id'])


def load_pings(path):
    valid, rejected, seen = [], [], set()
    for line, raw in rows(path, PING_FIELDS):
        try:
            p = dict(raw)
            if not p['vessel_id'] or not p['vessel_id'].strip():
                raise ValueError('missing vessel_id')
            p['timestamp'] = datetime.fromisoformat(p['timestamp'].replace('Z', '+00:00'))
            if p['timestamp'].utcoffset() is None or p['timestamp'].utcoffset().total_seconds() != 0:
                raise ValueError('timestamp must be UTC')
            for field, low, high in [('latitude', -90, 90), ('longitude', -180, 180), ('current_draft_meters', 0, 100), ('speed_knots', 0, 100)]:
                p[field] = number(p[field], low, high)
            key = tuple(p[f] for f in PING_FIELDS)
            if key in seen:
                raise ValueError('exact_duplicate')
            seen.add(key)
            valid.append(p)
        except (ValueError, TypeError, AttributeError) as exc:
            rejected.append({'source_line': line, 'reason': str(exc), 'raw': raw})
    # Ambiguous timestamps cannot safely define event ordering: quarantine all conflicts.
    from collections import Counter
    counts = Counter((p['vessel_id'], p['timestamp']) for p in valid)
    clean = []
    for p in valid:
        if counts[p['vessel_id'], p['timestamp']] > 1:
            rejected.append({'source_line': None, 'reason': 'conflicting_vessel_timestamp', 'raw': p})
        else:
            clean.append(p)
    return sorted(clean, key=lambda p: (p['vessel_id'], p['timestamp'])), rejected
