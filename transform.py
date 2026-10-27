"""Deterministic state transitions and transparent analytical definitions."""
from collections import defaultdict


def port_for(ping, ports):
    matches = [p['port_id'] for p in ports if p['lat_min'] <= ping['latitude'] <= p['lat_max'] and p['lon_min'] <= ping['longitude'] <= p['lon_max']]
    if len(matches) > 1:
        raise ValueError('overlapping geofences match telemetry; resolve reference data')
    return matches[0] if matches else None


def analyze(pings, ports, anchor_speed=2.0, berth_speed=0.5, max_gap_hours=6.0, draft_jump=2.0):
    visits, anomalies, enriched = [], [], []
    previous = anchor = active_port = None
    berthed = False
    entry = None
    for ping in pings:
        p = dict(ping, port_id=port_for(ping, ports))
        enriched.append(p)
        same_vessel = previous is not None and previous['vessel_id'] == p['vessel_id']
        gap = (p['timestamp'] - previous['timestamp']).total_seconds() / 3600 if same_vessel else None
        continuous = same_vessel and 0 < gap <= max_gap_hours
        if not continuous:
            anchor, active_port, berthed, entry = None, None, False, None
        if p['port_id'] is None:
            active_port, berthed, entry = None, False, None
            if p['speed_knots'] <= anchor_speed:
                anchor = anchor or p['timestamp']
            else:
                anchor = None
            if continuous and previous['port_id'] is None:
                delta = p['current_draft_meters'] - previous['current_draft_meters']
                if abs(delta) >= draft_jump:
                    anomalies.append({'vessel_id': p['vessel_id'], 'timestamp': p['timestamp'], 'previous_timestamp': previous['timestamp'], 'draft_change_meters': delta, 'absolute_change_meters': abs(delta), 'elapsed_hours': gap})
        else:
            if active_port != p['port_id']:
                # Preserve preceding offshore anchor only on direct entry from outside.
                if previous is None or previous['port_id'] is not None:
                    anchor = None
                active_port, berthed, entry = p['port_id'], False, p['timestamp'] if continuous and previous['port_id'] is None else None
            if not berthed and p['speed_knots'] <= berth_speed:
                visits.append({'vessel_id': p['vessel_id'], 'port_id': active_port, 'anchor_timestamp': anchor, 'berth_timestamp': p['timestamp'], 'delay_hours': (p['timestamp'] - anchor).total_seconds() / 3600 if anchor else None, 'entry_timestamp': entry, 'entry_to_berth_hours': (p['timestamp'] - entry).total_seconds() / 3600 if entry else None, 'status': 'complete' if anchor else 'missing_anchor'})
                berthed = True
                anchor = None
        previous = p
    grouped = defaultdict(list)
    for v in visits:
        grouped[v['port_id'], v['berth_timestamp'].strftime('%Y-%m')].append(v)
    monthly = []
    for (port, month), values in sorted(grouped.items()):
        delays = [v['delay_hours'] for v in values if v['delay_hours'] is not None]
        entry_delays = [v['entry_to_berth_hours'] for v in values if v['entry_to_berth_hours'] is not None]
        monthly.append({'entry_proxy_visits': len(entry_delays), 'average_entry_to_berth_hours': sum(entry_delays)/len(entry_delays) if entry_delays else None, 'port_id': port, 'calendar_month': month, 'berth_events': len(values), 'complete_visits': len(delays), 'missing_anchor_visits': len(values)-len(delays), 'average_delay_hours': sum(delays)/len(delays) if delays else None})
    # Rank distinct vessels by their strongest eligible jump, with stable ties.
    best = {}
    for a in sorted(anomalies, key=lambda a: (-a['absolute_change_meters'], a['vessel_id'], a['timestamp'])):
        best.setdefault(a['vessel_id'], a)
    top = [dict(rank=i, **a) for i, a in enumerate(list(best.values())[:10], 1)]
    return {'classified_pings': enriched, 'port_visits': visits, 'monthly_port_delays': monthly, 'draft_anomalies': anomalies, 'top10_vessel_anomalies': top}
