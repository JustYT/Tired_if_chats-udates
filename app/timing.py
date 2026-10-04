"""Approximate wall-clock ETA from measured work, without message content."""
import copy
import time
from datetime import datetime
from statistics import median


class RunTiming:
    def __init__(self, prior=None, clock=time.time):
        self.clock = clock
        self.changed = lambda: None
        self.data = {'prior': prior or {}, 'durations': {}, 'phase': 'collecting',
                     'done': 0, 'total': 0, 'later_units': 0, 'unit_started_at': clock()}

    def stage(self, phase, total, later_units=0):
        self.data.update(phase=phase, done=0, total=total, later_units=later_units,
                         unit_started_at=self.clock())
        self.changed()

    def add_units(self, number):
        self.data['total'] += number
        self.changed()

    def advance(self):
        at = self.clock()
        d = self.data
        # Collection is parallel: its throughput is not a model-call duration.
        if d['phase'] != 'collecting':
            d['durations'].setdefault(d['phase'], []).append(max(.001, at-d['unit_started_at']))
        d['done'] += 1
        d['unit_started_at'] = at
        self.changed()

    def finish(self):
        self.data['finished_at'] = self.clock()

    def snapshot(self):
        return copy.deepcopy(self.data)


def estimate(job, at=None):
    at = time.time() if at is None else at
    active = job['status'] in ('queued','collecting','summarizing','sending')
    timing = job.get('stats', {}).get('timing', {})
    started = datetime.fromisoformat(job['created']).timestamp()
    ended = timing.get('finished_at') or datetime.fromisoformat(job['updated']).timestamp()
    result = {'as_of': at, 'elapsed_seconds': max(0, (at if active else ended)-started),
              'active': active, 'remaining_low': None, 'remaining_high': None,
              'phase': timing.get('phase'), 'done': timing.get('done', 0),
              'total': timing.get('total', 0), 'delayed': False}
    if not active or not timing or timing['phase']=='collecting': return result
    phase = timing['phase']
    durations, prior = timing.get('durations', {}), timing.get('prior', {})
    def typical(name):
        values = durations.get(name) or prior.get(name) or []
        return median(values) if values else None
    pace = typical(phase)
    if pace is None and phase=='merging': pace = typical('extracting')
    if pace is None: return result
    current = max(0, at-timing['unit_started_at'])
    units = max(0, timing['total']-timing['done'])
    remaining = max(0, pace*units-current)
    if units and current>pace*1.8:
        result['delayed'] = True
        return result  # Do not leave a misleading zero-minute countdown.
    later = timing.get('later_units', 0)
    if phase=='extracting': remaining += later*(typical('merging') or pace)
    if phase in ('extracting','merging'): remaining += typical('sending') or 15
    result.update(remaining_low=max(5, remaining*.6), remaining_high=max(15, remaining*1.8))
    return result
