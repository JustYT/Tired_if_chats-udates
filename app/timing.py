"""Approximate wall-clock ETA from measured work, without message content."""
import copy
import time
from datetime import datetime
from statistics import median

TIMING_VERSION = 2


class RunTiming:
    def __init__(self, prior=None, clock=time.time):
        self.clock = clock
        self.changed = lambda: None
        self.data = {'version': TIMING_VERSION, 'prior': prior or {}, 'durations': {}, 'phase': 'collecting',
                     'done': 0, 'total': 0, 'later_units': 0, 'unit_started_at': clock(),
                     'phase_started_at': clock(), 'phase_seconds': {}, 'active_units': {},
                     'concurrency': 1, 'task_mode': False, 'requests': [], 'plan': {}}

    def _close_phase(self):
        phase = self.data['phase']
        seconds = max(0, self.clock()-self.data['phase_started_at'])
        self.data['phase_seconds'][phase] = self.data['phase_seconds'].get(phase, 0)+seconds
        self.data['phase_started_at'] = self.clock()

    def stage(self, phase, total, later_units=0, concurrency=None):
        self._close_phase()
        self.data.update(phase=phase, done=0, total=total, later_units=later_units,
                         unit_started_at=self.clock(), phase_started_at=self.clock(),
                         active_units={}, concurrency=concurrency or 1, task_mode=concurrency is not None)
        self.changed()

    def plan(self, **counts):
        self.data['plan'].update(counts)
        self.changed()

    def start_unit(self, key):
        self.data['active_units'][key] = self.clock()
        self.changed()

    def complete_unit(self, key, seconds, requests):
        d = self.data
        d['active_units'].pop(key, None)
        d['durations'].setdefault(d['phase'], []).append(max(.001, seconds))
        d['done'] += 1
        d['requests'].extend({'phase':d['phase'], **request} for request in requests)
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
        if 'finished_at' not in self.data:
            self._close_phase()
            self.data['finished_at'] = self.clock()

    def snapshot(self):
        result = copy.deepcopy(self.data)
        if 'finished_at' not in result:
            phase = result['phase']
            result['phase_seconds'][phase] = result['phase_seconds'].get(phase,0)+max(0,self.clock()-result['phase_started_at'])
        return result


def estimate(job, at=None):
    at = time.time() if at is None else at
    active = job['status'] in ('queued','collecting','summarizing','sending')
    timing = job.get('stats', {}).get('timing', {})
    started = datetime.fromisoformat(job['created']).timestamp()
    ended = timing.get('finished_at') or datetime.fromisoformat(job['updated']).timestamp()
    result = {'as_of': at, 'elapsed_seconds': max(0, (at if active else ended)-started),
              'active': active, 'remaining_low': None, 'remaining_high': None,
              'phase': timing.get('phase'), 'done': timing.get('done', 0),
              'total': timing.get('total', 0), 'delayed': False,
              'active_units':len(timing.get('active_units',{})) if active else 0,
              'concurrency':timing.get('concurrency',1)}
    if not active or not timing or timing['phase']=='collecting': return result
    phase = timing['phase']
    durations, prior = timing.get('durations', {}), timing.get('prior', {})
    def typical(name):
        values = durations.get(name) or prior.get(name) or []
        return median(values) if values else None
    pace = typical(phase)
    if pace is None and phase=='merging': pace = typical('extracting')
    if pace is None: return result
    units = max(0, timing['total']-timing['done'])
    if timing.get('task_mode'):
        elapsed = [max(0,at-start) for start in timing.get('active_units',{}).values()]
        if any(seconds>pace*1.8 for seconds in elapsed):
            result['delayed'] = True
            return result
        lanes = [max(0,pace-seconds) for seconds in elapsed]
        lanes += [0]*max(0,timing.get('concurrency',1)-len(lanes))
        for _ in range(max(0,units-len(elapsed))):
            lane = min(range(len(lanes)),key=lambda i:lanes[i])
            lanes[lane] += pace
        remaining = max(lanes,default=0)
    else:
        current = max(0, at-timing['unit_started_at'])
        remaining = max(0, pace*units-current)
        if units and current>pace*1.8:
            result['delayed'] = True
            return result  # Do not leave a misleading zero-minute countdown.
    later = timing.get('later_units', 0)
    if phase=='extracting':
        concurrency = timing.get('concurrency',1)
        remaining += ((later+concurrency-1)//concurrency)*(typical('merging') or pace)
    if phase in ('extracting','merging'): remaining += typical('sending') or 15
    result.update(remaining_low=max(5, remaining*.6), remaining_high=max(15, remaining*1.8))
    return result
