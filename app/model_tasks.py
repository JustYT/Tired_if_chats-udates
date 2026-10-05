"""Bounded model work; only the coordinator updates progress and persistent state."""
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
import threading
import time

from .history import checkpoint

MODEL_WORKERS = 2


class TaskCancellation:
    def __init__(self, parent):
        self.parent = parent
        self.failed = threading.Event()

    def is_set(self):
        return self.parent.is_set() or self.failed.is_set()


@dataclass
class Outcome:
    value: object
    error: object
    seconds: float
    calls: list


def parallel_tasks(tasks, function, cancel, timing, phase, progress, later_units=0):
    if not tasks:
        return []
    workers = min(MODEL_WORKERS, len(tasks))
    stop = TaskCancellation(cancel)
    values = [None] * len(tasks)
    pending = {}
    next_index = 0
    if timing:
        timing.stage(phase, len(tasks), later_units, concurrency=workers)

    def invoke(task):
        calls = []
        started = time.monotonic()
        try:
            checkpoint(stop)
            value = function(task, stop, calls)
            checkpoint(stop)
            return Outcome(value, None, time.monotonic()-started, calls)
        except Exception as error:
            return Outcome(None, error, time.monotonic()-started, calls)

    def record(index, result):
        if timing:
            timing.complete_unit(str(index), result.seconds, result.calls)
        values[index] = result.value

    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='summary-model')
    try:
        while next_index < len(tasks) or pending:
            checkpoint(stop)
            while next_index < len(tasks) and len(pending) < workers:
                checkpoint(stop)
                index = next_index
                if timing:
                    timing.start_unit(str(index))
                pending[executor.submit(invoke, tasks[index])] = index
                next_index += 1
            finished, _ = wait(pending, timeout=.2, return_when=FIRST_COMPLETED)
            failure = None
            for future in sorted(finished, key=lambda f: pending[f]):
                index = pending.pop(future)
                result = future.result()
                record(index, result)
                if result.error and failure is None:
                    failure = result.error
            if failure:
                raise failure
            if finished:
                completed = next_index-len(pending)
                label = 'Саммаризация' if phase == 'extracting' else 'Объединяем темы'
                progress(f'{label}: {completed}/{len(tasks)}')
    finally:
        stop.failed.set()
        # Codex observes cancellation while waiting and closes each isolated RPC.
        # Synchronous providers drain at their existing request timeout. No result
        # or delivery can escape until every worker has finished.
        executor.shutdown(wait=True, cancel_futures=True)
        for future, index in pending.items():
            if not future.cancelled():
                record(index, future.result())
    checkpoint(cancel)
    return values
