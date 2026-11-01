#!/usr/bin/env python3
"""Usage: rss_sampler.py <out.tsv> <process name> <max seconds>
Waits for the newest process of that name, then writes its VmRSS and VmHWM once per second."""
import os
import sys
import time


def newest(name):
    best = None
    for pid in filter(str.isdigit, os.listdir('/proc')):
        try:
            if open(f'/proc/{pid}/comm').read().strip() != name:
                continue
            start = os.stat(f'/proc/{pid}').st_ctime
        except OSError:
            continue
        if best is None or start > best[0]:
            best = (start, pid)
    return best and best[1]


def status(pid):
    fields = {}
    for line in open(f'/proc/{pid}/status'):
        key, _, value = line.partition(':')
        fields[key] = value.split()[0] if value.split() else ''
    return fields


out, name, limit = sys.argv[1], sys.argv[2], float(sys.argv[3])
t0 = time.time()
pid = None
while pid is None and time.time() - t0 < 60:
    pid = newest(name)
    time.sleep(0.1)
with open(out, 'w') as f:
    f.write('wall_unix\tvmrss_kb\tvmhwm_kb\n')
    while pid is not None and time.time() - t0 < limit:
        try:
            s = status(pid)
        except OSError:
            break
        f.write(f"{time.time():.3f}\t{s.get('VmRSS', '')}\t{s.get('VmHWM', '')}\n")
        f.flush()
        time.sleep(1.0)
