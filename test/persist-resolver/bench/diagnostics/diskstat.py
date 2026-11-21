#!/usr/bin/env python3
# Disk writes/s, MB/s, mean write latency, utilisation and flushes/s of nvme0n1 over argv[1] s.
import sys, time
def read():
    for l in open('/proc/diskstats'):
        f = l.split()
        if f[2] == 'nvme0n1':
            return [int(x) for x in f[3:]]
s = float(sys.argv[1]) if len(sys.argv) > 1 else 30
a = read(); time.sleep(s); b = read()
d = [y - x for x, y in zip(a, b)]
w, sec, wms, io = d[4], d[6], d[7], d[9]
fl = d[14] if len(d) > 14 else 0
print(f"writes/s {w/s:.0f}  write MB/s {sec*512/s/2**20:.1f}  mean write ms {wms/w if w else 0:.2f}  util {io/(s*10):.0f}%  flushes/s {fl/s:.0f}")
