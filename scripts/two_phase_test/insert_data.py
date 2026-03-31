#!/usr/bin/env python3
"""Insert 500K rows of 200-char random strings into bench.t."""
import subprocess, random, string

def rand_str(n):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

rows = []
for i in range(500000):
    pad = rand_str(200)
    rows.append(f"('{pad}')")
    if len(rows) == 1000:
        vals = ','.join(rows)
        subprocess.run(
            ['mysql', '-h', '127.0.0.1', '-P', '4000', '-u', 'root', 'bench',
             '-e', f'INSERT INTO t (pad) VALUES {vals}'],
            check=True, capture_output=True)
        rows = []
        if i % 100000 == 99999:
            print(f'{i+1} rows inserted')

if rows:
    vals = ','.join(rows)
    subprocess.run(
        ['mysql', '-h', '127.0.0.1', '-P', '4000', '-u', 'root', 'bench',
         '-e', f'INSERT INTO t (pad) VALUES {vals}'],
        check=True, capture_output=True)
print('done - 500K rows')
