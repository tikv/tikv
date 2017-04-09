#!/usr/bin/env python

"""Run rustfmt on diff lines.

Diff lines are extracted via git.
"""

import sys
import json
from subprocess import PIPE, Popen, check_call

DIFF_START_PREFIX = "diff --git a/"
CHANGED_FILE_PREFIX = "+++ b/"
DIFF_HINT_PREFIX = "@@ -"

def main():
    if len(sys.argv) < 3:
        sys.stderr.write(
            "Please specify base commit and rustfmt\n"
            "Usage %s commit rustfmt [..args]\n" % sys.argv[0])
        exit(1)

    diff_check = Popen(["git", "diff", "-U0", sys.argv[1], "--", "*.rs"], stdout=PIPE)
    cur_file = None
    to_format = []
    involved_files = []
    for line in diff_check.stdout:
        line = line.decode()
        if line.startswith(DIFF_START_PREFIX):
            # don't extract file name here in case there is space in path.
            cur_file = None
        elif line.startswith(CHANGED_FILE_PREFIX):
            cur_file = line[len(CHANGED_FILE_PREFIX):-1]
        elif line.startswith(DIFF_HINT_PREFIX) and cur_file is not None:
            # diff hint should like "@@ -line,count +line(,count) @@"
            changes = line.split()[2]
            line_counts = list(map(int, changes[1:].split(',')))
            if len(line_counts) == 1:
                line, count = line_counts[0], 1
            else:
                line, count = line_counts
            to_format.append({
                "file": cur_file,
                "range": [line, line + count],
            })
            if not involved_files or involved_files[-1] != cur_file:
                involved_files.append(cur_file)

    if diff_check.wait() != 0:
        exit(diff_check.returncode)
    if not involved_files:
        exit(0)

    args = sys.argv[2:]
    args.extend(["--file-lines", json.dumps(to_format)])
    args.extend(involved_files)
    check_call(args)

if __name__ == "__main__":
    main()
