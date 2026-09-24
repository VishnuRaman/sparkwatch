"""Replay a `script` capture of sparkwatch through a terminal emulator and
check expected strings against the *reconstructed screens*, frame by frame.
Usage: screens.py CAPTURE COLS ROWS [--dump] EXPECT... ; prefix '!' = must be absent everywhere."""
import sys, pyte
cap, cols, rows = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
args = sys.argv[4:]
dump = '--dump' in args
expect = [a for a in args if a != '--dump']
data = open(cap, 'rb').read()
screen = pyte.Screen(cols, rows); stream = pyte.ByteStream(screen)
frames = []
# ratatui begins every frame by hiding the cursor; snapshot the screen at each.
for chunk in data.split(b'\x1b[?25l'):
    stream.feed(chunk)
    frames.append('\n'.join(screen.display))
frames = [f for f in frames if f.strip()]
if dump:
    for i, f in enumerate(frames):
        print(f'----- frame {i} -----'); print('\n'.join(l.rstrip() for l in f.splitlines() if l.strip()))
allf = '\n'.join(frames)
bad = 0
for e in expect:
    if e.startswith('!'):
        ok = e[1:] not in allf
    else:
        ok = e in allf
    bad += not ok
    print(f"  {'ok  ' if ok else 'MISS'} {e}")
print(f"{len(frames)} frames, {bad} miss(es)")
