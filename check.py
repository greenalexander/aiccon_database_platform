import os
from pathlib import Path
from datetime import datetime

proc_dir = Path(r"C:\Users\green\AICCON\AICCON - Documenti\data_alex\aiccon-data\processed\labour")
for f in sorted(proc_dir.iterdir(), key=lambda x: x.stat().st_mtime):
    mtime = datetime.fromtimestamp(f.stat().st_mtime)
    size = f.stat().st_size
    print(f"{mtime}  {size:>12,} bytes  {f.name}")