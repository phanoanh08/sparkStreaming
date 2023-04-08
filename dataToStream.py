import os
from pathlib import Path
import shutil
import glob
import time

if __name__ == "__main__":
    root = 'data/'
    sroot = 'sdata/'
    if os.path.exists(sroot):
        shutil.rmtree(sroot)
    os.makedirs(sroot)
        
    for y in Path(root).iterdir():
        if y == Path(root+'year=__HIVE_DEFAULT_PARTITION__'):
            continue
        if y.is_dir():
            for m in y.iterdir():
                for d in m.iterdir():
                    d = str(d)+"\*.c000.csv"
                    csv = glob.glob(d)
                    for item in csv:
                        tmp = str(item).split("\\")[1:]
                        new_file_name = "{}-{}-{}-{}".format(tmp[0], tmp[1], tmp[2], tmp[3])
                        print(">>>>>", new_file_name)
                        shutil.copy(Path(item), Path(sroot+new_file_name))
                        time.sleep(5)
