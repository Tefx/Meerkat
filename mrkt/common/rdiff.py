import subprocess
import os.path
import os
import lzma

from mrkt.common.consts import RDIFF_FILE_SIG, RDIFF_FILE_DELTA


def dir_sig(path, is_dir=True):
    if not os.path.exists(path):
        if is_dir:
            os.mkdir(path)
        else:
            os.mknod(path)
    p = subprocess.run(["rdiffdir", "sig", path, "-"],
                       stdout=subprocess.PIPE)
    return lzma.compress(p.stdout)


def dir_delta(sig, new_path):
    with open(RDIFF_FILE_SIG, "wb") as f:
        f.write(lzma.decompress(sig))
    p = subprocess.run(["rdiffdir", "delta", RDIFF_FILE_SIG, new_path, "-"],
                       stdout=subprocess.PIPE,
                       input=sig)
    os.remove(RDIFF_FILE_SIG)
    return lzma.compress(p.stdout)


def dir_patch(path, delta):
    with open(RDIFF_FILE_DELTA, "wb") as f:
        f.write(lzma.decompress(delta))
    subprocess.run(["rdiffdir", "patch", path, RDIFF_FILE_DELTA])
    os.remove(RDIFF_FILE_DELTA)
    return True
