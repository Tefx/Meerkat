import subprocess
import os.path
import os
import lzma


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
    with open(".sig", "wb") as f:
        f.write(lzma.decompress(sig))
    p = subprocess.run(["rdiffdir", "delta", ".sig", new_path, "-"],
                       stdout=subprocess.PIPE,
                       input=sig)
    os.remove(".sig")
    return lzma.compress(p.stdout)


def dir_patch(path, delta):
    with open(".delta", "wb") as f:
        f.write(lzma.decompress(delta))
    subprocess.run(["rdiffdir", "patch", path, ".delta"])
    os.remove(".delta")
    return True


if __name__ == '__main__':
    sig = dir_sig("../test_mrkt")
    delta = dir_delta(sig, "../test_mrkt")
    dir_patch("../test_mrkt_2", delta)
