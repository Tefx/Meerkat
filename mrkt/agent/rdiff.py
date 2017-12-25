import subprocess
import os.path


def dir_sig(path):
    p = subprocess.run(["rdiffdir", "sig", path, "-"],
                       stdout=subprocess.PIPE)
    return p.stdout


def dir_delta(sig, new_path):
    with open(".sig", "wb") as f:
        f.write(sig)
    p = subprocess.run(["rdiffdir", "delta", ".sig", new_path, "-"],
                       stdout=subprocess.PIPE,
                       input=sig)
    os.remove(".sig")
    return p.stdout


def dir_patch(path, delta):
    with open(".delta", "wb") as f:
        f.write(delta)
    subprocess.run(["rdiffdir", "patch", path, ".delta"])
    os.remove(".delta")
    return True


if __name__ == '__main__':
    sig = dir_sig("../test_mrkt_2")
    delta = dir_delta(sig, "../test_mrkt")
    dir_patch("../test_mrkt_2", delta)
