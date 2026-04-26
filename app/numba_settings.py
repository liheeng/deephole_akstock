import os
import numba
import vectorbt as vbt


def auto_threads():
    cpu = os.cpu_count() or 1
    return max(1, cpu - 1)


def get_cpu():
    try:
        return len(os.sched_getaffinity(0))  # Linux container best
    except:
        return os.cpu_count() or 1


def allocate_cpu_to_numba_vbt():
    threads = auto_threads()
    os.environ["NUMBA_NUM_THREADS"] = str(threads)
    os.environ["OMP_NUM_THREADS"] = str(threads)
    os.environ["MKL_NUM_THREADS"] = str(threads)
    cpu = max(1, get_cpu() - 1)
    numba.set_num_threads(cpu)
    vbt.settings["caching"]["enabled"] = True