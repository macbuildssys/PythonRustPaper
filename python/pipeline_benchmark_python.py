import time
import functools
import multiprocessing as mp
import csv
import sys
import statistics

MASK = (1 << 64) - 1

# Two-tailed 95% Student's-t critical values, keyed by degrees of freedom (reps - 1)
# Kept as a small lookup table (rather than depending on scipy) so this script has
# zero third-party dependencies and runs anywhere Python 3 does.
T_CRIT_95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
             6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228}


def t_crit(df):
    if df in T_CRIT_95:
        return T_CRIT_95[df]
    if df < 1:
        return float("nan")
    return 1.96  # normal approximation for larger df than tabulated


def is_valid(rec):
    return rec[1] % 3 != 0


def normalise(rec):
    return (rec[0], (rec[1] * 2 + 1) & MASK)


def hash_record(rec):
    h = 1469598103934665603
    idb = rec[0].to_bytes(8, "little")
    vb = rec[1].to_bytes(8, "little")
    for b in idb + vb:
        h ^= b
        h = (h * 1099511628211) & MASK
    return h


def combine(a, b):
    return (a + b) & MASK


def make_data(n):
    return [(i, (i * 2654435761) % 1_000_003) for i in range(n)]


def run_functional(data):
    pipeline = map(hash_record, map(normalise, filter(is_valid, data)))
    return functools.reduce(combine, pipeline, 0)


def run_imperative(data):
    total = 0
    for rec in data:
        rid, val = rec
        if val % 3 == 0:
            continue
        val = (val * 2 + 1) & MASK
        h = 1469598103934665603
        idb = rid.to_bytes(8, "little")
        vb = val.to_bytes(8, "little")
        for b in idb + vb:
            h ^= b
            h = (h * 1099511628211) & MASK
        total = (total + h) & MASK
    return total


def _chunk_worker(chunk):
    return run_functional(chunk)


def run_multiprocess(data, pool, n_chunks):
    chunk_size = max(1, len(data) // n_chunks)
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    results = pool.map(_chunk_worker, chunks)
    return functools.reduce(combine, results, 0)


def bench(label, n, fn, reps, writer):
    """Run fn() reps times (plus one discarded warm-up) and report best, mean,
    sample standard deviation, and a 95% confidence interval on the mean,
    instead of only the best-of-N figure used previously"""
    fn()  # warm-up, discarded
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        times.append(dt)

    best = min(times)
    mean = statistics.mean(times)
    stdev = statistics.stdev(times) if reps >= 2 else 0.0
    df = reps - 1
    if reps >= 2 and stdev > 0:
        half_width = t_crit(df) * stdev / (reps ** 0.5)
    else:
        half_width = 0.0
    ci_low = mean - half_width
    ci_high = mean + half_width

    throughput_best = n / best
    throughput_mean = n / mean

    writer.writerow([
        label, n, reps,
        f"{best:.6f}", f"{mean:.6f}", f"{stdev:.6f}",
        f"{ci_low:.6f}", f"{ci_high:.6f}",
        f"{throughput_best:.1f}", f"{throughput_mean:.1f}",
        ";".join(f"{t:.6f}" for t in times),
    ])
    sys.stdout.flush()


def main():
    # Sizes kept smaller than the Rust benchmark: CPython's per-element interpretive
    # overhead makes 20M-record runs impractical for repeated trials in this environment
    sizes = [50_000, 200_000, 1_000_000, 4_000_000]
    reps_by_size = {50_000: 7, 200_000: 7, 1_000_000: 5, 4_000_000: 3}
    writer = csv.writer(sys.stdout)
    writer.writerow([
        "mode", "n", "reps",
        "best_s", "mean_s", "stdev_s",
        "ci95_low_s", "ci95_high_s",
        "throughput_best_rec_per_s", "throughput_mean_rec_per_s",
        "all_times_s",
    ])

    with mp.Pool(processes=max(2, mp.cpu_count())) as pool:
        for n in sizes:
            reps = reps_by_size[n]
            data = make_data(n)
            bench("python_functional", n, lambda: run_functional(data), reps, writer)
            bench("python_imperative", n, lambda: run_imperative(data), reps, writer)
            if n >= 200_000:
                bench(
                    "python_multiprocess",
                    n,
                    lambda: run_multiprocess(data, pool, max(2, mp.cpu_count()) * 4),
                    min(reps, 3),
                    writer,
                )


if __name__ == "__main__":
    main()
