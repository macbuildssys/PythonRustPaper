import time
import functools
import multiprocessing as mp
import csv
import sys

MASK = (1 << 64) - 1


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


def _chunk_worker(chunk):
    return run_functional(chunk)


def run_multiprocess(data, pool, n_chunks):
    """Split data into n_chunks pieces and reduce each on a worker process

    IMPORTANT FIX: pool.map() is called with an explicit chunksize=1
    Without it, CPython's own default chunksize heuristic
    (chunksize = ceil(len(chunks) / (4 * pool_size))) silently re-batches
    the chunks we just created, capping the real number of inter-process
    sends at roughly 4 * pool_size regardless of n_chunks. That bug made
    an earlier version of this sweep produce a misleadingly flat result:
    every n_chunks value from 64 upward was, in practice, re-running the
    same ~16-send experiment. chunksize=1 forces one real IPC send per
    logical chunk, so n_chunks now actually controls IPC granularity
    """
    chunk_size = max(1, len(data) // n_chunks)
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    results = pool.map(_chunk_worker, chunks, chunksize=1)
    return functools.reduce(combine, results, 0), len(chunks)


def bench_once(data, pool, n_chunks, reps=5):
    _, actual_tasks = run_multiprocess(data, pool, n_chunks)  # warm-up
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        run_multiprocess(data, pool, n_chunks)
        times.append(time.perf_counter() - t0)
    best = min(times)
    avg = sum(times) / len(times)
    return best, avg, actual_tasks, times


def main():
    writer = csv.writer(sys.stdout)
    writer.writerow([
        "n", "pool_size", "n_chunks_requested", "actual_ipc_tasks",
        "chunk_size_records", "best_s", "avg_s", "all_times_s",
    ])
    pool_size = 4  # held fixed so only chunk granularity varies
    chunk_counts = [4, 16, 64, 256, 1024, 4096]
    sizes = [500_000, 2_000_000]

    with mp.Pool(processes=pool_size) as pool:
        for n in sizes:
            data = make_data(n)
            for nc in chunk_counts:
                best, avg, actual_tasks, times = bench_once(data, pool, nc, reps=5)
                chunk_size = max(1, n // nc)
                writer.writerow([
                    n, pool_size, nc, actual_tasks, chunk_size,
                    f"{best:.6f}", f"{avg:.6f}",
                    ";".join(f"{t:.6f}" for t in times),
                ])
                sys.stdout.flush()


if __name__ == "__main__":
    main()
