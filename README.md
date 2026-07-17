 
# Purity as a parallelisation contract: Reproducibility artifacts

Benchmark source, raw results, and a mechanism isolation experiment supporting
"Purity as a Parallelisation Contract: Formal Composability Guarantees and
Empirical Throughput in Python and Rust Data Pipelines."

## What's here and what it supports

| File | Paper section | What it is |
|---|---|---|
| `python/pipeline_benchmark_python.py` | Tables I to IV, Fig. 1 to 4 | Canonical Python benchmark: functional, imperative, and multiprocess pipeline variants, with mean, stdev, and 95% CI reporting. |
| `rust/rust_bench/` | Tables I, III, Fig. 1 to 4 | Cargo project: sequential and rayon parallel pipeline variants, same CI reporting, plus the correctness unit test (`cargo test --release`). |
| `mechanism_sweep/sweep.py` | Tables V, VI, Section VII-C | Corrected chunk size sweep (`chunksize=1` forced explicitly; see Section VII-C for why the unforced default silently breaks this experiment). |
| `results/environment_A_pinned_core/` | Tables I, II | Single core results: same physical host as Environment B, restricted to one logical core via `taskset -c 0`. |
| `results/environment_B_22_threads/` | Tables III, IV | 22 thread results, same host, unrestricted. |
| `results/sweep_dedicated_hardware/` | Tables V, VI | The corrected sweep's output, run on dedicated hardware, not a shared sandbox. See Section VII-C for why that distinction matters. |
| `results/superseded_exploratory/` | Not cited | Early runs from before mean and CI reporting was added, and before the chunksize bug was found. Kept for transparency, not the source of any number printed in the paper. Do not cite these. |

## Reproducing the results

Python, Environments A and B:

```
python3 python/pipeline_benchmark_python.py | tee results_python.csv

taskset -c 0 python3 python/pipeline_benchmark_python.py | tee results_python_1core.csv
```

Rust, Environments A and B:

```
cd rust/rust_bench

cargo build --release

./target/release/pipeline_bench | tee ../../results_rust.csv

taskset -c 0 ./target/release/pipeline_bench | tee ../../results_rust_1core.csv

cargo test --release
```

The `cargo test` step runs the bit for bit correctness check comparing
sequential and parallel output.

Chunk size sweep, Section VII-C:

```
cd mechanism_sweep

python3 sweep.py | tee sweep_results.csv
```

Run this on dedicated hardware, not a shared or virtualised host. The paper
documents, in detail, how a noisy sandbox produced two different and
inconsistent results before this was rerun properly on real hardware. That
is not a hypothetical warning; it happened during this project, and it is
the reason this note is here.

## Environment as tested

- Host: 16 physical cores (6 performance cores with hyperthreading, 10 efficiency cores), 22 logical threads.
- Python 3.12.3 (CPython).
- Rust via rustup, current stable at time of testing.
- CPU governor set to performance before benchmarking.

Absolute throughput numbers are hardware specific and will not reproduce
exactly on different silicon. The qualitative findings, Rust's implied
parallel fraction converging versus Python's eroding, and the map fusion
and split fold correctness properties, should hold on any multi core host.

## Correctness

Every number in this repository was checked against an independent
implementation before being reported:

- Rust sequential output equals Rust parallel output (`cargo test --release`).
- Python functional output equals imperative output equals multiprocess
  output, for several values of N.
- Python output equals Rust output, for several values of N, on a minimal
  cross language check using the same 64 bit totals, byte for byte.

## Known limitations of this artifact set

- Single dedicated hardware host; no cross architecture validation across
  Intel, AMD, or Apple Silicon.
- Single workload shape (filter, then map, then map, then reduce). Results
  should not be assumed to generalise to other pipeline topologies without
  further testing.
- The N = 2,000,000 point in the chunk size sweep (Table VI) carries higher
  measurement noise than the rest of the dataset. See Section VII-C for the
  specific coefficient of variation figures.

## License

Distributed under the MIT License. See [LICENSE](LICENSE). 
Paper text and figures are not included in this repository; see the paper itself for terms.

## Citing this work

If you use this code or data, please cite the paper. DOI:10.5281/zenodo.21420325
