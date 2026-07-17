use rayon::prelude::*;
use std::time::Instant;

#[derive(Clone, Copy)]
struct Record {
    id: u64,
    value: u64,
}

#[inline]
fn is_valid(r: &Record) -> bool {
    r.value % 3 != 0
}

#[inline]
fn normalise(r: &Record) -> Record {
    Record { id: r.id, value: r.value.wrapping_mul(2).wrapping_add(1) }
}

#[inline]
fn hash_record(r: &Record) -> u64 {
    let mut h: u64 = 1469598103934665603;
    let id_bytes = r.id.to_le_bytes();
    let val_bytes = r.value.to_le_bytes();
    for &b in id_bytes.iter().chain(val_bytes.iter()) {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
}

#[inline]
fn combine(a: u64, b: u64) -> u64 {
    a.wrapping_add(b)
}

fn make_data(n: u64) -> Vec<Record> {
    (0..n).map(|i| Record { id: i, value: (i * 2654435761) % 1_000_003 }).collect()
}

fn run_sequential(data: &[Record]) -> u64 {
    data.iter()
        .filter(|r| is_valid(r))
        .map(normalise)
        .map(|r| hash_record(&r))
        .fold(0u64, combine)
}

fn run_parallel(data: &[Record]) -> u64 {
    data.par_iter()
        .filter(|r| is_valid(r))
        .map(normalise)
        .map(|r| hash_record(&r))
        .reduce(|| 0u64, combine)
}

// Two-tailed 95% Student's-t critical values, keyed by degrees of freedom (reps - 1).
// Matches the table used in the Python benchmark script so both report CIs the same way.
fn t_crit(df: u32) -> f64 {
    match df {
        1 => 12.706,
        2 => 4.303,
        3 => 3.182,
        4 => 2.776,
        5 => 2.571,
        6 => 2.447,
        7 => 2.365,
        8 => 2.306,
        9 => 2.262,
        10 => 2.228,
        0 => f64::NAN,
        _ => 1.96, // normal approximation for larger df than tabulated
    }
}

fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn sample_stdev(xs: &[f64], m: f64) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let var = xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() as f64 - 1.0);
    var.sqrt()
}

/// Run f() reps times (plus one discarded warm-up) and report best, mean, sample
/// standard deviation, and a 95% confidence interval on the mean, instead of only
/// the best-of-N figure used previously.
fn bench<F: Fn() -> u64>(label: &str, n: u64, f: F, reps: u32) {
    let _ = f(); // warm-up, discarded
    let mut times: Vec<f64> = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        let t0 = Instant::now();
        let result = f();
        let dt = t0.elapsed().as_secs_f64();
        times.push(dt);
        std::hint::black_box(result);
    }

    let best = times.iter().cloned().fold(f64::MAX, f64::min);
    let m = mean(&times);
    let sd = sample_stdev(&times, m);
    let df = reps.saturating_sub(1);
    let half_width = if reps >= 2 && sd > 0.0 {
        t_crit(df) * sd / (reps as f64).sqrt()
    } else {
        0.0
    };
    let ci_low = m - half_width;
    let ci_high = m + half_width;

    let throughput_best = n as f64 / best;
    let throughput_mean = n as f64 / m;

    let times_str = times
        .iter()
        .map(|t| format!("{t:.6}"))
        .collect::<Vec<_>>()
        .join(";");

    println!(
        "{label},{n},{reps},{best:.6},{m:.6},{sd:.6},{ci_low:.6},{ci_high:.6},{throughput_best:.1},{throughput_mean:.1},{times_str}"
    );
}

fn main() {
    println!("mode,n,reps,best_s,mean_s,stdev_s,ci95_low_s,ci95_high_s,throughput_best_rec_per_s,throughput_mean_rec_per_s,all_times_s");
    let sizes: [u64; 4] = [100_000, 1_000_000, 5_000_000, 20_000_000];
    let reps = 7;
    for &n in &sizes {
        let data = make_data(n);
        bench("rust_sequential", n, || run_sequential(&data), reps);
        bench("rust_rayon_parallel", n, || run_parallel(&data), reps);
    }
    eprintln!("logical_cpus={}", rayon::current_num_threads());
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sequential_matches_parallel() {
        for &n in &[1_000u64, 12_345u64, 250_000u64] {
            let data = make_data(n);
            let seq = run_sequential(&data);
            let par = run_parallel(&data);
            assert_eq!(seq, par, "mismatch at n={}", n);
        }
    }
}
