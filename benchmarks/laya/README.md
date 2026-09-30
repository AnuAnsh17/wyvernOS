# Isolated Laya benchmarks

These scripts establish empirical Laya inference and decision-quality baselines. They are experimental tools under `benchmarks/`; they are not part of Wyvern's production runtime and do not connect to its kernel, scheduler, context, memory, capability, or orchestration systems. No production Laya adapter or model router is implemented here.

## Python environments

Two existing environments are kept outside the repository:

| Environment | Python | PyTorch | Intended use |
| --- | --- | --- | --- |
| `C:\Users\anura\Desktop\laya-benchmark\.venv` | 3.14.2 | 2.14.0+cpu | Historical CPU baseline |
| `C:\Users\anura\Desktop\laya-benchmark\.venv-cuda` | 3.12.10 | 2.14.0+cu130 | CUDA and quality runs |

Activate the CUDA environment in PowerShell from the repository root:

```powershell
& "C:\Users\anura\Desktop\laya-benchmark\.venv-cuda\Scripts\Activate.ps1"
python --version
pip show laya torch
python -c "import torch; print(torch.__version__, torch.cuda.is_available(), torch.version.cuda); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'no CUDA GPU')"
```

The validated target is an NVIDIA GeForce RTX 3050 Laptop GPU with 6 GB VRAM and driver 596.08. Laya 0.3.21 and the English `convaiinnovations/laya` checkpoint are already installed/downloaded in the environments. The scripts do not move or duplicate either environment. Use `--model` to select another model ID or local checkpoint directory.

## Sequential CUDA performance benchmark

Run from the repository root:

```powershell
python benchmarks/laya/benchmark_cuda.py
```

The default run loads the checkpoint once on CUDA, performs five warm-ups, and records 100 sequential measured predictions. The same fixed Wyvern-style workload used by `benchmark.py` is cycled in order. Options are `--samples` (minimum 100), `--warmups` (minimum five), `--device`, `--model`, and `--output`.

For every timed CUDA call, the script synchronizes immediately before starting the high-resolution wall-clock timer and immediately after `Agent.predict(...)` returns. The measurement therefore includes completion of GPU work rather than just enqueueing asynchronous CUDA operations. The model remains resident for warm-up, measurement, and any separately requested stress run. CUDA peak memory counters are reset after warm-up and before the measured loop.

The result records cold model loading, raw per-call latency, min/max/mean/median, nearest-rank p50/p90/p95/p99, population standard deviation, total measured wall time, and throughput. It records process RSS/peak working set and CPU usage where available, CUDA allocated/reserved/peak memory, total VRAM, and NVIDIA system snapshots. `nvidia-smi` utilization values are before/after samples; they are not peak GPU utilization. Each result is written to `benchmarks/laya/results/cuda/` unless `--output` is provided.

The script also reads the latest available root-level CPU benchmark result and includes it as a clearly labeled historical measurement. Missing CPU fields remain null; values are never synthesized. Current CUDA results and the historical CPU baseline are separate fields in the result JSON.

The recorded CPU baseline from the CPU-only environment is: cold start 7,154.450 ms; warm p50 766.214 ms; p95 839.554 ms; p99 1,006.996 ms; mean 774.366 ms; throughput 1.291 decisions/sec; peak working set about 2.76 GB. These numbers came from 100 sequential predictions after five warm-ups and are environment-specific.

The first completed synchronized CUDA run on the target RTX 3050 measured cold start 10,446.398 ms; warm p50 64.376 ms; p95 66.649 ms; p99 68.788 ms; mean 64.840 ms; throughput 15.419 decisions/sec; CUDA peak allocated 2,431,107,072 bytes; and peak process working set 3,133,829,120 bytes. A later opt-in stress run's sequential portion measured p50 64.086 ms and throughput 15.540 decisions/sec. The earlier single CUDA call near 596.74 ms is retained as the initial observation; the warmed 100-call result is the comparable sequential measurement.

## Decision quality benchmark v1

Run the curated fixed scenario set:

```powershell
python benchmarks/laya/benchmark_quality.py
```

The default uses CUDA and loads one model for all scenarios. Pass `--device cpu` to evaluate on CPU, `--model` for another checkpoint, `--scenarios` for another JSON dataset, or `--output` to choose a result path. The inspectable 104-scenario dataset is `scenarios/wyvern_decisions_v1.json`.

Each scenario separates its inference input, expected answer, and evaluation rationale. Only `input.state` plus the matching question definition is sent to Laya; expected answers and rationales are withheld until evaluation. The set covers task routing, execution strategy, runtime lifecycle, control/escalation, failure recovery, verification, 0–4 complexity scores, and four binary decision questions.

The report includes overall and category accuracy, per-option precision/recall/F1, macro F1, confusion matrices, complexity score error, probabilities, and raw per-scenario latency and correctness. It reports both Laya `confidence` and `answer_confidence`. Calibration buckets compare each field with observed correctness for choice and binary answers; score questions are excluded because score confidence is not a correctness probability. ECE is the weighted absolute difference between bucket accuracy and mean confidence. Brier scores are reported by question: binary squared probability error for `noul`, and the sum of squared class probability errors for `choice`.

On the recorded 104-scenario CUDA run, overall accuracy was 53.8%. Category accuracy was 80.0% for task routing, 33.3% for execution strategy, 53.3% for runtime lifecycle, 33.3% for control/escalation, 66.7% for failure recovery, 53.3% for verification, 50.0% rounded-class accuracy for complexity, and 75.0% for binary decisions. ECE was 0.225 for `confidence` and 0.087 for `answer_confidence` across the 94 categorical predictions. These are observations on this fixed small dataset, not general performance claims.

Confidence values are measurements to evaluate, not assurances that a decision is reliable. Calibration depends on this checkpoint and on whether the curated scenarios resemble future use. The scenario labels and rationales are benchmark metadata and do not make this a production readiness evaluation. Results are written under `benchmarks/laya/results/quality/`.

## Optional concurrency stress experiment

The sequential CUDA latency results remain separate. To run an additional shared-model thread stress experiment after them:

```powershell
python benchmarks/laya/benchmark_cuda.py --stress
```

This submits a fixed set of requests at concurrency levels 1, 2, 4, and 8, and records throughput, latency percentiles, errors, process CPU, CUDA memory, and before/after NVIDIA snapshots for each level. This is opt-in because the public Laya API does not promise arbitrary shared-`Agent` thread safety. The stress results are stored in their own `stress` field and are not mixed into primary sequential statistics. Treat errors or memory pressure as evidence about this environment, not as a supported production concurrency contract.

The recorded experiment completed all 16 requests at each level with no errors. Throughput was 15.26, 17.02, 18.00, and 19.07 requests/sec at concurrency 1, 2, 4, and 8; corresponding p50 latencies were 64.6, 116.8, 220.8, and 406.5 ms. The modest throughput increase came with substantially higher per-request latency. This single run is exploratory and does not establish a production concurrency policy.

## Laya API and checkpoint warning

The scripts use the installed public API discovered in Laya 0.3.21:

```python
agent = laya.load("convaiinnovations/laya", device="cuda")
agent.predict(state, questions, lang="en")
```

`questions` is a dictionary keyed by question ID; choice criteria are a dictionary of option names to descriptions. The quality benchmark also uses the supported `score` and `noul` types.

The checkpoint emits a warning that its `choice:11+` temperature is outside Laya's valid range and is clamped to `0.5`. The benchmark records and displays this warning without suppressing it or modifying the checkpoint. Laya warns that confidence for affected entries may be uncalibrated. The benchmark's choice questions have at most ten options, but the checkpoint warning remains relevant when interpreting future results with larger option sets.

## Comparing CPU and CUDA

Use the JSON fields rather than comparing only a single CLI timing. The CPU values are historical and were measured with a CPU-only PyTorch build; CUDA values are newly measured with synchronized calls on the RTX 3050. Compare cold start separately from warm p50/p95/p99, mean, throughput, process RAM, and VRAM. Differences in Python, PyTorch build, driver, device load, and checkpoint revision can affect results. Repeat runs under representative system conditions before drawing conclusions.

Generated timestamped JSON files under `results/` are ignored by Git. They contain environment information, git revision when available, raw measurements, summary metrics, memory details, warnings, and limitations. Benchmark result artifacts should remain local unless explicitly requested for sharing.

## Implications for Wyvern

These measurements can provide evidence for evaluating Laya as a future local decision layer, including routing among runtime, tool, model, or escalation actions. They do not establish that Laya is reliable enough, fast enough under concurrent load, or appropriately calibrated for production. Architecture decisions also need broader decision-quality, safety, concurrency, and hardware testing.

The future flow may be evaluated as:

```text
User objective
      ↓
Wyvern Runtime
      ↓
Decision Engine
      ↓
Laya
      ↓
decision + confidence
      ↓
Model Router / Runtime / Tool / Escalation
```

This benchmark establishes evidence only. It does not implement or integrate that production flow.
