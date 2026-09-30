#!/usr/bin/env python3
"""Measure synchronized, sequential Laya inference on CUDA."""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.metadata
import json
import platform
import subprocess
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from benchmark import (
    DEFAULT_MODEL,
    RESULTS_DIR,
    QUESTIONS,
    WORKLOAD,
    decision_summary,
    nvidia_smi_snapshot,
    process_cpu_seconds,
    process_memory,
    summarize,
)


DEFAULT_SAMPLES = 100
DEFAULT_WARMUPS = 5


def git_revision() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            check=True,
            text=True,
            timeout=3,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def percentile_samples(samples: list[dict[str, Any]]) -> list[float]:
    return [float(sample["latency_ms"]) for sample in samples]


def synchronized_predict(torch: Any, agent: Any, state: dict[str, str]) -> dict[str, Any]:
    if agent.device.type == "cuda":
        torch.cuda.synchronize(agent.device)
    output = agent.predict(state, QUESTIONS, lang="en")
    if agent.device.type == "cuda":
        torch.cuda.synchronize(agent.device)
    return output


def run_stress(torch: Any, agent: Any, request_count: int = 16) -> dict[str, Any]:
    """Opt-in shared-Agent thread stress; results are isolated from sequential stats."""
    levels = (1, 2, 4, 8)
    results = []
    for workers in levels:
        if agent.device.type == "cuda":
            torch.cuda.synchronize(agent.device)
        system_before = nvidia_smi_snapshot()
        allocated_before = int(torch.cuda.memory_allocated(agent.device)) if agent.device.type == "cuda" else None
        reserved_before = int(torch.cuda.memory_reserved(agent.device)) if agent.device.type == "cuda" else None
        if agent.device.type == "cuda":
            torch.cuda.reset_peak_memory_stats(agent.device)
        cpu_before = process_cpu_seconds()
        wall_before = time.perf_counter_ns()
        samples = []
        errors = []

        def task(index: int) -> dict[str, Any]:
            item = WORKLOAD[index % len(WORKLOAD)]
            start = time.perf_counter_ns()
            output = synchronized_predict(torch, agent, {"request": item["request"]})
            end = time.perf_counter_ns()
            return {
                "sample_index": index + 1,
                "scenario_id": item["id"],
                "latency_ms": (end - start) / 1_000_000,
                "decision": decision_summary(output),
            }

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="laya-stress") as pool:
            futures = [pool.submit(task, index) for index in range(request_count)]
            for future in as_completed(futures):
                try:
                    samples.append(future.result())
                except Exception as error:  # record unsupported/OOM/thread-safety failures
                    errors.append(f"{type(error).__name__}: {error}")
        elapsed_ns = time.perf_counter_ns() - wall_before
        cpu_after = process_cpu_seconds()
        if agent.device.type == "cuda":
            torch.cuda.synchronize(agent.device)
            allocated_after = int(torch.cuda.memory_allocated(agent.device))
            reserved_after = int(torch.cuda.memory_reserved(agent.device))
            peak_allocated = int(torch.cuda.max_memory_allocated(agent.device))
            peak_reserved = int(torch.cuda.max_memory_reserved(agent.device))
        else:
            allocated_after = reserved_after = peak_allocated = peak_reserved = None
        values = percentile_samples(samples)
        cpu_percent = None
        if cpu_before is not None and cpu_after is not None and elapsed_ns:
            cpu_percent = max(0.0, (cpu_after - cpu_before) * 1_000_000_000 / elapsed_ns * 100)
        results.append(
            {
                "concurrent_requests": workers,
                "requested": request_count,
                "completed": len(samples),
                "errors": errors,
                "elapsed_ms": elapsed_ns / 1_000_000,
                "throughput_per_second": len(samples) / (elapsed_ns / 1_000_000_000),
                "latency_p50_ms": summarize(values, max(1, elapsed_ns))["p50_ms"] if values else None,
                "latency_p95_ms": summarize(values, max(1, elapsed_ns))["p95_ms"] if values else None,
                "raw_samples": samples,
                "cpu_percent_one_core_is_100": cpu_percent,
                "cuda_allocated_before_bytes": allocated_before,
                "cuda_allocated_after_bytes": allocated_after,
                "cuda_reserved_before_bytes": reserved_before,
                "cuda_reserved_after_bytes": reserved_after,
                "cuda_peak_allocated_bytes": peak_allocated,
                "cuda_peak_reserved_bytes": peak_reserved,
                "nvidia_smi_snapshot_before": system_before,
                "nvidia_smi_snapshot_after": nvidia_smi_snapshot(),
            }
        )
    return {
        "enabled": True,
        "method": "opt-in ThreadPoolExecutor calls sharing one Agent; each prediction synchronizes CUDA before and after",
        "thread_safety_note": "Concurrent safety is measured empirically; Laya does not promise arbitrary shared-Agent thread safety as a public contract.",
        "levels": results,
    }


def find_cpu_baseline() -> dict[str, Any] | None:
    paths = sorted(RESULTS_DIR.glob("laya_benchmark_*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    for path in paths:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            measurement = data.get("measurement", {})
            environment = data.get("environment", {})
            summary = measurement.get("warm_summary", {})
            if environment.get("backend") == "cpu" and summary:
                ram = data.get("resources", {}).get("ram", {})
                return {
                    "kind": "historical measured CPU result",
                    "source_file": str(path.relative_to(RESULTS_DIR.parent.parent.parent)),
                    "model": environment.get("model"),
                    "cold_start_ms": measurement.get("cold_start_ms"),
                    "warm_p50_ms": summary.get("p50_ms"),
                    "warm_p95_ms": summary.get("p95_ms"),
                    "warm_p99_ms": summary.get("p99_ms"),
                    "mean_ms": summary.get("mean_ms"),
                    "throughput_decisions_per_second": summary.get("decisions_per_second"),
                    "peak_ram_bytes": ram.get("peak_rss_bytes"),
                    "vram_bytes": None,
                }
        except (OSError, json.JSONDecodeError):
            continue
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES, help="measured sequential calls (minimum 100)")
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS, help="unmeasured warm-up calls (minimum 5)")
    parser.add_argument("--device", default="cuda", help="PyTorch device; defaults to cuda")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Laya model ID or local checkpoint directory")
    parser.add_argument("--output", type=Path, help="optional result JSON path")
    parser.add_argument("--stress", action="store_true", help="also run a separate 1/2/4/8-thread stress experiment")
    args = parser.parse_args()
    if args.samples < 100:
        parser.error("--samples must be at least 100")
    if args.warmups < 5:
        parser.error("--warmups must be at least 5")
    return args


def main() -> int:
    args = parse_args()
    ram_before = process_memory()
    gpu_system_before = nvidia_smi_snapshot()
    cold_started_ns = time.perf_counter_ns()

    import laya
    import torch

    captured_warnings: list[str] = []
    with warnings.catch_warnings(record=True) as observed:
        warnings.simplefilter("always")
        # One model load for the full process.
        agent = laya.load(args.model, device=args.device)
        cold_elapsed_ns = time.perf_counter_ns() - cold_started_ns
        if str(args.device).startswith("cuda") and (not torch.cuda.is_available() or agent.device.type != "cuda"):
            raise RuntimeError("CUDA was requested but Laya did not load on CUDA")

        warmup_samples = []
        for index in range(args.warmups):
            item = WORKLOAD[index % len(WORKLOAD)]
            if agent.device.type == "cuda":
                torch.cuda.synchronize(agent.device)
            started = time.perf_counter_ns()
            output = agent.predict({"request": item["request"]}, QUESTIONS, lang="en")
            if agent.device.type == "cuda":
                torch.cuda.synchronize(agent.device)
            ended = time.perf_counter_ns()
            warmup_samples.append(
                {
                    "warmup_index": index + 1,
                    "scenario_id": item["id"],
                    "latency_ms": (ended - started) / 1_000_000,
                    "decision": decision_summary(output),
                }
            )

        if agent.device.type == "cuda":
            torch.cuda.synchronize(agent.device)
            torch.cuda.reset_peak_memory_stats(agent.device)
            allocated_before = int(torch.cuda.memory_allocated(agent.device))
            reserved_before = int(torch.cuda.memory_reserved(agent.device))
            properties = torch.cuda.get_device_properties(agent.device)
            gpu_name = torch.cuda.get_device_name(agent.device)
            vram_total_bytes = int(properties.total_memory)
        else:
            allocated_before = reserved_before = vram_total_bytes = None
            gpu_name = None

        cpu_measure_before = process_cpu_seconds()
        cpu_wall_before_ns = time.perf_counter_ns()
        measured_samples = []
        measured_wall_before_ns = time.perf_counter_ns()
        for index in range(args.samples):
            item = WORKLOAD[index % len(WORKLOAD)]
            state = {"request": item["request"]}
            if agent.device.type == "cuda":
                torch.cuda.synchronize(agent.device)
            started = time.perf_counter_ns()
            output = agent.predict(state, QUESTIONS, lang="en")
            if agent.device.type == "cuda":
                torch.cuda.synchronize(agent.device)
            ended = time.perf_counter_ns()
            measured_samples.append(
                {
                    "sample_index": index + 1,
                    "scenario_id": item["id"],
                    "latency_ms": (ended - started) / 1_000_000,
                    "decision": decision_summary(output),
                }
            )
        measured_elapsed_ns = time.perf_counter_ns() - measured_wall_before_ns
        cpu_wall_elapsed_ns = time.perf_counter_ns() - cpu_wall_before_ns
        cpu_measure_after = process_cpu_seconds()

        if agent.device.type == "cuda":
            torch.cuda.synchronize(agent.device)
            allocated_after = int(torch.cuda.memory_allocated(agent.device))
            reserved_after = int(torch.cuda.memory_reserved(agent.device))
            peak_allocated = int(torch.cuda.max_memory_allocated(agent.device))
            peak_reserved = int(torch.cuda.max_memory_reserved(agent.device))
        else:
            allocated_after = reserved_after = peak_allocated = peak_reserved = None
        stress = run_stress(torch, agent) if args.stress else {"enabled": False}
        captured_warnings = [
            f"{warning.category.__name__}: {warning.message}" for warning in observed
        ]

    for warning in captured_warnings:
        print(f"WARNING: {warning}", file=sys.stderr)

    latencies = percentile_samples(measured_samples)
    summary = summarize(latencies, measured_elapsed_ns)
    cpu_percent = None
    if cpu_measure_before is not None and cpu_measure_after is not None and cpu_wall_elapsed_ns:
        cpu_percent = max(
            0.0,
            (cpu_measure_after - cpu_measure_before) * 1_000_000_000 / cpu_wall_elapsed_ns * 100,
        )
    ram_after = process_memory()
    gpu_system_after = nvidia_smi_snapshot()
    try:
        laya_version = importlib.metadata.version("laya")
    except importlib.metadata.PackageNotFoundError:
        laya_version = getattr(laya, "__version__", "unknown")
    result = {
        "benchmark": "wyvern-laya-cuda-sequential",
        "timestamp": dt.datetime.now().astimezone().isoformat(),
        "git_revision": git_revision(),
        "environment": {
            "os": platform.platform(),
            "python": sys.version,
            "pytorch": torch.__version__,
            "laya": laya_version,
            "cuda_version": torch.version.cuda,
            "cuda_available": bool(torch.cuda.is_available()),
            "gpu": gpu_name or (gpu_system_before["gpus"][0]["name"] if gpu_system_before and gpu_system_before["gpus"] else None),
            "model": args.model,
            "device": str(agent.device),
            "checkpoint_name": agent.cfg.get("model_name", "unknown"),
            "checkpoint_encoder": agent.cfg.get("encoder", "unknown"),
        },
        "methodology": {
            "api": "laya.load(model, device=device) once; Agent.predict(state, questions, lang='en') sequentially",
            "synchronize_before_and_after_each_prediction": str(agent.device.type) == "cuda",
            "samples": args.samples,
            "warmups": args.warmups,
            "latencies_include_python_api_preparation_and_decode": True,
            "percentile_method": "nearest rank",
            "standard_deviation_method": "population",
        },
        "cold_start": {"model_load_including_import_ms": cold_elapsed_ns / 1_000_000},
        "warm": {
            "summary": summary,
            "raw_samples": measured_samples,
            "warmup_samples": warmup_samples,
            "workload": WORKLOAD,
        },
        "memory": {
            "process_before_model_load": ram_before,
            "process_after_benchmark": ram_after,
            "process_cpu_percent_during_measured_loop": cpu_percent,
            "cpu_percent_note": "process CPU time divided by measured loop wall time; one logical core is 100%",
            "cuda_total_vram_bytes": vram_total_bytes,
            "cuda_allocated_before_measured_bytes": allocated_before,
            "cuda_allocated_after_measured_bytes": allocated_after,
            "cuda_reserved_before_measured_bytes": reserved_before,
            "cuda_reserved_after_measured_bytes": reserved_after,
            "cuda_peak_allocated_bytes": peak_allocated,
            "cuda_peak_reserved_bytes": peak_reserved,
            "nvidia_smi_before": gpu_system_before,
            "nvidia_smi_after": gpu_system_after,
            "nvidia_smi_utilization_note": "single system snapshots only; these are not peak utilization measurements",
        },
        "cpu_comparison": {
            "current_cuda_measurement": {
                "cold_start_ms": cold_elapsed_ns / 1_000_000,
                "warm_p50_ms": summary["p50_ms"],
                "warm_p95_ms": summary["p95_ms"],
                "warm_p99_ms": summary["p99_ms"],
                "mean_ms": summary["mean_ms"],
                "throughput_decisions_per_second": summary["decisions_per_second"],
                "peak_ram_bytes": ram_after.get("peak_rss_bytes") if ram_after else None,
                "vram_total_bytes": vram_total_bytes,
                "cuda_peak_allocated_bytes": peak_allocated,
            },
            "historical_cpu_baseline": find_cpu_baseline(),
        },
        "stress": stress,
        "warnings": captured_warnings,
        "limitations": [
            "Cold start includes Laya/PyTorch imports and model loading but excludes Python process creation.",
            "Per-call CUDA synchronization measures GPU completion, not only asynchronous dispatch time.",
            "NVIDIA utilization is sampled before and after; short-lived peaks are not captured.",
            "The optional stress mode shares one Agent across Python threads and is experimental; Laya's public API does not guarantee arbitrary shared-Agent concurrency safety.",
        ],
    }

    output_path = args.output or (RESULTS_DIR / "cuda" / f"laya_cuda_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print("=" * 44)
    print("     WYVERN LAYA CUDA BENCHMARK")
    print("=" * 44)
    print(f"Python: {platform.python_version()} | PyTorch: {torch.__version__} | Laya: {laya_version}")
    print(f"CUDA: {torch.version.cuda} | Available: {torch.cuda.is_available()} | GPU: {result['environment']['gpu']}")
    print(f"Model load: {cold_elapsed_ns / 1_000_000:.3f} ms | Device: {agent.device}")
    print(f"Samples: {args.samples} | Warmups: {args.warmups} | synchronized each call: {agent.device.type == 'cuda'}")
    for key in ("min_ms", "mean_ms", "median_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms", "stddev_ms"):
        print(f"{key}: {summary[key]:.3f} ms")
    print(f"Throughput: {summary['decisions_per_second']:.3f} decisions/sec")
    print(f"CUDA allocated before/after: {allocated_before} / {allocated_after} bytes")
    print(f"CUDA peak allocated/reserved: {peak_allocated} / {peak_reserved} bytes")
    print(f"CPU peak working set: {ram_after.get('peak_rss_bytes') if ram_after else 'unavailable'} bytes")
    baseline = result["cpu_comparison"]["historical_cpu_baseline"]
    print(f"Historical CPU baseline: {'available' if baseline else 'not found'}")
    print(f"Results: {output_path}")
    print("=" * 44)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
