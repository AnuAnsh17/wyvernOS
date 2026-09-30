#!/usr/bin/env python3
"""Measure direct, sequential Laya inference without including CLI startup."""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import importlib.metadata
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


RESULTS_DIR = Path(__file__).resolve().parent / "results"
DEFAULT_MODEL = "convaiinnovations/laya"
WARMUP_REQUESTS = 5
DEFAULT_SAMPLES = 100

# This fixed question set gives each representative Wyvern scenario the same
# inference path. Laya's public Agent.predict API accepts this structure.
QUESTIONS = {
    "next_action": {
        "type": "choice",
        "instructions": "What is the primary next action required by the Wyvern runtime in `request`?",
        "criteria": {
            "continue": "Continue the current runtime's work without waiting or spawning more work.",
            "wait": "Wait for currently running child runtimes or another required result.",
            "retry": "Retry a failed child runtime because its failure is retryable.",
            "schedule": "Start runnable work within the scheduler's available capacity.",
            "use_local_tool": "Use a local tool to inspect, modify, or validate source files.",
            "research": "Use external research tools and verify information from sources.",
            "browser": "Use a browser tool to interact with a page or submit information.",
            "decompose": "Split independent work into child runtimes before execution.",
        },
    },
    "requires_external_tools": {
        "type": "noul",
        "instructions": "Does the task in `request` require a tool outside the runtime's supplied context?",
    },
}

# Fixed, concise workload; each request is repeated in order when --samples
# exceeds this list. Keep requests independent of current date or external state.
WORKLOAD = [
    {
        "id": "runtime_wait_for_children",
        "request": "A runtime completed reconnaissance successfully. Two child runtimes are still running. Should the parent continue, wait, or spawn another runtime?",
    },
    {
        "id": "runtime_continue_after_children",
        "request": "All child runtimes completed successfully and returned the required structured results. The parent has work remaining. What should it do next?",
    },
    {
        "id": "retryable_child_failure",
        "request": "A child runtime failed with a retryable network timeout. Another child runtime is still running. Should the runtime retry, wait, or continue?",
    },
    {
        "id": "nonretryable_child_failure",
        "request": "A child runtime failed because its input was invalid and the error is not retryable. What should the parent do with that result?",
    },
    {
        "id": "schedule_with_partial_capacity",
        "request": "Five independent tasks are runnable and scheduler capacity is available for two. Determine the next scheduling action.",
    },
    {
        "id": "schedule_at_capacity",
        "request": "Four runtimes are already running and the configured concurrency limit is four. Additional work is queued. What should the scheduler do?",
    },
    {
        "id": "inspect_and_modify_source",
        "request": "The task requires inspecting a local Rust source file and then modifying it. Determine whether tool use is required.",
    },
    {
        "id": "review_supplied_source",
        "request": "The user supplied all relevant source text and asks for an explanation only, with no file changes. Is an external tool required?",
    },
    {
        "id": "verified_external_research",
        "request": "A research task requires current external information and verification against authoritative sources. Determine the next action.",
    },
    {
        "id": "answer_from_context",
        "request": "The answer is fully present in the runtime's supplied context, and the user asks for a short explanation. What should the runtime do?",
    },
    {
        "id": "browser_submit_information",
        "request": "The task requires interacting with a browser page and submitting information into a form. Determine whether tools are required.",
    },
    {
        "id": "browser_read_page",
        "request": "The task requires opening a web page and reading a value that is not included in the supplied context. What is required?",
    },
    {
        "id": "decompose_independent_modules",
        "request": "The user wants a Rust project implemented and the work can be divided into four independent modules. Determine the task characteristics.",
    },
    {
        "id": "small_single_runtime_change",
        "request": "The user requests one small, isolated fix in a single local file. The runtime can make the change directly. What is the next action?",
    },
    {
        "id": "explicit_context_propagation",
        "request": "A child runtime found a fact. Its parent needs the fact, but a sibling branch does not. What should happen before the sibling can see it?",
    },
    {
        "id": "timeout_frees_worker",
        "request": "A runtime exceeded its configured timeout while another runtime is queued. What should the scheduler do next?",
    },
]


def percentile(values: list[float], percent: float) -> float:
    """Return a deterministic nearest-rank percentile (1-based rank)."""
    ordered = sorted(values)
    rank = max(1, math.ceil((percent / 100.0) * len(ordered)))
    return ordered[rank - 1]


def summarize(values: list[float], elapsed_ns: int) -> dict[str, float | int]:
    return {
        "samples": len(values),
        "min_ms": min(values),
        "max_ms": max(values),
        "mean_ms": statistics.mean(values),
        "median_ms": statistics.median(values),
        "p50_ms": percentile(values, 50),
        "p90_ms": percentile(values, 90),
        "p95_ms": percentile(values, 95),
        "p99_ms": percentile(values, 99),
        "stddev_ms": statistics.pstdev(values),
        "elapsed_ms": elapsed_ns / 1_000_000,
        "decisions_per_second": len(values) / (elapsed_ns / 1_000_000_000),
    }


def _windows_process_memory() -> dict[str, int] | None:
    if os.name != "nt":
        return None
    from ctypes import wintypes

    class ProcessMemoryCountersEx(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    try:
        counters = ProcessMemoryCountersEx()
        counters.cb = ctypes.sizeof(counters)
        psapi = ctypes.WinDLL("Psapi.dll", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32.dll", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCountersEx),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        ok = psapi.GetProcessMemoryInfo(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        )
        if not ok:
            return None
        return {
            "rss_bytes": int(counters.WorkingSetSize),
            "peak_rss_bytes": int(counters.PeakWorkingSetSize),
            "private_bytes": int(counters.PrivateUsage),
        }
    except (AttributeError, OSError, TypeError):
        return None


def process_memory() -> dict[str, int] | None:
    windows = _windows_process_memory()
    if windows is not None:
        return windows
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        peak = int(usage.ru_maxrss * (1024 if sys.platform != "darwin" else 1))
        return {"rss_bytes": peak, "peak_rss_bytes": peak, "private_bytes": 0}
    except (ImportError, AttributeError, OSError):
        return None


def _filetime_seconds(value: Any) -> float:
    return ((int(value.dwHighDateTime) << 32) + int(value.dwLowDateTime)) / 10_000_000


def process_cpu_seconds() -> float | None:
    if os.name != "nt":
        try:
            import resource

            usage = resource.getrusage(resource.RUSAGE_SELF)
            return float(usage.ru_utime + usage.ru_stime)
        except (ImportError, AttributeError, OSError):
            return None
    try:
        from ctypes import wintypes

        class FileTime(ctypes.Structure):
            _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]

        creation, exit_time, kernel, user = FileTime(), FileTime(), FileTime(), FileTime()
        kernel32 = ctypes.WinDLL("kernel32.dll", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        kernel32.GetProcessTimes.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(FileTime),
            ctypes.POINTER(FileTime),
            ctypes.POINTER(FileTime),
            ctypes.POINTER(FileTime),
        ]
        kernel32.GetProcessTimes.restype = wintypes.BOOL
        ok = kernel32.GetProcessTimes(
            kernel32.GetCurrentProcess(),
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        )
        if not ok:
            return None
        return _filetime_seconds(kernel) + _filetime_seconds(user)
    except (AttributeError, OSError, TypeError):
        return None


def nvidia_smi_snapshot() -> dict[str, Any] | None:
    executable = shutil.which("nvidia-smi")
    if not executable:
        return None
    try:
        output = subprocess.run(
            [
                executable,
                "--query-gpu=name,memory.total,memory.used,memory.free,utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            check=True,
            text=True,
            timeout=3,
        ).stdout.strip()
        rows = []
        for line in output.splitlines():
            columns = [part.strip() for part in line.split(",")]
            if len(columns) != 5:
                continue
            rows.append(
                {
                    "name": columns[0],
                    "memory_total_mb": _number_or_none(columns[1]),
                    "memory_used_mb": _number_or_none(columns[2]),
                    "memory_free_mb": _number_or_none(columns[3]),
                    "utilization_percent": _number_or_none(columns[4]),
                }
            )
        return {"gpus": rows, "available": bool(rows)} if rows else None
    except (OSError, subprocess.SubprocessError):
        return None


def _number_or_none(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _run_inference(agent: Any, state: dict[str, str]) -> dict[str, Any]:
    """One direct API call, kept small so later concurrency work can reuse it."""
    return agent.predict(state, QUESTIONS, lang="en")


def decision_summary(result: dict[str, Any]) -> dict[str, Any]:
    answers = result.get("answers", result)
    summary: dict[str, Any] = {}
    if isinstance(answers, dict):
        for key, answer in answers.items():
            if not isinstance(answer, dict):
                continue
            summary[key] = {
                field: answer.get(field)
                for field in ("choice", "noul", "score", "confidence")
                if field in answer
            }
    return summary


def run_sequential(agent: Any, samples: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    warmups = []
    for index in range(WARMUP_REQUESTS):
        item = WORKLOAD[index % len(WORKLOAD)]
        state = {"request": item["request"]}
        started = time.perf_counter_ns()
        output = _run_inference(agent, state)
        ended = time.perf_counter_ns()
        warmups.append(
            {
                "warmup_index": index + 1,
                "scenario_id": item["id"],
                "latency_ms": (ended - started) / 1_000_000,
                "decision": decision_summary(output),
            }
        )

    measured: list[dict[str, Any]] = []
    started_all = time.perf_counter_ns()
    for index in range(samples):
        item = WORKLOAD[index % len(WORKLOAD)]
        state = {"request": item["request"]}
        started = time.perf_counter_ns()
        output = _run_inference(agent, state)
        ended = time.perf_counter_ns()
        measured.append(
            {
                "sample_index": index + 1,
                "scenario_id": item["id"],
                "latency_ms": (ended - started) / 1_000_000,
                "decision": decision_summary(output),
            }
        )
    elapsed = time.perf_counter_ns() - started_all
    return warmups, measured, elapsed


def format_ms(value: float | None) -> str:
    return "unavailable" if value is None else f"{value:.3f} ms"


def format_mb(value: int | float | None) -> str:
    return "unavailable" if value is None else f"{value / (1024 * 1024):.1f} MiB"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Laya checkpoint ID or local checkpoint path")
    parser.add_argument("--device", default=None, help="optional PyTorch device override; defaults to Laya auto-detection")
    parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES, help="sequential measured requests (minimum 100)")
    args = parser.parse_args()
    if args.samples < 100:
        parser.error("--samples must be at least 100")
    return args


def main() -> int:
    args = parse_args()
    ram_before = process_memory()
    gpu_before = nvidia_smi_snapshot()
    cold_started = time.perf_counter_ns()

    # Import and model initialization are deliberately timed separately from
    # warm calls. load() is called exactly once in this process.
    import laya
    import torch

    agent = laya.load(args.model, device=args.device)
    cold_start_ns = time.perf_counter_ns() - cold_started

    cuda_available = bool(torch.cuda.is_available())
    backend = str(agent.device)
    gpu_name = None
    torch_gpu_memory_before = None
    if cuda_available:
        gpu_name = torch.cuda.get_device_name(agent.device)
        if agent.device.type == "cuda":
            torch.cuda.synchronize(agent.device)
            torch.cuda.reset_peak_memory_stats(agent.device)
            torch_gpu_memory_before = {
                "allocated_bytes": int(torch.cuda.memory_allocated(agent.device)),
                "reserved_bytes": int(torch.cuda.memory_reserved(agent.device)),
            }
    if gpu_name is None and gpu_before and gpu_before["gpus"]:
        gpu_name = gpu_before["gpus"][0]["name"]

    cpu_before = process_cpu_seconds()
    cpu_wall_started = time.perf_counter_ns()
    warmups, measured, measured_elapsed_ns = run_sequential(agent, args.samples)
    cpu_wall_elapsed_ns = time.perf_counter_ns() - cpu_wall_started
    cpu_after = process_cpu_seconds()

    if cuda_available and agent.device.type == "cuda":
        torch.cuda.synchronize(agent.device)
    torch_gpu_memory_after = None
    torch_gpu_peak_bytes = None
    if cuda_available and agent.device.type == "cuda":
        torch_gpu_memory_after = {
            "allocated_bytes": int(torch.cuda.memory_allocated(agent.device)),
            "reserved_bytes": int(torch.cuda.memory_reserved(agent.device)),
        }
        torch_gpu_peak_bytes = int(torch.cuda.max_memory_allocated(agent.device))

    ram_after = process_memory()
    gpu_after = nvidia_smi_snapshot()
    latencies = [sample["latency_ms"] for sample in measured]
    stats = summarize(latencies, measured_elapsed_ns)
    cpu_percent = None
    if cpu_before is not None and cpu_after is not None and cpu_wall_elapsed_ns > 0:
        cpu_percent = max(0.0, (cpu_after - cpu_before) * 1_000_000_000 / cpu_wall_elapsed_ns * 100)

    # Laya uses PyTorch for inference. A CUDA backend means this process ran its
    # model on NVIDIA CUDA; a CPU backend means this benchmark did not use it.
    process_uses_nvidia_gpu: bool | None
    if agent.device.type == "cuda":
        process_uses_nvidia_gpu = True
    elif not cuda_available or agent.device.type == "cpu":
        process_uses_nvidia_gpu = False
    else:
        process_uses_nvidia_gpu = None

    gpu_report = {
        "cuda_available": cuda_available,
        "model_backend": backend,
        "gpu_name": gpu_name,
        "process_uses_nvidia_gpu": process_uses_nvidia_gpu,
        "system_before": gpu_before,
        "system_after": gpu_after,
        "torch_process_memory_before": torch_gpu_memory_before,
        "torch_process_memory_after": torch_gpu_memory_after,
        "torch_process_peak_allocated_bytes": torch_gpu_peak_bytes,
    }
    ram_report = {
        "before_model_load": ram_before,
        "after_benchmark": ram_after,
        "peak_rss_bytes": ram_after.get("peak_rss_bytes") if ram_after else None,
        "process_cpu_percent_during_warmup_and_measurement": cpu_percent,
        "cpu_measurement_note": "process CPU time divided by wall time; one fully used logical core is 100%",
    }

    try:
        laya_version = importlib.metadata.version("laya")
    except importlib.metadata.PackageNotFoundError:
        laya_version = getattr(laya, "__version__", "unknown")
    result = {
        "benchmark": "wyvern-laya-direct-inference",
        "created_at": dt.datetime.now().astimezone().isoformat(),
        "environment": {
            "os": platform.platform(),
            "python": sys.version,
            "laya_version": laya_version,
            "model": args.model,
            "checkpoint_name": agent.cfg.get("model_name", "unknown"),
            "checkpoint_encoder": agent.cfg.get("encoder", "unknown"),
            "checkpoint_max_len": agent.cfg.get("max_len"),
            "backend": backend,
        },
        "measurement": {
            "api": "laya.load(...).predict(state, questions, lang='en')",
            "mode": "single-process, single-model, sequential requests",
            "samples": args.samples,
            "warmup_requests": WARMUP_REQUESTS,
            "statistics_exclude_warmups": True,
            "percentile_method": "nearest rank",
            "standard_deviation_method": "population",
            "cold_start_ms": cold_start_ns / 1_000_000,
            "warm_summary": stats,
            "warmup_samples": warmups,
            "samples_raw": measured,
            "workload": WORKLOAD,
        },
        "resources": {"ram": ram_report, "gpu": gpu_report},
        "limitations": [
            "Inference latency includes Agent.predict tokenization, model inference, and decoding, but excludes Python process startup and one-time model loading.",
            "NVIDIA system utilization and memory are sampled before and after; short spikes may not be observed.",
            "CPU utilization is process CPU time over warm-up and measured calls, expressed relative to one logical core.",
            "This benchmark is sequential and does not measure concurrent inference or model quality.",
        ],
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    result_path = RESULTS_DIR / f"laya_benchmark_{stamp}.json"
    result_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print("=" * 40)
    print("        WYVERN LAYA BENCHMARK")
    print("=" * 40)
    print("\nEnvironment\n" + "-" * 40)
    print(f"OS: {platform.platform()}")
    print(f"Python: {platform.python_version()}")
    print(f"Laya version: {laya_version}")
    print(f"Model: {args.model}")
    print(f"Backend: {backend}")
    print(f"CUDA available: {cuda_available}")
    print(f"GPU: {gpu_name or 'unavailable'}")
    print("\nCold Start\n" + "-" * 40)
    print(f"Model load (including Laya/Torch import): {cold_start_ns / 1_000_000:.3f} ms")
    print("\nWarm Inference\n" + "-" * 40)
    print(f"Samples: {args.samples}")
    print(f"Warm-up discarded: {WARMUP_REQUESTS}")
    for key in ("min_ms", "mean_ms", "median_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms", "stddev_ms"):
        print(f"{key.removesuffix('_ms').replace('_', ' ').title()}: {stats[key]:.3f} ms")
    print(f"Throughput: {stats['decisions_per_second']:.3f} decisions/sec")
    print("\nResources\n" + "-" * 40)
    print(f"RAM before model load: {format_mb(ram_before.get('rss_bytes') if ram_before else None)}")
    print(f"RAM after benchmark: {format_mb(ram_after.get('rss_bytes') if ram_after else None)}")
    print(f"Peak process RAM: {format_mb(ram_report['peak_rss_bytes'])}")
    print(f"Process CPU: {('unavailable' if cpu_percent is None else f'{cpu_percent:.1f}%')}")
    print(f"NVIDIA GPU used by this process: {process_uses_nvidia_gpu if process_uses_nvidia_gpu is not None else 'unavailable'}")
    gpu_before_text = gpu_before["gpus"][0] if gpu_before and gpu_before["gpus"] else None
    gpu_after_text = gpu_after["gpus"][0] if gpu_after and gpu_after["gpus"] else None
    print(f"GPU memory before (system used): {('unavailable' if not gpu_before_text else str(gpu_before_text['memory_used_mb']) + ' MiB')}")
    print(f"GPU memory after (system used): {('unavailable' if not gpu_after_text else str(gpu_after_text['memory_used_mb']) + ' MiB')}")
    print(f"GPU utilization before/after: {('unavailable' if not gpu_before_text or not gpu_after_text else str(gpu_before_text['utilization_percent']) + '% / ' + str(gpu_after_text['utilization_percent']) + '%')}")
    print(f"Results: {result_path}")
    print("=" * 40)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
