#!/usr/bin/env python3
"""Evaluate Laya decisions against a curated Wyvern control-plane scenario set."""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.metadata
import json
import math
import platform
import subprocess
import sys
import time
import warnings
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from benchmark import nvidia_smi_snapshot, process_cpu_seconds, process_memory


HERE = Path(__file__).resolve().parent
DEFAULT_SCENARIOS = HERE / "scenarios" / "wyvern_decisions_v1.json"
RESULTS_DIR = HERE / "results" / "quality"
DEFAULT_MODEL = "convaiinnovations/laya"
DEFAULT_DEVICE = "cuda"
WARMUPS = 5

QUESTION_SPECS: dict[str, dict[str, Any]] = {
    "routing": {
        "type": "choice",
        "instructions": "Which runtime/domain should own the task in `request`?",
        "criteria": {
            "CODE_RUNTIME": "Implementing or changing software, code, tests, or architecture.",
            "DEBUG_RUNTIME": "Diagnosing a failure, crash, or unexpected behavior.",
            "RESEARCH_RUNTIME": "Finding and verifying information from external sources.",
            "TERMINAL_RUNTIME": "Running local commands or processes.",
            "FILESYSTEM_RUNTIME": "Finding, reading, moving, or inspecting local files.",
            "BROWSER_RUNTIME": "Navigating or interacting with a web page.",
            "SECURITY_RUNTIME": "Reviewing security, authorization, or vulnerability behavior.",
            "DATA_ANALYSIS_RUNTIME": "Analyzing structured data, metrics, or datasets.",
            "DOCUMENTATION_RUNTIME": "Writing or revising explanatory project documentation.",
            "GENERAL_REASONING_RUNTIME": "Reasoning over supplied constraints without a specialized activity.",
        },
    },
    "execution_strategy": {
        "type": "choice",
        "instructions": "Choose the safest efficient execution strategy for `request`.",
        "criteria": {
            "DIRECT_EXECUTION": "Perform a small, clear task directly.",
            "PLAN_FIRST": "Plan dependencies and validation before starting implementation.",
            "DELEGATE": "Assign a bounded task to another suitable runtime.",
            "PARALLELIZE": "Run independent tasks concurrently with clear ownership.",
            "SEQUENTIAL": "Perform dependent steps in order, using each result in the next step.",
        },
    },
    "runtime_lifecycle": {
        "type": "choice",
        "instructions": "What lifecycle action should the runtime take next for `request`?",
        "criteria": {
            "SPAWN_RUNTIME": "Create a child runtime for approved, bounded additional work.",
            "CONTINUE_RUNTIME": "Continue the current runtime's remaining objective.",
            "PAUSE_RUNTIME": "Wait for a required child, event, or result and release the worker slot.",
            "TERMINATE_RUNTIME": "End this runtime because it is complete or explicitly cancelled.",
            "REPLAN": "Revise the execution plan because new evidence invalidated it.",
        },
    },
    "control_escalation": {
        "type": "choice",
        "instructions": "Choose the appropriate control or escalation action for `request`.",
        "criteria": {
            "EXECUTE": "Proceed with an authorized, bounded, reversible action.",
            "ASK_USER": "Ask the user to resolve material ambiguity or supply missing intent.",
            "REQUIRE_APPROVAL": "Obtain explicit approval before an external or irreversible action.",
            "ESCALATE": "Refer a serious incident or policy conflict to its responsible owner.",
            "REFUSE": "Decline to assist with harmful or unauthorized activity.",
        },
    },
    "failure_recovery": {
        "type": "choice",
        "instructions": "Choose the safest recovery action after the failure described in `request`.",
        "criteria": {
            "RETRY": "Retry a transient operation that is safe to repeat.",
            "REPLAN": "Change the approach because the current plan is infeasible.",
            "ROLLBACK": "Restore a known-good prior state after a reversible failed change.",
            "DELEGATE": "Assign the remaining bounded diagnosis to a suitable specialist runtime.",
            "ASK_USER": "Ask for missing target or intent that cannot be inferred safely.",
            "ABORT": "Stop because continuation is cancelled or cannot be safe or reliable.",
        },
    },
    "verification": {
        "type": "choice",
        "instructions": "What should happen to verify or disposition the result in `request`?",
        "criteria": {
            "VERIFY": "Run the relevant check because the result is not yet verified.",
            "ACCEPT": "Accept the result because its stated requirements and checks passed.",
            "REJECT": "Reject the result because it violates a requirement or contradicts evidence.",
            "RETRY": "Repeat a safe check after correcting its test setup or transient failure.",
            "ESCALATE": "Stop and notify the responsible owner about a serious or unauthorized blocker.",
        },
    },
    "complexity": {
        "type": "score",
        "instructions": "Rate the implementation or reasoning complexity of `request` from 0 to 4.",
        "criteria": [
            "0: trivial lookup or single obvious text change with no dependencies.",
            "1: simple bounded task in one area with clear behavior.",
            "2: moderate task requiring several steps or focused validation.",
            "3: complex task with interacting modules, state, compatibility, or concurrency.",
            "4: very complex task combining uncertain diagnosis and broad security or reliability constraints.",
        ],
    },
    "requires_delegation": {
        "type": "noul",
        "instructions": "Does `request` benefit from assigning an independent bounded task to another runtime?",
    },
    "requires_user_approval": {
        "type": "noul",
        "instructions": "Does the action in `request` require explicit user approval before proceeding?",
    },
    "should_parallelize": {
        "type": "noul",
        "instructions": "Are the tasks in `request` independent enough to run concurrently safely?",
    },
    "verification_required": {
        "type": "noul",
        "instructions": "Does `request` require an implementation or persistence check before claiming success?",
    },
}


def git_revision() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=HERE.parents[1],
            capture_output=True,
            check=True,
            text=True,
            timeout=3,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def load_scenarios(path: Path) -> list[dict[str, Any]]:
    scenarios = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("scenario file must contain a non-empty JSON array")
    seen = set()
    for scenario in scenarios:
        scenario_id = scenario.get("id")
        if not scenario_id or scenario_id in seen:
            raise ValueError(f"scenario IDs must be present and unique: {scenario_id!r}")
        seen.add(scenario_id)
        input_data = scenario.get("input", {})
        question_id = input_data.get("question_id")
        if question_id not in QUESTION_SPECS:
            raise ValueError(f"unknown question_id {question_id!r} in scenario {scenario_id}")
        if "state" not in input_data or "expected" not in scenario.get("ground_truth", {}):
            raise ValueError(f"scenario {scenario_id} needs input.state and ground_truth.expected")
    return scenarios


def class_label(question_type: str, predicted: Any) -> str:
    if question_type == "noul":
        return "true" if bool(predicted) else "false"
    if question_type == "score":
        return str(int(round(float(predicted))))
    return str(predicted)


def f1_metrics(records: list[dict[str, Any]], question_id: str) -> dict[str, Any]:
    subset = [record for record in records if record["question_id"] == question_id]
    labels = sorted({class_label(record["question_type"], record["expected"]) for record in subset} |
                    {class_label(record["question_type"], record["predicted"]) for record in subset})
    matrix = {expected: {predicted: 0 for predicted in labels} for expected in labels}
    per_option = {}
    f1s = []
    for record in subset:
        expected = class_label(record["question_type"], record["expected"])
        predicted = class_label(record["question_type"], record["predicted"])
        matrix[expected][predicted] += 1
    for label in labels:
        tp = matrix[label][label]
        fp = sum(matrix[other][label] for other in labels if other != label)
        fn = sum(matrix[label][other] for other in labels if other != label)
        support = tp + fn
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / support if support else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_option[label] = {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "support": support,
            "predicted_count": tp + fp,
        }
        f1s.append(f1)
    return {
        "count": len(subset),
        "accuracy": sum(record["correct"] for record in subset) / len(subset) if subset else None,
        "options": per_option,
        "macro_f1": sum(f1s) / len(f1s) if f1s else None,
        "confusion_matrix": matrix,
    }


def score_metrics(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    subset = [record for record in records if record["question_type"] == "score"]
    if not subset:
        return None
    errors = [float(record["predicted"]) - float(record["expected"]) for record in subset]
    absolute = [abs(error) for error in errors]
    return {
        "count": len(subset),
        "mean_absolute_error": sum(absolute) / len(absolute),
        "root_mean_squared_error": math.sqrt(sum(error * error for error in errors) / len(errors)),
        "mean_signed_error": sum(errors) / len(errors),
        "max_absolute_error": max(absolute),
        "rounded_class_accuracy": sum(record["correct"] for record in subset) / len(subset),
    }


def calibration(records: list[dict[str, Any]], confidence_field: str) -> dict[str, Any]:
    subset = [
        record for record in records
        if record["question_type"] in ("choice", "noul") and record.get(confidence_field) is not None
    ]
    buckets = []
    ece = 0.0
    for index in range(5):
        lower = index * 0.2
        upper = (index + 1) * 0.2
        members = [
            record for record in subset
            if lower <= float(record[confidence_field]) < upper
            or (index == 4 and float(record[confidence_field]) == 1.0)
        ]
        mean_confidence = sum(float(row[confidence_field]) for row in members) / len(members) if members else None
        accuracy = sum(bool(row["correct"]) for row in members) / len(members) if members else None
        if members:
            ece += len(members) / len(subset) * abs(mean_confidence - accuracy)
        buckets.append(
            {
                "interval": f"{lower:.1f}-{upper:.1f}" + (" inclusive" if index == 4 else " exclusive upper"),
                "sample_count": len(members),
                "accuracy": accuracy,
                "mean_confidence": mean_confidence,
                "actual_correctness_rate": accuracy,
            }
        )
    return {
        "confidence_field": confidence_field,
        "evaluated_question_types": ["choice", "noul"],
        "sample_count": len(subset),
        "expected_calibration_error": ece if subset else None,
        "buckets": buckets,
    }


def confidence_summary(records: list[dict[str, Any]], field: str) -> dict[str, Any]:
    values = [float(record[field]) for record in records if record.get(field) is not None]
    if not values:
        return {"count": 0, "mean": None, "min": None, "median": None, "max": None}
    ordered = sorted(values)
    middle = len(ordered) // 2
    median = ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2
    return {
        "count": len(values),
        "mean": sum(values) / len(values),
        "min": min(values),
        "median": median,
        "max": max(values),
    }


def quality_metrics(records: list[dict[str, Any]]) -> dict[str, Any]:
    by_question = defaultdict(list)
    by_category = defaultdict(list)
    for record in records:
        by_question[record["question_id"]].append(record)
        by_category[record["category"]].append(record)
    classification_metrics = {
        question_id: f1_metrics(records, question_id)
        for question_id in sorted(by_question)
        if QUESTION_SPECS[question_id]["type"] in ("choice", "noul")
    }
    category_accuracy = {
        category: {
            "count": len(rows),
            "accuracy": sum(row["correct"] for row in rows) / len(rows),
        }
        for category, rows in sorted(by_category.items())
    }
    classifiable = [row for row in records if row["question_type"] in ("choice", "noul")]
    brier_by_question = {}
    for question_id, rows in by_question.items():
        if QUESTION_SPECS[question_id]["type"] not in ("choice", "noul"):
            continue
        scores = []
        for row in rows:
            probs = row.get("probabilities", {})
            if not probs:
                continue
            if row["question_type"] == "noul":
                p_true = float(probs.get("true", 0.0))
                expected_true = 1.0 if bool(row["expected"]) else 0.0
                scores.append((p_true - expected_true) ** 2)
            else:
                labels = QUESTION_SPECS[question_id]["criteria"].keys()
                scores.append(
                    sum((float(probs.get(label, 0.0)) - (1.0 if label == row["expected"] else 0.0)) ** 2 for label in labels)
                )
        brier_by_question[question_id] = {
            "count": len(scores),
            "brier_score": sum(scores) / len(scores) if scores else None,
            "method": "binary squared probability error for noul; sum of squared class-probability errors for choice",
        }
    all_correct = [bool(row["correct"]) for row in records]
    return {
        "scenario_count": len(records),
        "overall_accuracy": sum(all_correct) / len(all_correct) if all_correct else None,
        "accuracy_by_category": category_accuracy,
        "classification_by_question": classification_metrics,
        "score_error": score_metrics(records),
        "brier_by_question": brier_by_question,
        "confidence_distribution": {
            "confidence": confidence_summary(records, "confidence"),
            "answer_confidence": confidence_summary(records, "answer_confidence"),
        },
        "calibration": {
            "confidence": calibration(records, "confidence"),
            "answer_confidence": calibration(records, "answer_confidence"),
            "note": "Confidence calibration uses Laya's reported field for categorical choice/noul predictions; score questions are excluded because their confidence is not correctness probability.",
        },
        "calibration_sample_count": len(classifiable),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Laya model ID or local checkpoint directory")
    parser.add_argument("--device", default=DEFAULT_DEVICE, help="PyTorch device (default: cuda)")
    parser.add_argument("--scenarios", type=Path, default=DEFAULT_SCENARIOS, help="scenario JSON file")
    parser.add_argument("--output", type=Path, help="optional result JSON path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scenarios = load_scenarios(args.scenarios)
    ram_before = process_memory()
    gpu_before = nvidia_smi_snapshot()
    cold_started = time.perf_counter_ns()

    import laya
    import torch

    captured_warnings = []
    with warnings.catch_warnings(record=True) as observed:
        warnings.simplefilter("always")
        agent = laya.load(args.model, device=args.device)
        cold_start_ns = time.perf_counter_ns() - cold_started
        if str(args.device).startswith("cuda") and (not torch.cuda.is_available() or agent.device.type != "cuda"):
            raise RuntimeError("CUDA was requested but Laya did not load on CUDA")

        warmups = []
        for index in range(WARMUPS):
            scenario = scenarios[index % len(scenarios)]
            question_id = scenario["input"]["question_id"]
            if agent.device.type == "cuda":
                torch.cuda.synchronize(agent.device)
            started = time.perf_counter_ns()
            agent.predict(scenario["input"]["state"], {question_id: QUESTION_SPECS[question_id]}, lang="en")
            if agent.device.type == "cuda":
                torch.cuda.synchronize(agent.device)
            warmups.append((time.perf_counter_ns() - started) / 1_000_000)

        if agent.device.type == "cuda":
            torch.cuda.synchronize(agent.device)
            torch.cuda.reset_peak_memory_stats(agent.device)
            cuda_before = {
                "allocated_bytes": int(torch.cuda.memory_allocated(agent.device)),
                "reserved_bytes": int(torch.cuda.memory_reserved(agent.device)),
                "total_bytes": int(torch.cuda.get_device_properties(agent.device).total_memory),
                "gpu_name": torch.cuda.get_device_name(agent.device),
            }
        else:
            cuda_before = None

        predictions = []
        cpu_before = process_cpu_seconds()
        cpu_wall_before = time.perf_counter_ns()
        for scenario in scenarios:
            question_id = scenario["input"]["question_id"]
            question = QUESTION_SPECS[question_id]
            question_type = question["type"]
            expected = scenario["ground_truth"]["expected"]
            state = scenario["input"]["state"]  # rationale/expected metadata never enters inference
            if agent.device.type == "cuda":
                torch.cuda.synchronize(agent.device)
            started = time.perf_counter_ns()
            output = agent.predict(state, {question_id: question}, lang="en")
            if agent.device.type == "cuda":
                torch.cuda.synchronize(agent.device)
            elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000

            answers = output.get("answers", output)
            answer = answers.get(question_id)
            if not isinstance(answer, dict):
                raise RuntimeError(f"Laya returned no answer for scenario {scenario['id']}: {output!r}")
            raw_probabilities = answer.get("probabilities") or {}
            probabilities: dict[str, float]
            if question_type == "choice":
                probabilities = {str(key): float(value) for key, value in raw_probabilities.items()}
                predicted: Any = answer.get("choice")
                if predicted is None:
                    raise RuntimeError(f"Laya returned no choice for scenario {scenario['id']}")
                correct = predicted == expected
            elif question_type == "score":
                probabilities = {str(key): float(value) for key, value in raw_probabilities.items()}
                predicted = float(answer["score"])
                correct = abs(predicted - float(expected)) <= 0.5
            else:
                positive_probability = float(answer["noul"])
                probabilities = {"false": 1.0 - positive_probability, "true": positive_probability}
                predicted = positive_probability >= 0.5
                correct = bool(predicted) == bool(expected)

            predictions.append(
                {
                    "scenario_id": scenario["id"],
                    "category": scenario["category"],
                    "question_id": question_id,
                    "question_type": question_type,
                    "expected": expected,
                    "predicted": predicted,
                    "probabilities": probabilities,
                    "confidence": answer.get("confidence"),
                    "answer_confidence": answer.get("answer_confidence"),
                    "latency_ms": elapsed_ms,
                    "correct": bool(correct),
                }
            )
        cpu_elapsed_ns = time.perf_counter_ns() - cpu_wall_before
        cpu_after = process_cpu_seconds()

        if agent.device.type == "cuda":
            torch.cuda.synchronize(agent.device)
            cuda_after = {
                "allocated_bytes": int(torch.cuda.memory_allocated(agent.device)),
                "reserved_bytes": int(torch.cuda.memory_reserved(agent.device)),
                "peak_allocated_bytes": int(torch.cuda.max_memory_allocated(agent.device)),
                "peak_reserved_bytes": int(torch.cuda.max_memory_reserved(agent.device)),
            }
        else:
            cuda_after = None
        captured_warnings = [f"{item.category.__name__}: {item.message}" for item in observed]

    for warning in captured_warnings:
        print(f"WARNING: {warning}", file=sys.stderr)

    metrics = quality_metrics(predictions)
    cpu_percent = None
    if cpu_before is not None and cpu_after is not None and cpu_elapsed_ns:
        cpu_percent = max(0.0, (cpu_after - cpu_before) * 1_000_000_000 / cpu_elapsed_ns * 100)
    ram_after = process_memory()
    gpu_after = nvidia_smi_snapshot()
    try:
        laya_version = importlib.metadata.version("laya")
    except importlib.metadata.PackageNotFoundError:
        laya_version = getattr(laya, "__version__", "unknown")
    result = {
        "benchmark": "wyvern-laya-decision-quality-v1",
        "timestamp": dt.datetime.now().astimezone().isoformat(),
        "git_revision": git_revision(),
        "environment": {
            "os": platform.platform(),
            "python": sys.version,
            "pytorch": torch.__version__,
            "laya": laya_version,
            "cuda_version": torch.version.cuda,
            "cuda_available": bool(torch.cuda.is_available()),
            "gpu": cuda_before["gpu_name"] if cuda_before else None,
            "model": args.model,
            "device": str(agent.device),
            "checkpoint_name": agent.cfg.get("model_name", "unknown"),
            "checkpoint_encoder": agent.cfg.get("encoder", "unknown"),
        },
        "dataset": {
            "path": str(args.scenarios),
            "scenario_count": len(scenarios),
            "category_counts": dict(Counter(scenario["category"] for scenario in scenarios)),
            "warmup_requests": WARMUPS,
            "warmup_latencies_ms_excluded_from_metrics": warmups,
            "question_definitions": QUESTION_SPECS,
            "note": "Only input.state is passed to Laya. Ground truth and metadata.rationale are used only during evaluation.",
        },
        "metrics": metrics,
        "predictions": predictions,
        "resources": {
            "process_before_model_load": ram_before,
            "process_after_benchmark": ram_after,
            "process_cpu_percent_during_scenario_predictions": cpu_percent,
            "cpu_percent_note": "process CPU time divided by scenario loop wall time; one logical core is 100%",
            "cuda_before_scenarios": cuda_before,
            "cuda_after_scenarios": cuda_after,
            "nvidia_smi_before": gpu_before,
            "nvidia_smi_after": gpu_after,
            "nvidia_smi_utilization_note": "system snapshots, not peak GPU utilization",
        },
        "warnings": captured_warnings,
        "limitations": [
            "The curated 104-scenario v1 dataset is a small evaluation set, not a representative production distribution.",
            "Accuracy depends on the written labels and scenario scope; this is not a substitute for human review or broader evaluation.",
            "Choice confidence is evaluated as reported confidence and answer_confidence against exact correctness; score-question confidence is excluded from calibration metrics.",
            "The checkpoint emits a temperature-range warning for its choice:11+ bucket and clamps that value; this benchmark does not change the checkpoint.",
        ],
    }

    output_path = args.output or (RESULTS_DIR / f"laya_quality_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print("=" * 44)
    print("   WYVERN DECISION QUALITY BENCHMARK v1")
    print("=" * 44)
    print(f"Scenarios: {len(scenarios)} | Device: {agent.device} | Laya: {laya_version}")
    print(f"Overall accuracy: {metrics['overall_accuracy']:.3f}")
    for category, summary in metrics["accuracy_by_category"].items():
        print(f"{category}: {summary['accuracy']:.3f} ({summary['count']})")
    if metrics["score_error"]:
        print(f"Complexity score MAE: {metrics['score_error']['mean_absolute_error']:.3f}")
    for name in ("confidence", "answer_confidence"):
        calibration_metrics = metrics["calibration"][name]
        print(f"{name} ECE: {calibration_metrics['expected_calibration_error']}")
    print(f"Results: {output_path}")
    print("=" * 44)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
