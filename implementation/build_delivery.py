from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REQUIRED = {
    "README.txt", "change_request.txt", "dag_schedule.csv", "dependency_plan.csv",
    "release_request.json", "starter/day_cut.py",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def parse_bool(value: str) -> bool:
    if value.lower() not in {"true", "false"}:
        raise ValueError("布尔字段格式不正确")
    return value.lower() == "true"


def quoted(value: str) -> str:
    return repr(value)


def build_dag_source(schedule: dict[str, str], tasks: list[str], dependency: dict[str, str] | None) -> str:
    imports = ["from datetime import datetime, timedelta", "", "from airflow import DAG", "from airflow.operators.empty import EmptyOperator"]
    if dependency:
        imports.append("from airflow.sensors.external_task import ExternalTaskSensor")
    imports.extend(["from airflow.utils.timezone import make_aware", "", ""])
    lines = imports + [
        "with DAG(",
        f"    dag_id={quoted(schedule['dag_id'])},",
        f"    start_date=datetime.fromisoformat({quoted(schedule['start_date'].replace('Z', '+00:00'))}),",
        f"    schedule={quoted(schedule['schedule'])},",
        f"    catchup={parse_bool(schedule['catchup'])},",
        f"    max_active_runs={int(schedule['max_active_runs'])},",
        f"    default_args={{'owner': {quoted(schedule['owner'])}}},",
        ") as dag:",
    ]
    variables: list[str] = []
    for task_id in tasks:
        variable = task_id
        if dependency and task_id == dependency["sensor_task_id"]:
            variables.append(variable)
            lines.extend([
                f"    {variable} = ExternalTaskSensor(",
                f"        task_id={quoted(task_id)},",
                f"        external_dag_id={quoted(dependency['producer_dag'])},",
                f"        external_task_id={quoted(dependency['producer_task_id'])},",
                f"        execution_delta=timedelta(minutes={int(dependency['execution_delta_minutes'])}),",
                f"        allowed_states={repr(dependency['allowed_states'].split('|'))},",
                f"        failed_states={repr(dependency['failed_states'].split('|'))},",
                f"        skipped_states={repr(dependency['skipped_states'].split('|'))},",
                f"        mode={quoted(dependency['mode'])},",
                f"        poll_interval={int(dependency['poll_interval_seconds'])},",
                f"        timeout={int(dependency['timeout_seconds'])},",
                f"        check_existence={parse_bool(dependency['check_existence'])},",
                f"        soft_fail={parse_bool(dependency['soft_fail'])},",
                f"        deferrable={parse_bool(dependency['deferrable'])},",
                "    )",
            ])
        else:
            variables.append(variable)
            lines.append(f"    {variable} = EmptyOperator(task_id={quoted(task_id)})")
    lines.extend(["", "    " + " >> ".join(variables), ""])
    return "\n".join(lines)


def run_probe(dags_dir: Path) -> dict[str, object]:
    home = Path(tempfile.mkdtemp(prefix="airflow_day_cut_"))
    env = os.environ.copy()
    env.update({
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
        "AIRFLOW__CORE__DAGS_FOLDER": str(dags_dir),
    })
    completed = subprocess.run(
        [sys.executable, str(ROOT / "probe_dags.py"), "--dags", str(dags_dir)],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=300,
        env=env,
    )
    shutil.rmtree(home, ignore_errors=True)
    if completed.returncode:
        raise RuntimeError(completed.stdout + completed.stderr)
    marker = next((line for line in reversed(completed.stdout.splitlines()) if line.startswith("DAY_CUT_RESULT=")), None)
    if marker is None:
        raise RuntimeError("DagBag没有返回结构结果")
    return json.loads(marker.split("=", 1)[1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    input_dir = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    try:
        present = {path.relative_to(input_dir).as_posix() for path in input_dir.rglob("*") if path.is_file()}
        if present != REQUIRED:
            raise ValueError("日切材料集合发生变化")
        schedules = read_csv(input_dir / "dag_schedule.csv")
        dependencies = read_csv(input_dir / "dependency_plan.csv")
        request = json.loads((input_dir / "release_request.json").read_text(encoding="utf-8"))
        if not (input_dir / "change_request.txt").read_text(encoding="utf-8").strip():
            raise ValueError("岗位说明为空")
        starter = (input_dir / "starter/day_cut.py").read_text(encoding="utf-8")
        if "feature_day_cut_draft" not in starter:
            raise ValueError("当前草稿身份缺失")
        expected_dags = set(request["dags"])
        if len(schedules) != len(expected_dags) or {row["dag_id"] for row in schedules} != expected_dags:
            raise ValueError("日程与申请DAG不一致")
        if len(dependencies) != 2 or len({(row["consumer_dag"], row["sensor_task_id"]) for row in dependencies}) != 2:
            raise ValueError("跨DAG依赖缺失或重复")
        if set(request["report_files"]) != {"dag_inventory.csv", "dependency_inventory.csv", "release_sequence.csv", "release_handoff.csv"}:
            raise ValueError("结果文件申请发生变化")
        release_fields = ["release_window", "affected_dags", "wait_budget_seconds", "rollout_mode", "rollback_condition", "observation_metrics"]
        if any(field not in request for field in release_fields) or not isinstance(request["wait_budget_seconds"], int) or request["wait_budget_seconds"] < 1 or not request["observation_metrics"]:
            raise ValueError("发布安排不完整")
        if set(request["affected_dags"]) != expected_dags:
            raise ValueError("发布影响范围与DAG身份不一致")
        for row in schedules:
            if row["timezone"] != "UTC" or int(row["max_active_runs"]) < 1:
                raise ValueError("日程属性不受支持")
            datetime.fromisoformat(row["start_date"].replace("Z", "+00:00"))
        for row in dependencies:
            if row["consumer_dag"] not in expected_dags or row["producer_dag"] not in expected_dags:
                raise ValueError("依赖引用未知DAG")
            if row["sensor_task_id"] not in request["dags"][row["consumer_dag"]] or row["producer_task_id"] not in request["dags"][row["producer_dag"]]:
                raise ValueError("依赖引用未知任务")

        output_dir.mkdir(parents=True)
        dags_dir = output_dir / "dags"
        results_dir = output_dir / "results"
        dags_dir.mkdir()
        results_dir.mkdir()
        dependency_by_consumer = {row["consumer_dag"]: row for row in dependencies}
        for schedule in schedules:
            source = build_dag_source(schedule, request["dags"][schedule["dag_id"]], dependency_by_consumer.get(schedule["dag_id"]))
            (dags_dir / f"{schedule['dag_id']}.py").write_text(source, encoding="utf-8")
        observed = run_probe(dags_dir)
        if observed["import_errors"]:
            raise ValueError("DAG导入失败")
        dag_rows = observed["dag_inventory"]
        dep_rows = observed["dependency_inventory"]
        write_csv(results_dir / "dag_inventory.csv", ["dag_id", "task_id", "task_type", "schedule", "timezone", "catchup", "max_active_runs", "owner", "upstream_task_ids", "downstream_task_ids"], dag_rows)
        write_csv(results_dir / "dependency_inventory.csv", ["consumer_dag", "sensor_task_id", "producer_dag", "producer_task_id", "execution_delta_minutes", "allowed_states", "failed_states", "skipped_states", "mode", "poll_interval_seconds", "timeout_seconds", "check_existence", "soft_fail", "deferrable"], dep_rows)
        schedule_by_id = {row["dag_id"]: row for row in schedules}
        reference_date = datetime.fromisoformat(request["reference_run_date"]).date()
        sequence = []
        for dep in dep_rows:
            consumer_schedule = schedule_by_id[dep["consumer_dag"]]["schedule"].split()
            consumer_time = datetime(reference_date.year, reference_date.month, reference_date.day, int(consumer_schedule[1]), int(consumer_schedule[0]), tzinfo=timezone.utc)
            producer_time = consumer_time - timedelta(minutes=int(dep["execution_delta_minutes"]))
            sequence.append({
                "consumer_dag": dep["consumer_dag"], "sensor_task_id": dep["sensor_task_id"], "consumer_logical_time": consumer_time.isoformat().replace("+00:00", "Z"),
                "producer_dag": dep["producer_dag"], "producer_task_id": dep["producer_task_id"], "producer_logical_time": producer_time.isoformat().replace("+00:00", "Z"),
                "execution_delta_minutes": dep["execution_delta_minutes"],
            })
        write_csv(results_dir / "release_sequence.csv", ["consumer_dag", "sensor_task_id", "consumer_logical_time", "producer_dag", "producer_task_id", "producer_logical_time", "execution_delta_minutes"], sequence)
        write_csv(results_dir / "release_handoff.csv", ["release_window", "affected_dags", "wait_budget_seconds", "rollout_mode", "rollback_condition", "observation_metrics"], [{
            "release_window": request["release_window"], "affected_dags": "|".join(request["affected_dags"]), "wait_budget_seconds": request["wait_budget_seconds"],
            "rollout_mode": request["rollout_mode"], "rollback_condition": request["rollback_condition"], "observation_metrics": "|".join(request["observation_metrics"]),
        }])
        (output_dir / "README.txt").write_text(
            f"这份日切材料交给特征平台版本负责人。dags目录是待进入版本库的DAG源码，dag_inventory.csv记录任务图，dependency_inventory.csv记录跨DAG等待属性，release_sequence.csv供发布值班定位上游逻辑日期。\n\nrelease_handoff.csv记录变更窗口、影响范围、{request['wait_budget_seconds']}秒等待预算、{request['rollout_mode']}启用方式、回滚条件和观察指标。出现DAG导入错误或Sensor超时时恢复旧版本，补跑和历史清理另行处理。\n",
            encoding="utf-8",
        )
    except Exception:
        if output_dir.exists():
            shutil.rmtree(output_dir)
        raise


if __name__ == "__main__":
    main()
