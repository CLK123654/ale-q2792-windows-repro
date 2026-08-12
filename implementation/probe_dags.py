from __future__ import annotations

import argparse
import json
from pathlib import Path

from airflow.models import DagBag
from airflow.sensors.external_task import ExternalTaskSensor


def text_bool(value: bool) -> str:
    return str(bool(value)).lower()


parser = argparse.ArgumentParser()
parser.add_argument("--dags", required=True)
args = parser.parse_args()
bag = DagBag(dag_folder=str(Path(args.dags).resolve()), include_examples=False, safe_mode=False, read_dags_from_db=False)
dags = []
dependencies = []
for dag in sorted(bag.dags.values(), key=lambda value: value.dag_id):
    for task in sorted(dag.tasks, key=lambda value: value.task_id):
        dags.append({
            "dag_id": dag.dag_id,
            "task_id": task.task_id,
            "task_type": task.task_type,
            "schedule": str(dag.schedule_interval),
            "timezone": str(dag.timezone),
            "catchup": text_bool(dag.catchup),
            "max_active_runs": dag.max_active_runs,
            "owner": task.owner,
            "upstream_task_ids": "|".join(sorted(task.upstream_task_ids)),
            "downstream_task_ids": "|".join(sorted(task.downstream_task_ids)),
        })
        if isinstance(task, ExternalTaskSensor):
            dependencies.append({
                "consumer_dag": dag.dag_id,
                "sensor_task_id": task.task_id,
                "producer_dag": task.external_dag_id,
                "producer_task_id": task.external_task_id,
                "execution_delta_minutes": int(task.execution_delta.total_seconds() // 60),
                "allowed_states": "|".join(sorted(task.allowed_states)),
                "failed_states": "|".join(sorted(task.failed_states)),
                "skipped_states": "|".join(sorted(task.skipped_states)),
                "mode": task.mode,
                "poll_interval_seconds": int(task.poll_interval),
                "timeout_seconds": int(task.timeout),
                "check_existence": text_bool(task.check_existence),
                "soft_fail": text_bool(task.soft_fail),
                "deferrable": text_bool(task.deferrable),
            })
payload = {"import_errors": {str(key): str(value) for key, value in bag.import_errors.items()}, "dag_inventory": dags, "dependency_inventory": dependencies}
print("DAY_CUT_RESULT=" + json.dumps(payload, ensure_ascii=False, sort_keys=True))
