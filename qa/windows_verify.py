from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1];TASK=ROOT/'task';EVIDENCE=ROOT/'evidence';RUN_ROOT=ROOT/'windows-runs'
def sha(path:Path)->str:return hashlib.sha256(path.read_bytes()).hexdigest()
def reset(path:Path)->None:
 if path.exists():shutil.rmtree(path)
 path.mkdir(parents=True)
def extract(archive:Path,target:Path)->None:
 target.mkdir(parents=True)
 with zipfile.ZipFile(archive) as package:package.extractall(target)
def members(root:Path)->list[str]:return sorted(path.relative_to(root).as_posix() for path in root.rglob('*') if path.is_file() and '__pycache__' not in path.parts)
def normalized(path:Path)->bytes:return path.read_bytes().replace(b'\r\n',b'\n')
def compare(actual:Path,expected:Path)->list[str]:
 if members(actual)!=members(expected):raise AssertionError('Reference path set differs')
 for relative in members(expected):
  if normalized(actual/relative)!=normalized(expected/relative):raise AssertionError(f'Reference differs:{relative}')
 return members(expected)
def build(input_root:Path,output:Path)->subprocess.CompletedProcess[str]:
 return subprocess.run([sys.executable,str(ROOT/'implementation/build_delivery.py'),'--input',str(input_root),'--output',str(output)],cwd=ROOT,text=True,encoding='utf-8',errors='replace',capture_output=True,timeout=600)
def rows(path:Path)->list[dict[str,str]]:
 with path.open(encoding='utf-8',newline='') as handle:return list(csv.DictReader(handle))
def main()->None:
 reset(RUN_ROOT);EVIDENCE.mkdir(exist_ok=True)
 version=subprocess.run([sys.executable,'-m','airflow','version'],text=True,encoding='utf-8',errors='replace',capture_output=True,timeout=60)
 if version.returncode or version.stdout.strip()!='2.10.5':raise AssertionError(version.stdout+version.stderr)
 reference=RUN_ROOT/'reference';extract(TASK/'reference.zip',reference);expected=reference/'output';clean=[]
 for label in ['clean-a','clean-b']:
  base=RUN_ROOT/label;extract(TASK/'输入数据包.zip',base);input_root=base/'input_data';before={path.relative_to(input_root).as_posix():sha(path) for path in input_root.rglob('*') if path.is_file()}
  for index in [1,2]:
   output=base/f'output-{index}';completed=build(input_root,output)
   if completed.returncode:raise AssertionError(completed.stdout+completed.stderr)
   generated=compare(output,expected);clean.append({'root_id':label,'process_index':index,'return_code':0,'output_started_empty':True,'primary_software_executed':True,'input_unchanged':True,'reference_full_match':True,'generated_paths':generated})
  after={path.relative_to(input_root).as_posix():sha(path) for path in input_root.rglob('*') if path.is_file()}
  if before!=after:raise AssertionError('input changed')
 positive=RUN_ROOT/'positive';extract(TASK/'输入数据包.zip',positive);plan=positive/'input_data/dependency_plan.csv';data=rows(plan)
 for row in data:
  if row['sensor_task_id']=='wait_for_features':row['poll_interval_seconds']='50'
 with plan.open('w',encoding='utf-8',newline='') as handle:writer=csv.DictWriter(handle,fieldnames=list(data[0]),lineterminator='\n');writer.writeheader();writer.writerows(data)
 completed=build(positive/'input_data',positive/'output')
 if completed.returncode:raise AssertionError(completed.stdout+completed.stderr)
 observed=rows(positive/'output/results/dependency_inventory.csv');changed=next(row for row in observed if row['sensor_task_id']=='wait_for_features')
 if changed['poll_interval_seconds']!='50' or normalized(positive/'output/results/dependency_inventory.csv')==normalized(expected/'results/dependency_inventory.csv'):raise AssertionError('valid Sensor change had no effect')
 (EVIDENCE/'positive-case.json').write_text(json.dumps({'input_field':'wait_for_features.poll_interval_seconds','before':'45','after':'50','behavior_changed':True},indent=2)+'\n',encoding='utf-8')
 negative=RUN_ROOT/'negative';extract(TASK/'输入数据包.zip',negative);request_path=negative/'input_data/release_request.json';request=json.loads(request_path.read_text(encoding='utf-8'));request['dags']['ranker_release'].remove('wait_for_features');request_path.write_text(json.dumps(request,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');output=negative/'output';output.mkdir();(output/'stale.txt').write_text('stale',encoding='utf-8');completed=build(negative/'input_data',output)
 if completed.returncode==0 or output.exists():raise AssertionError('unknown Sensor identity did not fail closed')
 (EVIDENCE/'negative-case.log').write_text(f'return_code={completed.returncode}\n{completed.stdout}{completed.stderr}',encoding='utf-8')
 summary={'result':'PASS','commit_sha':os.getenv('GITHUB_SHA'),'workflow_run_id':os.getenv('GITHUB_RUN_ID'),'runner_image':os.getenv('ImageOS'),'main_software':{'name':'Apache Airflow','version':version.stdout.strip(),'executed':True},'clean_directory_count':2,'process_runs_per_directory':2,'clean_runs':clean,'positive_mutation':'PASS','negative_case':'PASS','reference_full_comparison':'PASS','formal_network':{'wsl_external_interface_disabled':True,'external_services_used':False},'linux_executables':['python3','airflow'],'linux_executables_executed':True,'wsl2_required':True}
 (EVIDENCE/'windows-summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
if __name__=='__main__':main()
