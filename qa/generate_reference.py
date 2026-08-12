from __future__ import annotations
import json,os,shutil,subprocess,sys,zipfile
from pathlib import Path
root=Path(__file__).resolve().parents[1];work=root/'work-reference';evidence=root/'evidence'
if work.exists():shutil.rmtree(work)
work.mkdir()
with zipfile.ZipFile(root/'task/输入数据包.zip') as package:package.extractall(work)
completed=subprocess.run([sys.executable,str(root/'implementation/build_delivery.py'),'--input',str(work/'input_data'),'--output',str(work/'output')],cwd=root,text=True,encoding='utf-8',errors='replace',capture_output=True,timeout=600)
if completed.returncode:raise SystemExit(completed.stdout+completed.stderr)
evidence.mkdir(exist_ok=True)
with zipfile.ZipFile(evidence/'reference-candidate.zip','w',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as archive:
 for path in sorted((work/'output').rglob('*')):
  if path.is_file() and '__pycache__' not in path.parts:archive.write(path,path.relative_to(work).as_posix())
(evidence/'reference-generation.json').write_text(json.dumps({'result':'PASS','mode':'reference','commit_sha':os.getenv('GITHUB_SHA'),'workflow_run_id':os.getenv('GITHUB_RUN_ID'),'reference_members':sorted(path.relative_to(work).as_posix() for path in (work/'output').rglob('*') if path.is_file() and '__pycache__' not in path.parts)},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
