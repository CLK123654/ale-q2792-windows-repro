from __future__ import annotations
import hashlib,json
from pathlib import Path
root=Path(__file__).resolve().parents[1];expected=json.loads((root/'qa/expected_hashes.json').read_text(encoding='utf-8'));actual={name:hashlib.sha256((root/'task'/name).read_bytes()).hexdigest() for name in expected}
if actual!=expected:raise SystemExit('attachment hash mismatch')
(root/'evidence').mkdir(exist_ok=True);(root/'evidence/attachment-hashes.json').write_text(json.dumps(actual,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
