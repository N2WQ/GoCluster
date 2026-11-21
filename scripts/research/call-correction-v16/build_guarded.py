"""Build the isolated v16 guarded frequency rescue from its frozen replay sources.

Run from a GoCluster checkout at the documented base. Only cmd/rbn_replay can
see this overlay. The shipping server is built from the original source files.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

def digest(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f,'sha256').hexdigest()

def main():
    cli=argparse.ArgumentParser()
    cli.add_argument('--repo',required=True,type=Path)
    cli.add_argument('--out',required=True,type=Path)
    args=cli.parse_args()
    here=Path(__file__).resolve().parent
    spec=json.loads((here/'guarded-sources.json').read_bytes())
    root=args.repo.resolve(strict=True)
    out=args.out.resolve()
    out.mkdir(parents=True,exist_ok=True)
    mapping={}
    for rel,hashes in spec['sources'].items():
        src=root/rel
        replacement=here/'guarded-sources'/(rel+'.tmpl')
        if digest(src)!=hashes['base'] or digest(replacement)!=hashes['research']:
            raise ValueError(f'Source differs from the frozen experiment: {rel}')
        mapping[src.as_posix()]=replacement.as_posix()
    overlay=out/'overlay.json'
    overlay.write_text(json.dumps({'Replace':mapping},indent=2)+'\n',encoding='utf-8')
    binary=out/('rbn_replay_v16.exe' if os.name=='nt' else 'rbn_replay_v16')
    subprocess.run(['go','build','-overlay',str(overlay),'-o',str(binary),'./cmd/rbn_replay'],cwd=root,check=True)
    print(binary,digest(binary))
if __name__=='__main__':main()
