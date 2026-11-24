#!/usr/bin/env python3
"""Combine an existing path tree and XLSX storage-address lists for MDS import.

Requires openpyxl. File sizes continue to use the MDS generator's size rule;
the spreadsheets supply paths, not inode size overrides.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil

from openpyxl import load_workbook


def sha256(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def normalize(value):
    if not isinstance(value, str):
        raise ValueError(f'expected a path string: {value!r}')
    value = value.strip().replace('\\', '/')
    while value.startswith('./'):
        value = value[2:]
    parts = [part for part in value.strip('/').split('/') if part]
    if not parts or any(part in ('.', '..') or len(part.encode('utf-8')) > 64 or
                        any(ord(ch) < 32 for ch in part) for part in parts):
        raise ValueError(f'invalid path or component longer than 64 bytes: {value!r}')
    return '/'.join(parts)


def compose(base, spreadsheets, output):
    inputs = [base, *spreadsheets]
    hashes = {str(path): sha256(path) for path in inputs}
    base_roots = set()
    base_lines = 0
    with base.open(encoding='utf-8') as stream:
        for line in stream:
            base_lines += 1
            path = line.strip().replace('\\', '/')
            while path.startswith('./'):
                path = path[2:]
            path = path.strip('/')
            if path and path != '.':
                base_roots.add(path.split('/', 1)[0])
    all_files = set()
    sources = []
    for path in spreadsheets:
        files = set()
        rows = 0
        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            for sheet in workbook:
                iterator = sheet.iter_rows(values_only=True)
                header = next(iterator, ())
                if '存储地址' not in header or '文件名' not in header:
                    raise ValueError(f'{path}: {sheet.title}: required columns missing')
                address_column = header.index('存储地址')
                name_column = header.index('文件名')
                for number, row in enumerate(iterator, 2):
                    if all(cell is None for cell in row):
                        continue
                    address = normalize(row[address_column])
                    name = address.rsplit('/', 1)[-1]
                    if name != row[name_column] or '.' not in name:
                        raise ValueError(f'{path}: row {number}: filename mismatch or extensionless file')
                    files.add(address)
                    rows += 1
        finally:
            workbook.close()
        roots = {value.split('/', 1)[0] for value in files}
        if not files or roots & base_roots or files & all_files:
            raise ValueError(f'{path}: empty tree or overlap with another input tree')
        all_files.update(files)
        ordered = sorted(files)
        sources.append(dict(path=str(path), sha256=hashes[str(path)], rows=rows,
                            unique_files=len(files), duplicate_rows=rows-len(files),
                            roots=sorted(roots), sample_paths=[ordered[0], ordered[len(ordered)//2], ordered[-1]]))
    directories = {file.rsplit('/', depth)[0] for file in all_files
                   for depth in range(1, file.count('/') + 1)}
    if directories & all_files:
        raise ValueError('a spreadsheet path is both a file and a directory')
    if len(hashes) != len(inputs):
        raise ValueError('duplicate input files')
    output.mkdir(parents=True, exist_ok=False)
    staging = output / 'combined_tree.txt.tmp'
    combined = output / 'combined_tree.txt'
    try:
        with staging.open('wb') as dest, base.open('rb') as src:
            shutil.copyfileobj(src, dest)
            # An extra blank line is ignored by the generator and also covers a
            # base list without a trailing newline.
            dest.write(b'\n')
            for path in sorted(all_files):
                dest.write((path + '\n').encode('utf-8'))
            dest.flush()
            os.fsync(dest.fileno())
        if any(sha256(path) != hashes[str(path)] for path in inputs):
            raise ValueError('input changed during composition')
        report = dict(schema=1, base_tree=dict(path=str(base), sha256=hashes[str(base)],
                      lines=base_lines, root_count=len(base_roots)), spreadsheets=sources,
                      added_files=len(all_files), added_directories=len(directories),
                      size_policy='existing MDS deterministic 0.5–1.5 GB rule; spreadsheet sizes not applied',
                      combined_path=str(combined), combined_sha256=sha256(staging))
        os.replace(staging, combined)
        (output / 'sources.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        return report
    finally:
        staging.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-tree', type=Path, required=True)
    parser.add_argument('--xlsx', type=Path, action='append', required=True)
    parser.add_argument('--output-dir', type=Path, required=True, help='new directory for the template input and provenance')
    args = parser.parse_args()
    report = compose(args.base_tree.resolve(), [p.resolve() for p in args.xlsx], args.output_dir.resolve())
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
