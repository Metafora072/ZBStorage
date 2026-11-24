#!/usr/bin/env python3
"""Manual XLSX composition checks using temporary source workbooks."""
import importlib.util
from pathlib import Path
import tempfile

from openpyxl import Workbook

spec = importlib.util.spec_from_file_location('compose', Path(__file__).resolve().parents[2] /
                                             'scripts/compose_mds_path_template.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def workbook(path, rows):
    book = Workbook()
    sheet = book.active
    sheet.append(['文件名', '存储地址', '文件大小'])
    for row in rows:
        sheet.append(row)
    book.save(path)
    book.close()


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    base = root / 'base.txt'
    base.write_text('./original\n./original/a.dat')  # Missing final newline.
    before = base.read_bytes()
    sources = []
    for i in range(3):
        path = root / f'{i}.xlsx'
        workbook(path, [['x.dat', f'new{i}/year.2020/x.dat', 12],
                        ['x.dat', f'new{i}/year.2020/x.dat', 12]])
        sources.append(path)
    report = module.compose(base, sources, root / 'valid')
    assert report['added_files'] == 3 and report['added_directories'] == 6
    assert all(s['duplicate_rows'] == 1 for s in report['spreadsheets'])
    lines = (root / 'valid/combined_tree.txt').read_text().splitlines()
    assert lines == ['./original', './original/a.dat',
                     'new0/year.2020/x.dat', 'new1/year.2020/x.dat', 'new2/year.2020/x.dat']
    assert base.read_bytes() == before
    cases = [
        [['a.dat', 'original/a.dat', 1]],  # Original root collision.
        [['wrong.dat', 'new/a.dat', 1]],
        [['a.dat', '../new/a.dat', 1]],
        [['plain', 'new/plain', 1]],  # Historical leaf classification would change it to a directory.
        [['x.dat', 'new/' + 'x'*65 + '/x.dat', 1]],
        [['a.dat', 'new/a.dat', 1], ['x.dat', 'new/a.dat/x.dat', 1]],
    ]
    for i, rows in enumerate(cases):
        path = root / f'invalid{i}.xlsx'
        workbook(path, rows)
        try:
            module.compose(base, [path], root / f'bad{i}')
        except ValueError:
            assert not (root / f'bad{i}/combined_tree.txt').exists()
        else:
            raise AssertionError(f'invalid case {i} accepted')
print('PASS: three-source merge, deduplication, missing newline, dotted directories, '
      'source preservation, and six invalid-input cases')
