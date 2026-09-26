"""Read-only ACPI platform-meter acquisition; never substitutes model power."""
import csv
import json
from pathlib import Path
import socket
import threading
import time

from .energy import capacity_power_ratio, meets_target, meter_summary
from .support import save_json
from .workloads import run_batch

DEFAULT_SENSOR = Path('/sys/devices/LNXSYSTM:00/LNXSYBUS:00/ACPI000D:00/power1_average')


def read_microwatts(path):
    value = int(Path(path).read_text().strip())
    if value < 0:
        raise ValueError('negative hardware power')
    return value


def hardware_inventory(device, sensor):
    """Count only members of the measured workload's Linux MD array, once."""
    root = Path('/sys/class/block') / device
    members = []
    for entry in sorted((root / 'slaves').iterdir()):
        block = Path('/sys/class/block') / entry.name
        members.append({'device': entry.name, 'capacity_bytes': int((block / 'size').read_text()) * 512,
                        'model': (block / 'device/model').read_text().strip()})
    if not members or int((root / 'md/degraded').read_text()) != 0:
        raise ValueError('nondegraded MD array with observed members required')
    if len(members) != int((root / 'md/raid_disks').read_text()):
        raise ValueError('MD member count differs from configured RAID disk count')
    return {'source': 'hardware_inventory', 'nodes': [{
        'node_id': socket.gethostname(), 'meter_id': str(sensor.resolve()),
        'disk_groups': [{'count': 1, 'capacity_bytes': d['capacity_bytes']} for d in members]}],
        'members': members, 'array_device': '/dev/' + device,
        'raid_level': (root / 'md/level').read_text().strip(),
        'raid_capacity_bytes': int((root / 'size').read_text()) * 512,
        'capacity_scope': 'only workload array; excludes other host disks and test directory quotas',
        'power_scope': 'whole physical host including CMS, Scheduler, nodes, OS and unrelated activity',
        'disk_power_supply': 'user confirmed disks supplied by this server PSU; not independently wired/measured'}


class HardwareSampler:
    def __init__(self, output, sensor=DEFAULT_SENSOR, device='md0', interval=1.0):
        self.output, self.sensor = Path(output), Path(sensor)
        self.interval, self.device = interval, device
        self.stop = threading.Event()
        self.error = None
        self.rows = []

    def sample(self):
        start = time.monotonic_ns()
        watts = read_microwatts(self.sensor)
        end = time.monotonic_ns()
        row = {'timestamp_us': time.time_ns() // 1000, 'monotonic_ns': end,
               'node_id': self.inventory['nodes'][0]['node_id'], 'power_uw': watts,
               'power_w': watts / 1e6, 'read_duration_us': (end - start) / 1000}
        self.writer.writerow(row)
        self.stream.flush()
        self.rows.append(row)

    def worker(self):
        try:
            while not self.stop.wait(self.interval):
                self.sample()
        except BaseException as exc:
            self.error = exc

    def __enter__(self):
        if self.interval <= 0:
            raise ValueError('positive sample interval required')
        self.inventory = hardware_inventory(self.device, self.sensor)
        base = self.sensor.parent
        self.metadata = {name: (base / name).read_text().strip() for name in
                         ('name', 'path', 'power1_model_number', 'power1_oem_info',
                          'power1_accuracy', 'power1_average_interval', 'power1_is_battery')}
        if self.metadata['name'] != 'power_meter':
            raise ValueError('expected ACPI power_meter device')
        self.metadata.update(source='acpi_hardware_sensor', sensor=str(self.sensor.resolve()),
                             unit='microwatts', sample_interval_s=self.interval,
                             calibration='not a calibrated wall-socket meter; AC/DC boundary unverified',
                             measures=[str(p.resolve()) for p in (base / 'measures').iterdir()])
        self.output.mkdir(parents=True, exist_ok=True)
        save_json(self.output / 'inventory.json', self.inventory)
        save_json(self.output / 'sensor.json', self.metadata)
        self.stream = (self.output / 'meter.csv').open('w', newline='')
        self.writer = csv.DictWriter(self.stream, fieldnames=[
            'timestamp_us', 'monotonic_ns', 'node_id', 'power_uw', 'power_w', 'read_duration_us'])
        self.writer.writeheader()
        try:
            self.sample()  # Fail before any workload if the sensor cannot be read.
        except BaseException:
            self.stream.close()
            raise
        self.thread = threading.Thread(target=self.worker, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop.set()
        self.thread.join(timeout=10)
        try:
            if self.thread.is_alive():
                raise RuntimeError('hardware sampler did not stop')
            if self.error:
                raise RuntimeError('hardware sampling failed') from self.error
            self.sample()  # Bracket the final workload without extrapolation.
            deltas = [abs((b['timestamp_us'] - a['timestamp_us']) -
                          (b['monotonic_ns'] - a['monotonic_ns']) / 1000)
                      for a, b in zip(self.rows, self.rows[1:])]
            if max(deltas, default=0) > 100_000:
                raise RuntimeError('wall clock jumped >100 ms relative to monotonic clock')
            save_json(self.output / 'acquisition.json', {
                'status': 'PASS', 'samples': len(self.rows),
                'max_clock_step_error_us': max(deltas, default=0),
                'value_changes': sum(a['power_uw'] != b['power_uw'] for a, b in zip(self.rows, self.rows[1:])),
                'min_power_w': min(r['power_w'] for r in self.rows),
                'max_power_w': max(r['power_w'] for r in self.rows)})
        except BaseException as error:
            save_json(self.output / 'failure.json', {'status': 'FAIL', 'error': repr(error)})
            raise
        finally:
            self.stream.close()

    def report(self, start, end):
        report = meter_summary(self.output / 'meter.csv', self.inventory, start, end)
        ratio = capacity_power_ratio(self.inventory['raid_capacity_bytes'], report['average_power_w'])
        report.update(source='acpi_hardware_sensor', hardware_energy_status='HARDWARE_SENSOR_MEASURED',
                      power_scope=self.inventory['power_scope'], raid_capacity_power_pb_per_kw=ratio,
                      raid_meets_10_pb_per_kw=meets_target(ratio),
                      resolution_status='SHORT_WINDOW' if end - start < 10_000_000 else 'OK',
                      uncertainty='firmware averaging and sensor calibration not independently verified; no idle subtraction')
        return report


def idle_window(seconds):
    start = time.time_ns() // 1000
    time.sleep(seconds)
    return {'start_us': start, 'end_us': time.time_ns() // 1000,
            'scope': 'no test workload submitted; shared host is not guaranteed idle'}


def run_hardware_workload(cluster, output, files, mount, idle_seconds):
    output.mkdir(parents=True)
    idle = idle_window(idle_seconds)
    save_json(output / 'idle/window.json', idle)
    write = run_batch(output / 'write', cluster.scheduler, cluster.mds, 'sequential_write',
                      files=files, min_size=4096, max_size=65536, mount=mount)
    read = run_batch(output / 'read', cluster.scheduler, cluster.mds, 'sequential_read',
                     reuse_run=output / 'write', mount=mount)
    written = json.loads((output / 'write/handoff.json').read_text())['files']
    readback = json.loads((output / 'read/handoff.json').read_text())['files']
    if written != readback or len(written) != files or {f['node_id'] for f in written} != {'node-real-01'}:
        raise RuntimeError('single-node same-file byte/placement validation failed')
    summary = {'task': 'T03-2', 'status': 'PENDING_ANALYSIS', 'io_status': 'PASS',
               'source': 'fuse_posix_with_acpi_hardware_sensor',
               'files': files, 'bytes': write['final_bytes'], 'same_file_readback': 'PASS',
               'hardware_energy_status': 'PENDING_ANALYSIS',
               'phases': {'idle': idle, 'write': {'start_us': write['start_us'], 'end_us': write['end_us']},
                          'read': {'start_us': read['start_us'], 'end_us': read['end_us']}}}
    save_json(output / 'summary.json', summary)
    return summary


def finish_hardware_reports(output, sampler, summary):
    """Recompute every real window from immutable CSV; leave model reports separate."""
    phases = {}
    names = ['sequential_write', 'sequential_read', 'random_rw', 'typical_trace', 'restart_read']
    if 'external_trace' in summary['workloads']:
        names.append('external_trace')
    for name in names:
        path = output / name / 'summary.json'
        data = json.loads(path.read_text())
        phases[name] = sampler.report(data['start_us'], data['end_us'])
        save_json(output / name / 'hardware_energy.json', phases[name])
    for name in ('baseline_before', 'baseline_after', 'migration', 'health_recovery'):
        path = output / name / 'window.json'
        data = json.loads(path.read_text())
        phases[name] = sampler.report(data['start_us'], data['end_us'])
        save_json(output / name / 'hardware_energy.json', phases[name])
    meter = json.loads((output / 'meter/summary.json').read_text())
    for name, window in meter['phases'].items():
        report = sampler.report(window['start_us'], window['end_us'])
        phases['meter/' + name] = report
        save_json(output / 'meter' / name / 'hardware_energy.json', report)
    meter.update(status='PASS', hardware_energy_status='HARDWARE_SENSOR_MEASURED',
                 energy_target_status='PASS' if all(phases['meter/' + n]['meets_10_pb_per_kw'] is True
                                                     for n in meter['phases']) else 'FAIL',
                 measured_phases={n: phases['meter/' + n] for n in meter['phases']})
    save_json(output / 'meter/summary.json', meter)
    summary['meter']['status'] = meter['status']
    summary.update(status='PASS', hardware_energy_status='HARDWARE_SENSOR_MEASURED',
                   energy_target_status=meter['energy_target_status'],
                   hardware_power={'physical_meters': 1, 'source': 'acpi_hardware_sensor',
                                   'raw_csv': str(sampler.output / 'meter.csv'), 'phases': phases})
    return summary
