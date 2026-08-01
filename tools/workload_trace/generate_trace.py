#!/usr/bin/env python3
"""Generate deterministic, state-aware file-system workload traces."""

from __future__ import annotations

import argparse
import dataclasses
import math
import pathlib
import random
import sys
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


FILE_OPS = ("f_c", "f_w", "f_r", "f_d")
DIR_OPS = ("d_c", "d_l", "d_d")


@dataclasses.dataclass(frozen=True)
class Operation:
    op: str
    path: str
    offset: Optional[int] = None
    size: Optional[int] = None

    def encode(self, timestamp_us: int) -> str:
        if self.op in ("f_w", "f_r"):
            return f"{timestamp_us} {self.op} {self.path} {self.offset} {self.size}"
        return f"{timestamp_us} {self.op} {self.path}"


@dataclasses.dataclass
class NamespaceLayout:
    directories: List[str]
    files: List[str]
    parent_by_directory: Dict[str, str]
    parent_by_file: Dict[str, str]


@dataclasses.dataclass
class NamespaceState:
    layout: NamespaceLayout
    existing_directories: Set[str]
    file_sizes: Dict[str, int]

    @classmethod
    def create(
        cls,
        layout: NamespaceLayout,
        initial_state: str,
        initial_file_size: int,
    ) -> "NamespaceState":
        if initial_state == "populated":
            return cls(layout, {"/", *layout.directories}, {
                path: initial_file_size for path in layout.files
            })
        return cls(layout, {"/"}, {})

    def directory_is_empty(self, path: str) -> bool:
        prefix = path.rstrip("/") + "/"
        if any(candidate.startswith(prefix) for candidate in self.file_sizes):
            return False
        return not any(
            candidate != path and candidate.startswith(prefix)
            for candidate in self.existing_directories
        )


def build_namespace(top_directories: int, subdirectories_per_top: int, files_per_directory: int) -> NamespaceLayout:
    directories: List[str] = []
    parent_by_directory: Dict[str, str] = {}
    for top_index in range(1, top_directories + 1):
        top = f"/D{top_index}"
        directories.append(top)
        parent_by_directory[top] = "/"
        for sub_index in range(1, subdirectories_per_top + 1):
            child = f"{top}/D{top_index}_{sub_index}"
            directories.append(child)
            parent_by_directory[child] = top

    files: List[str] = []
    parent_by_file: Dict[str, str] = {}
    file_index = 1
    for directory in directories:
        for _ in range(files_per_directory):
            path = f"{directory}/f{file_index}"
            files.append(path)
            parent_by_file[path] = directory
            file_index += 1
    return NamespaceLayout(directories, files, parent_by_directory, parent_by_file)


class WorkloadGenerator:
    def __init__(
        self,
        state: NamespaceState,
        rng: random.Random,
        max_file_size: int,
        max_io_size: int,
        fixed_io_size: int,
        io_size_distribution: str,
        hotset_fraction: float,
        hotset_probability: float,
    ) -> None:
        self.state = state
        self.rng = rng
        self.max_file_size = max_file_size
        self.max_io_size = max_io_size
        self.fixed_io_size = fixed_io_size
        self.io_size_distribution = io_size_distribution
        hot_count = max(1, int(math.ceil(len(state.layout.files) * hotset_fraction)))
        self.hot_files = set(state.layout.files[:hot_count])
        self.hotset_probability = hotset_probability

    def bootstrap_directories(self) -> List[Operation]:
        operations: List[Operation] = []
        for path in self.state.layout.directories:
            parent = self.state.layout.parent_by_directory[path]
            if path not in self.state.existing_directories and parent in self.state.existing_directories:
                self.state.existing_directories.add(path)
                operations.append(Operation("d_c", path))
        return operations

    def _choose_file(self, candidates: Sequence[str]) -> str:
        hot_candidates = [path for path in candidates if path in self.hot_files]
        if hot_candidates and self.rng.random() < self.hotset_probability:
            return self.rng.choice(hot_candidates)
        return self.rng.choice(list(candidates))

    def _sample_io_size(self, upper_bound: int) -> int:
        upper_bound = max(1, min(upper_bound, self.max_io_size))
        if self.io_size_distribution == "fixed":
            return min(self.fixed_io_size, upper_bound)
        if self.io_size_distribution == "lognormal":
            median = max(1.0, min(float(self.fixed_io_size), float(upper_bound)))
            sampled = int(self.rng.lognormvariate(math.log(median), 1.0))
            return max(1, min(sampled, upper_bound))
        return self.rng.randint(1, upper_bound)

    def _valid_file_targets(self) -> Dict[str, List[str]]:
        missing = [
            path for path in self.state.layout.files
            if path not in self.state.file_sizes
            and self.state.layout.parent_by_file[path] in self.state.existing_directories
        ]
        existing = list(self.state.file_sizes)
        readable = [path for path, size in self.state.file_sizes.items() if size > 0]
        return {"f_c": missing, "f_w": existing, "f_r": readable, "f_d": existing}

    def _valid_directory_targets(self) -> Dict[str, List[str]]:
        creatable = [
            path for path in self.state.layout.directories
            if path not in self.state.existing_directories
            and self.state.layout.parent_by_directory[path] in self.state.existing_directories
        ]
        listable = sorted(self.state.existing_directories)
        deletable = [
            path for path in self.state.existing_directories
            if path != "/" and self.state.directory_is_empty(path)
        ]
        return {"d_c": creatable, "d_l": listable, "d_d": deletable}

    def _weighted_operation(self, targets: Dict[str, List[str]], weights: Dict[str, float]) -> Tuple[str, str]:
        available = [op for op, candidates in targets.items() if candidates]
        if not available:
            raise ValueError("no valid operation is available for the current namespace state")
        available_weights = [weights[op] for op in available]
        if sum(available_weights) <= 0:
            available_weights = [1.0] * len(available)
        op = self.rng.choices(available, weights=available_weights, k=1)[0]
        candidates = targets[op]
        path = self._choose_file(candidates) if op in FILE_OPS else self.rng.choice(candidates)
        return op, path

    def generate_file_operation(self, weights: Dict[str, float]) -> Operation:
        op, path = self._weighted_operation(self._valid_file_targets(), weights)
        if op == "f_c":
            self.state.file_sizes[path] = 0
            return Operation(op, path)
        if op == "f_d":
            del self.state.file_sizes[path]
            return Operation(op, path)
        if op == "f_w":
            size = self._sample_io_size(self.max_file_size)
            max_offset = self.max_file_size - size
            offset = self.rng.randint(0, max_offset)
            self.state.file_sizes[path] = max(self.state.file_sizes[path], offset + size)
            return Operation(op, path, offset, size)

        current_size = self.state.file_sizes[path]
        size = self._sample_io_size(current_size)
        offset = self.rng.randint(0, current_size - size)
        return Operation(op, path, offset, size)

    def generate_directory_operation(self, weights: Dict[str, float]) -> Operation:
        op, path = self._weighted_operation(self._valid_directory_targets(), weights)
        if op == "d_c":
            self.state.existing_directories.add(path)
        elif op == "d_d":
            self.state.existing_directories.remove(path)
        return Operation(op, path)


def generate_timestamps(count: int, rate: float, distribution: str, rng: random.Random) -> List[int]:
    if count == 0:
        return []
    timestamps: List[int] = []
    elapsed_us = 0.0
    for index in range(count):
        if index > 0:
            if distribution == "constant":
                elapsed_us += 1_000_000.0 / rate
            else:
                elapsed_us += rng.expovariate(rate) * 1_000_000.0
        timestamps.append(int(elapsed_us))
    return timestamps


def validate_operations(
    operations: Iterable[Operation],
    layout: NamespaceLayout,
    initial_state: str,
    initial_file_size: int,
) -> None:
    state = NamespaceState.create(layout, initial_state, initial_file_size)
    for index, operation in enumerate(operations, start=1):
        path = operation.path
        if operation.op == "d_c":
            parent = layout.parent_by_directory.get(path)
            if path in state.existing_directories or parent not in state.existing_directories:
                raise ValueError(f"operation {index}: invalid directory create: {path}")
            state.existing_directories.add(path)
        elif operation.op == "d_l":
            if path not in state.existing_directories:
                raise ValueError(f"operation {index}: list of missing directory: {path}")
        elif operation.op == "d_d":
            if path not in state.existing_directories or not state.directory_is_empty(path):
                raise ValueError(f"operation {index}: delete of non-empty or missing directory: {path}")
            state.existing_directories.remove(path)
        elif operation.op == "f_c":
            parent = layout.parent_by_file.get(path)
            if path in state.file_sizes or parent not in state.existing_directories:
                raise ValueError(f"operation {index}: invalid file create: {path}")
            state.file_sizes[path] = 0
        elif operation.op == "f_d":
            if path not in state.file_sizes:
                raise ValueError(f"operation {index}: delete of missing file: {path}")
            del state.file_sizes[path]
        elif operation.op in ("f_w", "f_r"):
            if path not in state.file_sizes or operation.offset is None or operation.size is None:
                raise ValueError(f"operation {index}: invalid I/O target: {path}")
            if operation.offset < 0 or operation.size <= 0:
                raise ValueError(f"operation {index}: invalid I/O range: {path}")
            if operation.op == "f_r" and operation.offset + operation.size > state.file_sizes[path]:
                raise ValueError(f"operation {index}: read beyond end of file: {path}")
            if operation.op == "f_w":
                state.file_sizes[path] = max(state.file_sizes[path], operation.offset + operation.size)
        else:
            raise ValueError(f"operation {index}: unknown operation: {operation.op}")


def positive_int(text: str) -> int:
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def nonnegative_int(text: str) -> int:
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return value


def probability(text: str) -> float:
    value = float(text)
    if not 0.0 <= value <= 1.0:
        raise argparse.ArgumentTypeError("must be in [0, 1]")
    return value


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a state-aware ZBStorage file workload trace")
    parser.add_argument("--num-file-ops", "--Nfop", dest="num_file_ops", type=nonnegative_int, default=10000)
    parser.add_argument("--num-dir-ops", "--Ndop", dest="num_dir_ops", type=nonnegative_int, default=2000)
    parser.add_argument("--top-dirs", "--M", dest="top_dirs", type=positive_int, default=10)
    parser.add_argument("--subdirs-per-top", "--i", dest="subdirs_per_top", type=nonnegative_int, default=5)
    parser.add_argument("--files-per-dir", "--k", dest="files_per_dir", type=positive_int, default=20)
    parser.add_argument("--rate", type=float, default=1000.0, help="Mean operations per second")
    parser.add_argument("--dist", choices=("constant", "exponential", "poisson"), default="exponential")
    parser.add_argument("--initial-state", choices=("empty", "populated"), default="empty")
    parser.add_argument("--initial-file-size", type=nonnegative_int, default=16 * 1024)
    parser.add_argument("--max-file-size", type=positive_int, default=1024 * 1024 * 1024)
    parser.add_argument("--max-io-size", type=positive_int, default=1024 * 1024)
    parser.add_argument("--fixed-io-size", type=positive_int, default=16 * 1024)
    parser.add_argument("--io-size-dist", choices=("uniform", "fixed", "lognormal"), default="lognormal")
    parser.add_argument("--hotset-fraction", type=probability, default=0.1)
    parser.add_argument("--hotset-probability", type=probability, default=0.8)
    parser.add_argument("--file-c-weight", "--file-c-prob", dest="file_c_weight", type=float, default=10)
    parser.add_argument("--file-w-weight", "--file-w-prob", dest="file_w_weight", type=float, default=20)
    parser.add_argument("--file-r-weight", "--file-r-prob", dest="file_r_weight", type=float, default=60)
    parser.add_argument("--file-d-weight", "--file-d-prob", dest="file_d_weight", type=float, default=10)
    parser.add_argument("--dir-c-weight", "--dir-c-prob", dest="dir_c_weight", type=float, default=10)
    parser.add_argument("--dir-l-weight", "--dir-l-prob", dest="dir_l_weight", type=float, default=90)
    parser.add_argument("--dir-d-weight", "--dir-d-prob", dest="dir_d_weight", type=float, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.rate <= 0:
        raise ValueError("--rate must be greater than zero")
    if args.max_io_size > args.max_file_size:
        raise ValueError("--max-io-size cannot exceed --max-file-size")

    file_weights = {
        "f_c": args.file_c_weight,
        "f_w": args.file_w_weight,
        "f_r": args.file_r_weight,
        "f_d": args.file_d_weight,
    }
    directory_weights = {
        "d_c": args.dir_c_weight,
        "d_l": args.dir_l_weight,
        "d_d": args.dir_d_weight,
    }
    if any(weight < 0 for weight in (*file_weights.values(), *directory_weights.values())):
        raise ValueError("operation weights must be non-negative")
    if args.num_file_ops and sum(file_weights.values()) <= 0:
        raise ValueError("at least one file operation weight must be positive")
    if args.num_dir_ops and sum(directory_weights.values()) <= 0:
        raise ValueError("at least one directory operation weight must be positive")

    rng = random.Random(args.seed)
    layout = build_namespace(args.top_dirs, args.subdirs_per_top, args.files_per_dir)
    state = NamespaceState.create(layout, args.initial_state, args.initial_file_size)
    generator = WorkloadGenerator(
        state,
        rng,
        args.max_file_size,
        args.max_io_size,
        args.fixed_io_size,
        args.io_size_dist,
        args.hotset_fraction,
        args.hotset_probability,
    )

    operations: List[Operation] = []
    if args.initial_state == "empty":
        operations.extend(generator.bootstrap_directories())

    file_remaining = args.num_file_ops
    directory_remaining = args.num_dir_ops
    while file_remaining or directory_remaining:
        choose_file = directory_remaining == 0 or (
            file_remaining > 0
            and rng.randrange(file_remaining + directory_remaining) < file_remaining
        )
        if choose_file:
            operations.append(generator.generate_file_operation(file_weights))
            file_remaining -= 1
        else:
            operations.append(generator.generate_directory_operation(directory_weights))
            directory_remaining -= 1

    validate_operations(operations, layout, args.initial_state, args.initial_file_size)
    distribution = "exponential" if args.dist == "poisson" else args.dist
    if args.dist == "poisson":
        print("warning: --dist=poisson is an alias for exponential inter-arrival times", file=sys.stderr)
    timestamps = generate_timestamps(len(operations), args.rate, distribution, rng)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as output:
        output.write("# zbstorage_workload_trace_v2\n")
        output.write("# columns: timestamp_us op path [offset size]\n")
        output.write(f"# seed={args.seed} initial_state={args.initial_state} rate={args.rate} dist={distribution}\n")
        output.write(
            f"# requested_file_ops={args.num_file_ops} requested_dir_ops={args.num_dir_ops} "
            f"bootstrap_ops={len(operations) - args.num_file_ops - args.num_dir_ops}\n"
        )
        for timestamp, operation in zip(timestamps, operations):
            output.write(operation.encode(timestamp) + "\n")

    print(
        f"generated {len(operations)} operations "
        f"({args.num_file_ops} file, {args.num_dir_ops} directory, "
        f"{len(operations) - args.num_file_ops - args.num_dir_ops} bootstrap) -> {args.output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2)
