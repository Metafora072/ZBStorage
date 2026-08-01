import importlib.util
import pathlib
import random
import sys
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "generate_trace.py"
SPEC = importlib.util.spec_from_file_location("zbstorage_generate_trace", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class GenerateTraceTest(unittest.TestCase):
    def test_empty_namespace_trace_is_valid(self):
        layout = MODULE.build_namespace(2, 2, 3)
        state = MODULE.NamespaceState.create(layout, "empty", 0)
        generator = MODULE.WorkloadGenerator(
            state, random.Random(7), 1024 * 1024, 64 * 1024, 4096, "lognormal", 0.2, 0.9
        )
        operations = generator.bootstrap_directories()
        file_weights = {"f_c": 10, "f_w": 30, "f_r": 50, "f_d": 10}
        dir_weights = {"d_c": 10, "d_l": 80, "d_d": 10}
        operations.extend(generator.generate_file_operation(file_weights) for _ in range(200))
        operations.extend(generator.generate_directory_operation(dir_weights) for _ in range(50))
        MODULE.validate_operations(operations, layout, "empty", 0)

    def test_cli_is_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            first = pathlib.Path(directory) / "first.trace"
            second = pathlib.Path(directory) / "second.trace"
            args = [
                "--num-file-ops", "100",
                "--num-dir-ops", "20",
                "--top-dirs", "2",
                "--subdirs-per-top", "1",
                "--files-per-dir", "4",
                "--seed", "99",
                "--output", str(first),
            ]
            self.assertEqual(MODULE.main(args), 0)
            args[-1] = str(second)
            self.assertEqual(MODULE.main(args), 0)
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_true_nested_directory_layout(self):
        layout = MODULE.build_namespace(1, 2, 1)
        self.assertIn("/D1/D1_1", layout.directories)
        self.assertIn("/D1/D1_2", layout.directories)
        self.assertNotIn("/D1_1", layout.directories)


if __name__ == "__main__":
    unittest.main()
