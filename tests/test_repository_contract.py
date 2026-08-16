import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "credlake"


EXPECTED_NOTEBOOKS = [
    "00_setup.py",
    "01_generate_source_data.py",
    "02_bronze_batch.py",
    "03_bronze_snapshots.py",
    "04_bronze_payments.py",
    "05_silver_dimensions.py",
    "06_silver_contracts.py",
    "07_silver_payments.py",
    "08_silver_installments.py",
    "09_gold_portfolio.py",
    "10_quality_observability.py",
    "99_reset_demo.py",
]


class RepositoryContractTests(unittest.TestCase):
    def test_expected_notebooks_exist(self):
        for notebook in EXPECTED_NOTEBOOKS:
            with self.subTest(notebook=notebook):
                self.assertTrue((SRC / notebook).is_file())

    def test_python_notebooks_compile(self):
        for notebook in EXPECTED_NOTEBOOKS:
            path = SRC / notebook
            with self.subTest(notebook=notebook):
                ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    def test_job_notebook_paths_exist(self):
        resource = ROOT / "resources" / "credlake_job.yml"
        content = resource.read_text(encoding="utf-8")
        paths = re.findall(r"notebook_path:\s+(.+)", content)
        self.assertEqual(11, len(paths))
        for relative_path in paths:
            resolved = (resource.parent / relative_path.strip()).resolve()
            with self.subTest(path=relative_path):
                self.assertTrue(resolved.is_file())

    def test_notebooks_use_databricks_source_format(self):
        for notebook in EXPECTED_NOTEBOOKS:
            first_line = (SRC / notebook).read_text(encoding="utf-8").splitlines()[0]
            with self.subTest(notebook=notebook):
                self.assertEqual("# Databricks notebook source", first_line)

    def test_no_obvious_credentials_are_committed(self):
        forbidden = [
            r"(?i)aws_secret_access_key\s*=",
            r"(?i)client_secret\s*=",
            r"(?i)github_token\s*=",
            r"(?i)password\s*=\s*['\"][^'\"]+",
        ]
        checked_suffixes = {".py", ".sql", ".yml", ".yaml", ".md"}
        for path in ROOT.rglob("*"):
            if not path.is_file() or path.suffix not in checked_suffixes:
                continue
            content = path.read_text(encoding="utf-8")
            for pattern in forbidden:
                with self.subTest(path=path, pattern=pattern):
                    self.assertIsNone(re.search(pattern, content))


if __name__ == "__main__":
    unittest.main()

