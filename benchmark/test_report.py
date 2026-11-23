"""Tests with invented timing numbers in temporary folders, never benchmark evidence."""
import contextlib
import io
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
import report

class ReportTests(unittest.TestCase):
    def fixture(self, root):
        results = root / 'results'
        logs = root / 'logs'
        results.mkdir(); logs.mkdir()
        (logs / 'environment.txt').write_text('Synthetic unit test only')
        for size, factor in [('original', 1), ('larger', 10)]:
            (results / (size + '.rows')).write_text(str(61527 * factor))
            (results / (size + '.bytes')).write_text('12345 24690 /test')
            text = 'SITE-01\t2024\t7\tconsultation\t%d\nSITE-08\t2024\t7\tprocedure\t%d\n' % (61507 * factor, 20 * factor)
            for engine in ['spark', 'mr']:
                for trial in [1, 2, 3]:
                    name = '%s_%s_%s' % (engine, size, trial)
                    (results / (name + '.tsv')).write_text(text)
                    (logs / (name + '.time')).write_text(str(trial * 2))
                    (logs / (name + '.log')).write_text('MR_MODE=local\n')
        return results

    def test_statistics_chart_and_totals(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); results = self.fixture(root)
            with contextlib.redirect_stdout(io.StringIO()): report.make_report(root)
            import csv
            with (results / 'summary.csv').open() as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(float(rows[0]['median_seconds']), 4)
            self.assertEqual(float(rows[0]['median_rows_per_second']), 61527 / 4)
            self.assertEqual(float(rows[2]['median_rows_per_second']), 615270 / 4)
            ET.parse(str(results / 'comparison_chart.svg'))
            self.assertEqual(len((results / 'trials.csv').read_text().splitlines()), 13)

    def test_same_total_wrong_groups_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); results = self.fixture(root)
            p = results / 'mr_original_2.tsv'
            p.write_text(p.read_text().replace('61507', '61506').replace('procedure\t20', 'procedure\t21'))
            with self.assertRaises(AssertionError): report.make_report(root)

    def test_failed_timing_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); self.fixture(root)
            (root / 'logs/spark_original_1.time').write_text('nan')
            with self.assertRaises(AssertionError): report.make_report(root)

if __name__ == '__main__': unittest.main()
