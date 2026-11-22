"""Validate exported results and summarize real VM timings; no PySpark used."""
import csv
import math
import statistics
import sys
from pathlib import Path
from xml.sax.saxutils import escape


def load_result(path):
    result = {}
    for line in path.open().read().splitlines():
        fields = line.split('\t')
        if len(fields) != 5:
            raise ValueError('Invalid output: ' + str(path))
        key = tuple(fields[:4])
        count = int(fields[4])
        if key in result or count <= 0:
            raise ValueError('Duplicate group or nonpositive count: ' + str(path))
        result[key] = count
    if not result:
        raise ValueError('Empty output: ' + str(path))
    return result


def write_csv(path, rows):
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def make_report(work):
    results = work / 'results'
    logs = work / 'logs'
    validations = []
    trials = []
    summaries = []
    references = {}
    for size, required in [('original', 61527), ('larger', 615270)]:
        n = int((results / (size + '.rows')).open().read())
        assert n == required, 'Unexpected input row count'
        logical_bytes = int((results / (size + '.bytes')).open().read().split()[0])
        reference = load_result(results / ('spark_%s_1.tsv' % size))
        references[size] = reference
        assert sum(reference.values()) == n, 'Total differs from input'
        demo = reference.get(('SITE-08', '2024', '7', 'procedure'))
        assert demo == (20 if size == 'original' else 200), 'Demo count differs'
        for engine in ['spark', 'mr']:
            seconds = []
            for trial in [1, 2, 3]:
                name = '%s_%s_%s' % (engine, size, trial)
                observed = load_result(results / (name + '.tsv'))
                assert observed == reference, 'Grouped counts differ: ' + name
                elapsed = float((logs / (name + '.time')).open().read().strip())
                assert math.isfinite(elapsed) and elapsed > 0, 'Invalid timing'
                seconds.append(elapsed)
                trials.append(dict(engine=engine, size=size, trial=trial,
                    input_rows=n, input_bytes=logical_bytes, elapsed_seconds=elapsed,
                    rows_per_second=n / elapsed))
                validations.append('PASS %s groups=%s total=%s demo=%s' % (
                    name, len(observed), sum(observed.values()), demo))
            rates = [n / t for t in seconds]
            summaries.append(dict(engine=engine, size=size, input_rows=n,
                input_bytes=logical_bytes, median_seconds=statistics.median(seconds),
                min_seconds=min(seconds), max_seconds=max(seconds),
                median_rows_per_second=statistics.median(rates),
                min_rows_per_second=min(rates), max_rows_per_second=max(rates)))
    assert references['larger'] == {k: v * 10 for k, v in references['original'].items()}, \
        'Larger grouped counts are not exactly ten times original'
    validations.append('PASS every larger group equals ten times its original group')
    (results / 'matching_totals.txt').open("w").write('\n'.join(validations) + '\n')
    write_csv(results / 'trials.csv', trials)
    write_csv(results / 'summary.csv', summaries)
    # SVG chart generated only from the successful measured run. Whiskers show min/max.
    width, height = 940, 520
    left, top, plot_height = 90, 100, 290
    maximum = max(s['max_seconds'] for s in summaries) * 1.15
    chart = ['<svg xmlns="http://www.w3.org/2000/svg" width="940" height="520" viewBox="0 0 940 520">',
      '<rect width="940" height="520" fill="white"/>',
      '<g font-family="Arial, sans-serif" fill="#142334">',
      '<text x="50" y="38" font-size="24">Monthly visit count benchmark</text>',
      '<text x="50" y="64" font-size="14">Median elapsed seconds with minimum to maximum whiskers</text>']
    for i in range(6):
        value = maximum * i / 5
        y = top + plot_height * (1 - i / 5)
        chart.append('<path d="M90 %s H890" stroke="#dce2e8"/>' % y)
        chart.append('<text x="80" y="%s" text-anchor="end" font-size="12">%.1f</text>' % (y + 4, value))
    for index, item in enumerate(summaries):
        x = 175 + (index // 2) * 400 + (index % 2) * 145
        def ypos(v):
            return top + plot_height * (1 - v / maximum)
        y = ypos(item['median_seconds'])
        color = '#28628c' if item['engine'] == 'spark' else '#d47b35'
        chart.append('<rect x="%s" y="%s" width="90" height="%s" fill="%s"/>' % (
            x, y, top + plot_height - y, color))
        low, high = ypos(item['min_seconds']), ypos(item['max_seconds'])
        chart.append('<path d="M%s %s V%s M%s %s h20 M%s %s h20" stroke="#18232c" stroke-width="2"/>' % (
            x + 45, high, low, x + 35, high, x + 35, low))
        label = 'Spark SQL' if item['engine'] == 'spark' else 'MapReduce'
        chart.append('<text x="%s" y="412" text-anchor="middle" font-size="14">%s</text>' % (x + 45, label))
        chart.append('<text x="%s" y="%s" text-anchor="middle" font-size="13">%.2f s</text>' % (
            x + 45, high - 12, item['median_seconds']))
    chart.extend(['<text x="290" y="452" text-anchor="middle" font-size="16">Original 61,527 rows</text>',
      '<text x="690" y="452" text-anchor="middle" font-size="16">Synthetic 10 times 615,270 rows</text>',
      '<text x="50" y="492" font-size="13">Three trials per engine per size on the same VM. End to end command timing.</text>',
      '</g></svg>'])
    (results / 'comparison_chart.svg').open("w").write('\n'.join(chart))
    environment = (logs / 'environment.txt').open().read()
    modes = sorted(set(line.strip() for p in logs.glob('mr_*.log')
                       for line in p.open(errors='replace').read().splitlines()
                       if line.startswith('MR_MODE=')))
    markdown = ['# Spark SQL and MapReduce comparison', '',
      '## Workload and correctness', '',
      'Both jobs count accepted visits by clinic, visit year, visit month and visit type. '
      'They read the identical headerless HDFS TSV, with one visit represented by each row. '
      'The original input contains 61,527 rows. A synthetic larger input repeats these rows ten times '
      'and contains 615,270 rows; it does not represent additional clinic activity.', '',
      'All twelve measured outputs match their size-specific reference, and every total equals '
      'the input row count. Every larger grouped count is ten times the original. '
      'SITE-08 procedure visits in July 2024 equal 20 and 200 respectively. '
      'See matching_totals.txt for the individual checks.', '',
      '## Measured results', '',
      '| Engine | Input | Rows | Logical bytes | Median seconds | Min to max seconds | Median rows per second | Min to max rows per second |',
      '| --- | --- | ---: | ---: | ---: | --- | ---: | --- |']
    for s in summaries:
        markdown.append('| {engine} | {size} | {input_rows} | {input_bytes} | {median_seconds:.3f} | {min_seconds:.3f} to {max_seconds:.3f} | {median_rows_per_second:.2f} | {min_rows_per_second:.2f} to {max_rows_per_second:.2f} |'.format(**s))
    markdown.extend(['', '## Interpretation', ''])
    for size in ['original', 'larger']:
        pair = [s for s in summaries if s['size'] == size]
        winner = min(pair, key=lambda s: s['median_seconds'])
        if pair[0]['median_seconds'] == pair[1]['median_seconds']:
            markdown.append('At the %s size the two median times were equal.' % size)
        else:
            markdown.append('At the %s size, %s had the lower median elapsed time in this run.' % (size, winner['engine']))
    markdown.extend(['', 'Use the measured winner for this tested batch workload when elapsed time is the deciding factor. '
      'Spark SQL expresses the reporting rule directly and can share the project’s existing Spark transformations. '
      'Java MapReduce provides an explicit batch execution path but requires mapper, reducer and packaging code. '
      'These measurements alone do not establish a universal engine preference or the clinic’s required refresh interval.', '',
      '## Method and limitations', '',
      'Three trials were run per engine per size, sequentially on the same VM. Engine order reverses in trial 2. '
      'Timing includes process launch, input parsing, aggregation and HDFS output writing. It excludes baseline '
      'creation, input expansion, Java compilation and output checks. Throughput is input rows divided by elapsed seconds. '
      'Ranges are the observed minimum and maximum, not confidence intervals. Fixture jobs run before the measurements; '
      'filesystem and JVM caching may affect timing. Spark uses one shuffle partition; MapReduce uses one reducer. '
      'Input file counts are saved beside the byte measurements. Synthetic duplication preserves group cardinality and '
      'does not test increased reporting diversity. Startup can dominate these small inputs.', '',
      'Actual MapReduce modes: ' + (', '.join(modes) or 'MISSING MODE RECORD investigate before submission'), '',
      'If Spark runs locally and MapReduce runs through YARN, disclose the different launch/resource overheads. '
      'Record VirtualBox CPU cores and RAM allocation here before submission: **ADD YOUR VM SETTINGS**.', '',
      '## Figure and evidence', '',
      'comparison_chart.svg shows median times with min/max whiskers. trials.csv contains all twelve timings. '
      'summary.csv contains time and throughput statistics. Keep the complete logs folder and run.log with this report. '
      'Copy the final report/figure to the team evidence location and link them from the PR.', '',
      '## Recorded environment', '', '```text', environment.strip(), '```', ''])
    (results / 'comparison.md').open("w").write('\n'.join(markdown))
    print('\n'.join(validations))
    for s in summaries:
        print('{engine} {size}: median {median_seconds:.3f} s; range {min_seconds:.3f} to {max_seconds:.3f}; {median_rows_per_second:.2f} rows/s'.format(**s))
    print('Report and chart:', results)


if __name__ == '__main__':
    make_report(Path(sys.argv[1]).resolve())
