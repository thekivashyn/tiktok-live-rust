"""Alternating 0.7.0/0.7.1 Rust runs to distinguish patch cost from separate-run noise."""
from datetime import datetime, timezone
import hashlib
import json
import statistics
import zlib
from measure import CORPUS, OUT, ROOT, once

executables = {
    '0.7.0': OUT / 'baseline-v0.7/matched_pipeline.exe',
    '0.7.1': ROOT / 'target/release/examples/matched_pipeline.exe',
}
passes = 64
oracle = (CORPUS / 'expected.ndjson').read_bytes()
crc = 0
for _ in range(passes):
    crc = zlib.crc32(oracle, crc)
expected = {'events': 65536 * passes, 'gift_quantity': 24576 * passes,
            'serialized_bytes': len(oracle) * passes, 'crc32': crc}
modes = {}
for mode in ('plain', 'mixed', 'gzip'):
    groups = {version: [] for version in executables}
    for pair in range(3):
        for version in (('0.7.0', '0.7.1') if pair % 2 == 0 else ('0.7.1', '0.7.0')):
            run = once([str(executables[version]), str(CORPUS / f'{mode}.bin'), str(passes)], 120, False, False)
            assert run['exit_code'] == 0
            assert run['child_report']['version'] == version
            assert all(run['child_report']['output'][key] == value for key, value in expected.items())
            groups[version].append(run)
    medians = {version: {
        'events_per_second': statistics.median(run['child_report']['events_per_second'] for run in runs),
        'peak_working_set_mib': statistics.median(run['peak_working_set_bytes'] for run in runs) / 1048576,
        'peak_private_mib': statistics.median(run['peak_private_bytes_sampled'] for run in runs) / 1048576,
        'cpu_seconds': statistics.median(run['cpu_seconds'] for run in runs),
    } for version, runs in groups.items()}
    medians['throughput_change_percent'] = 100 * (medians['0.7.1']['events_per_second'] / medians['0.7.0']['events_per_second'] - 1)
    modes[mode] = {'summary': medians, 'runs': groups}
    print(json.dumps({'mode': mode, 'summary': medians}), flush=True)
document = {
    'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
    'pairs': 3, 'passes': passes, 'expected': expected, 'modes': modes,
    'binary_sha256': {version: hashlib.sha256(path.read_bytes()).hexdigest() for version, path in executables.items()},
    'limits': 'Alternating runs on the same host while the final paced socket soak continued. Synthetic core path only; not a confidence interval or real LIVE latency.',
}
(OUT / 'patch-comparison-v0.7.1.json').write_text(json.dumps(document, indent=2) + '\n', encoding='utf-8')
