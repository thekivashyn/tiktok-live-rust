"""Verify the named release-validation artifacts and summarize without rerunning them."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import statistics

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'reports/matched-comparison'
CORPUS = ROOT / 'reports/matched-corpus'
MIB = 1024 * 1024


def read(name):
    return json.loads((OUT / name).read_text(encoding='utf-8-sig'))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_socket(record):
    assert all(record['checks'].values()), record['checks']
    fixture = record['fixture']
    child = record['measurement']['child_report']
    n = record['settings']['frames']
    expected_counts = {kind: n for kind in ('chat', 'like', 'follow', 'share', 'join', 'viewers')}
    expected_counts.update(gift=n * 2, stream_end=1)
    assert child['output']['event_counts'] == expected_counts
    errors = child.get('decode_errors', child.get('report', {}).get('decode_errors'))
    assert errors == 0
    assert fixture['completed'] and not fixture['failures']
    assert fixture['active'] == fixture['active_tasks'] == 0
    assert fixture['peak_active'] == 1
    assert child.get('connections', child.get('report', {}).get('connections')) == fixture['connections']
    if record['implementation'] == 'rust':
        report = child['report']
        assert report['error'] is None and report['stop_reason'] == 'stream_ended'
        assert report['events_unqueued'] == 0
        assert report['events_enqueued'] == child['output']['events']
    else:
        assert child['ended'] and child['connect_errors'] == 0
        assert child['active_sockets_after_stop'] == 0 and child['peak_sockets'] == 1
    return child


def memory_window(samples, start, end):
    selected = [s for s in samples if start <= s['seconds'] <= end]
    assert selected
    return {
        'samples': len(selected),
        'start_seconds': selected[0]['seconds'],
        'end_seconds': selected[-1]['seconds'],
        **{field: {
            'min_mib': min(s[key] for s in selected) / MIB,
            'median_mib': statistics.median(s[key] for s in selected) / MIB,
            'max_mib': max(s[key] for s in selected) / MIB,
        } for field, key in [('working_set', 'working_set_bytes'), ('private', 'private_bytes')]},
    }


def soak_summary(record):
    child = validate_socket(record)
    fixture = record['fixture']
    assert fixture['frames'] == 120000
    assert (fixture['cuts'], fixture['stalls'], fixture['replays'], fixture['connections']) == (13, 6, 19, 20)
    measurement = record['measurement']
    samples = measurement['samples']
    end = samples[-1]['seconds']
    return {
        'version': child['version'],
        'elapsed_seconds': child['elapsed_seconds'],
        'duration_minutes': child['elapsed_seconds'] / 60,
        'peak_working_set_mib': measurement['peak_working_set_bytes'] / MIB,
        'peak_private_mib_sampled': measurement['peak_private_bytes_sampled'] / MIB,
        'cpu_seconds': measurement['cpu_seconds'],
        'average_cpu_one_core_percent': 100 * measurement['cpu_seconds'] / child['elapsed_seconds'],
        'minute_2': memory_window(samples, 60, 120),
        'last_minute': memory_window(samples, end - 60, end),
        'last_five_minutes': memory_window(samples, end - 300, end),
        'output': child['output'],
        'fixture': fixture,
        'receiver': {k: v for k, v in child.items() if k not in ('output', 'elapsed_seconds')},
        'all_checks_passed': True,
    }


def paired_socket():
    result = {}
    for impl in ('rust', 'node'):
        records = [read(f'loopback{i}-{impl}.json') for i in range(1, 4)]
        children = [validate_socket(r) for r in records]
        result[impl] = {
            'events_per_run': children[0]['output']['events'],
            'median_events_per_second': statistics.median(c['output']['events'] / c['elapsed_seconds'] for c in children),
            'median_elapsed_seconds': statistics.median(c['elapsed_seconds'] for c in children),
            **{f'median_{key}': statistics.median(r['summary'][key] for r in records)
               for key in ('peak_working_set_mib', 'peak_private_mib', 'cpu_seconds')},
            'all_checks_passed': True,
        }
    result['rust_speed_ratio'] = result['rust']['median_events_per_second'] / result['node']['median_events_per_second']
    result['rust_peak_working_set_reduction_percent'] = 100 * (1 - result['rust']['median_peak_working_set_mib'] / result['node']['median_peak_working_set_mib'])
    return result


def pipeline_summaries(name):
    document = read(name)
    result = {}
    for mode, data in document['modes'].items():
        for runs in data['runs'].values():
            for run in runs:
                assert run['exit_code'] == 0
                assert all(run['child_report']['output'][key] == value for key, value in document['expected'].items())
        result[mode] = data['summary'] | {'rust_speed_ratio': data['summary']['rust']['events_per_second'] / data['summary']['node']['events_per_second']}
    return result


def main():
    golden = {}
    for expected, rust, node in [
        ('expected.ndjson', 'rust-v0.7.1-output.ndjson', 'node-v2.5.0-output.ndjson'),
        ('expected-eviction.ndjson', 'rust-eviction.ndjson', 'node-eviction.ndjson'),
    ]:
        hashes = {name: digest(CORPUS / name) for name in (expected, rust, node)}
        assert len(set(hashes.values())) == 1, hashes
        golden[expected] = hashes
    soak = {impl: soak_summary(read(f'soak3-v0.7.1-{impl}.json')) for impl in ('rust', 'node')}
    for impl in ('rust', 'node'):
        for name, expected_hash in read(f'soak3-v0.7.1-{impl}.json')['artifact_sha256_at_start'].items():
            path = Path(name)
            if not path.is_absolute():
                path = ROOT / path
            assert digest(path) == expected_hash, f'tested artifact changed: {name}'
    baseline_soak = {impl: soak_summary(read(f'soak2-{impl}.json')) for impl in ('rust', 'node')}
    patch = read('patch-comparison-v0.7.1.json')
    for mode in patch['modes'].values():
        for runs in mode['runs'].values():
            for run in runs:
                assert run['exit_code'] == 0
                assert all(run['child_report']['output'][key] == value for key, value in patch['expected'].items())
    assert soak['rust']['output'] == soak['node']['output']
    assert soak['rust']['fixture']['wire_crc32'] == soak['node']['fixture']['wire_crc32']
    test_log = (OUT / 'tests-v0.7.1.log').read_text(encoding='utf-8')
    test_counts = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed', test_log)
    assert sum(int(passed) for passed, _ in test_counts) == 76
    assert all(int(failed) == 0 for _, failed in test_counts)
    final_checks = read('final-checks-v0.7.1.json')
    assert all(final_checks[key] == 0 for key in ('format_exit_code', 'release_exit_code', 'version_exit_code'))
    assert 'tiktok-live-rust 0.7.1' in final_checks['version']
    paths = [ROOT / 'Cargo.toml', ROOT / 'Cargo.lock', ROOT / 'target/release/tiktok-live-rust.exe',
             ROOT / 'target/release/examples/matched_receiver_candidate.exe', ROOT / 'target/release/examples/matched_pipeline.exe']
    paths += list((ROOT / 'benchmarks/connector').glob('*.mjs')) + list((ROOT / 'benchmarks/connector').glob('*.py'))
    paths += [ROOT / 'benchmarks/connector/package-lock.json']
    paths += list((ROOT / 'src').rglob('*.rs')) + list((ROOT / 'tests').rglob('*.rs'))
    paths += [ROOT / 'examples/matched_pipeline.rs', ROOT / 'examples/matched_receiver.rs',
              ROOT / 'examples/matched_receiver_candidate.rs', ROOT / 'examples/support/matched.rs']
    paths += list(CORPUS.glob('*.bin'))
    report = {
        'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
        'engine': 'tiktok-live-rust', 'version': '0.7.1', 'developer': 'Tri Nguyen',
        'connector': {'package': 'tiktok-live-connector', 'version': '2.5.0', 'node': 'v26.7.0', 'git_head': '5bc9dc5d308d374c948876fc15c6475eb2430652'},
        'host': {'os': 'Microsoft Windows 10 Pro 10.0.19045', 'cpu': '13th Gen Intel Core i5-13500', 'cores': 14, 'logical_processors': 20},
        'measurement_method': 'benchmarks/connector/README.md',
        'golden_sha256': golden,
        'pipeline_baseline_0_7_0': pipeline_summaries('pipeline-v0.7.json'),
        'pipeline_0_7_1': pipeline_summaries('pipeline-v0.7.1.json'),
        'large_gzip_0_7_1': pipeline_summaries('pipeline-large-v0.7.1.json'),
        'alternating_patch_comparison': {mode: data['summary'] for mode, data in patch['modes'].items()},
        'loopback_0_7_1': paired_socket(),
        'soak_baseline': baseline_soak,
        'soak_0_7_1': soak,
        'patch': {
            'problem': 'A late cumulative gift update after combo-state eviction could be counted again in full.',
            'behavior': 'After cache overflow, an unseen combo with count greater than one preserves repeat_count but emits quantity_delta=null, then resumes incremental deltas from the new baseline.',
            'tradeoff': 'A genuinely new combo first seen above one also becomes unknown after overflow. Missing prior quantities are not recovered. Bounded history cannot prove all-time exactly-once totals.',
            'streak_capacity_before_and_after': 4096,
            'new_state': 'Two u64 counters; no larger cache or new production dependency.',
            'failure_before': 'reports/matched-comparison/eviction-regression-before.log',
            'regression_tests': ['evicted_streak_reports_unknown_delta_then_resynchronizes', 'gift_window_overflow_is_reported_without_guessing_a_cumulative_delta'],
        },
        'checks': {'tests_passed': 76, 'tests_log': 'reports/matched-comparison/tests-v0.7.1.log', 'clippy_log': 'reports/matched-comparison/clippy-v0.7.1.log', 'final_build_and_format': final_checks},
        'exclusions': ['Initial control-message fixture used u64::MAX outside the shared positive signed int64 domain; corrected before accepted runs.', 'soak1 used a three-second test watchdog and had unexpected peer exits. It was aborted and retained; root cause was not established. Accepted soaks use ten-second test watchdogs.'],
        'limitations': ['Synthetic core event contract, not all connector event schemas.', 'No TikTok discovery/signing/TLS or internet failures in matched tests.', 'Different reconnect policies; do not compare paced wall time as recovery speed.', 'Paced soaks shared the host with short throughput tests; small changes can be scheduling noise.', 'Bounded replay and dedupe cannot recover events never delivered during a real outage.', 'Finite roughly 23-minute soak is not proof of 24/7 stability or absence of leaks.'],
        'sha256_at_summary': {str(path.relative_to(ROOT)).replace('\\', '/'): digest(path) for path in paths},
    }
    destination = OUT / 'verification-v0.7.1.json'
    destination.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'report': str(destination), 'loopback': report['loopback_0_7_1'], 'soak': {impl: {key: value for key, value in entry.items() if key not in ('receiver', 'fixture')} for impl, entry in soak.items()}}, indent=2))


if __name__ == '__main__':
    main()
