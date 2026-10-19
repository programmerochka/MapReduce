"""Этапы лабораторной: три раунда из RustFS, отказ mapper и публикация результатов.

Функции вызываются из ноутбука. Ключи берутся из окружения и не записываются в отчёт.
"""
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

import psutil

from compare_methods import compare_files, correctness, load
from step_mapreduce import describe_source, iter_results, s3_client

HERE = Path(__file__).resolve().parent
IDENTITY_FIELDS = ('etag', 'size', 'version_id', 'endpoint')


def new_folder(kind):
    folder = HERE / 'checks_work' / kind / (time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6])
    folder.mkdir(parents=True)
    return folder


def source_guard(uri):
    if not uri.startswith('s3://'):
        raise ValueError('Итоговые проверки выполняются только для s3://... из RustFS')
    return describe_source(uri)


def check_report(report, source, minimum, unknown):
    if report['input'] != source['location'] or not report.get('source'):
        raise ValueError('Запуск использовал другой источник')
    for field in IDENTITY_FIELDS:
        if report['source'].get(field) != source.get(field):
            raise ValueError(f'Разные версии объекта RustFS: {field}')
    if report.get('min_salaries', 30) != minimum or report.get('unknown_gross_as_net', True) != unknown:
        raise ValueError('Различаются правила подготовки зарплаты')


def write_csv(path, rows):
    with Path(path).open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0])
        writer.writeheader()
        writer.writerows(rows)


def run_process(command, log_path):
    """Время и пик всего дерева процесса, от запуска Python до завершения."""
    started = time.perf_counter()
    peak = 0
    stop = threading.Event()
    metric = 'private bytes' if os.name == 'nt' else 'RSS'
    with subprocess.Popen(command, cwd=HERE, env=os.environ.copy(),
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, encoding='utf-8', errors='replace') as process:
        def sample():
            nonlocal peak
            while not stop.is_set():
                total = 0
                try:
                    parent = psutil.Process(process.pid)
                    processes = [parent] + parent.children(recursive=True)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    processes = []
                for child in processes:
                    try:
                        info = child.memory_info()
                        total += info.private if os.name == 'nt' else info.rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                peak = max(peak, total)
                stop.wait(0.05)
        monitor = threading.Thread(target=sample, daemon=True)
        monitor.start()
        folder = None
        try:
            with Path(log_path).open('w', encoding='utf-8') as log:
                for line in process.stdout:
                    log.write(line)
                    print(line, end='', flush=True)
                    if line.startswith('Результаты: '):
                        folder = Path(line[len('Результаты: '):].strip())
            code = process.wait()
        finally:
            stop.set()
            monitor.join()
    seconds = time.perf_counter() - started
    if code or folder is None:
        tail = '\n'.join(Path(log_path).read_text(encoding='utf-8').splitlines()[-12:])
        raise RuntimeError(f'Ошибка запуска (код {code}); см. {log_path}\n'
                           f'Последние строки лога:\n{tail}')
    return folder, load(folder / 'summary.json'), seconds, peak, metric


def command_for(tool, uri, ks, minimum, unknown, workers=4, fail_map=-1):
    script = 'step_mapreduce.py' if tool == 'MapReduce' else 'step_duckdb.py'
    command = [sys.executable, '-X', 'utf8', '-u', str(HERE / script), uri,
               *map(str, ks), '--min-salaries', str(minimum),
               '--unknown-gross-as-net', str(int(unknown))]
    if tool == 'MapReduce':
        command += ['--workers', str(workers), '--fail-map', str(fail_map)]
    return command


def profile_methods(uri, rounds=3, minimum=30, unknown=True):
    """Три раунда из RustFS; незавершённая серия хранится в *.partial.csv."""
    if rounds < 3:
        raise ValueError('Для отчёта нужны минимум три раунда каждого метода')
    source = source_guard(uri)
    folder = new_folder('profile')
    rows, checks = [], []
    for number in range(1, rounds + 1):
        for ks, label in [((1, 2), 'Навыки + пары'), ((3,), 'Тройки')]:
            runs = {}
            order = ['MapReduce', 'DuckDB'] if number % 2 else ['DuckDB', 'MapReduce']
            for tool in order:
                print(f'\nРАУНД {number}/{rounds}; {tool}; {label}', flush=True)
                log = folder / f'round-{number}-{tool}-k{"-".join(map(str,ks))}.txt'
                run_folder, report, seconds, peak, metric = run_process(
                    command_for(tool, uri, ks, minimum, unknown), log)
                check_report(report, source, minimum, unknown)
                if describe_source(uri) != source:
                    raise ValueError('Объект RustFS изменился во время профилирования')
                runs[tool] = (run_folder, report)
                rows.append({
                    'Раунд': number, 'Метод': tool, 'Расчёт': label,
                    'Время полного запуска, с': seconds,
                    'Время внутри метода, с': report['seconds'],
                    'Пик памяти, ГиБ': peak / 2**30, 'Метрика памяти': metric,
                    'Процессов или потоков': report.get('workers', report.get('threads')),
                    'Источник': uri, 'Endpoint': source['endpoint'],
                    'ETag': source['etag'], 'Размер, байт': source['size'],
                    'Папка': str(run_folder), 'Лог': str(log),
                })
                # Сохраняем измерение сразу, вне замеряемого времени.
                # Файл partial не выдаётся за успешно завершённую серию.
                write_csv(folder / 'measurements.partial.csv', rows)
            mr_folder, mr_report = runs['MapReduce']
            _, sql_report = runs['DuckDB']
            for k in ks:
                bad = compare_files(mr_folder / f'k{k}', sql_report['results'][str(k)]['parquet'])
                checks.append({'Раунд': number, 'K': k, 'Расхождений': bad, 'Совпало': bad == 0})
                write_csv(folder / 'correctness.partial.csv', checks)
                if bad:
                    raise AssertionError(f'Раунд {number}, K={k}: {bad} несовпадений')
            print(f'Раунд {number}, {label}: все ключи и метрики совпали', flush=True)
    measurement_path = folder / 'measurements.csv'
    checks_path = folder / 'correctness.csv'
    write_csv(measurement_path, rows)
    write_csv(checks_path, checks)
    proof = dict(source=source, rounds=rounds, minimum=minimum, unknown=unknown,
                 measurements=str(measurement_path), checks=str(checks_path))
    (folder / 'profile.json').write_text(json.dumps(proof, ensure_ascii=False, indent=2), encoding='utf-8')
    (HERE / 'checks_work/latest_profile.json').write_text(
        json.dumps(proof, ensure_ascii=False, indent=2), encoding='utf-8')
    return proof


def demonstrate_failure(uri, minimum=30, unknown=True):
    source = source_guard(uri)
    baseline = {}
    for k in (1, 2, 3):
        result = load(HERE / 'mr_work' / f'latest-k{k}.json')
        check_report(load(Path(result['directory']).parent / 'summary.json'), source, minimum, unknown)
        baseline[k] = Path(result['directory'])
    folder = new_folder('failure')
    pointers = [HERE / 'mr_work' / name for name in
                ('latest.json', 'latest-k1.json', 'latest-k2.json', 'latest-k3.json')]
    previous = {path: path.read_bytes() if path.exists() else None for path in pointers}
    try:
        failed_folder, report, _, _, _ = run_process(
            command_for('MapReduce', uri, (1, 2, 3), minimum, unknown, fail_map=0),
            folder / 'stdout.txt')
        check_report(report, source, minimum, unknown)
        output = (folder / 'stdout.txt').read_text(encoding='utf-8')
        if 'Map 0: попытка 1 не удалась' not in output:
            raise AssertionError('Не найдено подтверждение неудачной первой попытки')
        rows = []
        for k in (1, 2, 3):
            work = failed_folder / f'k{k}'
            first_log = (work / 'map-0000-attempt-1.log').read_text(encoding='utf-8')
            if 'Имитирован отказ mapper' not in first_log or not (work / 'map-0000-attempt-2.log').exists():
                raise AssertionError('Нужны отказ процесса и вторая попытка')
            before = {key: (n, paid, median) for key, n, paid, median in iter_results(baseline[k])}
            after = {key: (n, paid, median) for key, n, paid, median in iter_results(work)}
            if before != after:
                raise AssertionError(f'K={k}: повторный mapper изменил результат')
            rows.append({'K': k, 'Попыток Map 0': 2, 'Стеков': len(after),
                         'Совпало с запуском без отказа': True, 'Папка': str(work)})
        if describe_source(uri) != source:
            raise AssertionError('Объект изменился во время теста отказа')
        write_csv(folder / 'failure_check.csv', rows)
        print('Отказ mapper обработан: повторная попытка успешна, удвоения частот нет.', flush=True)
        return rows
    finally:
        # Возвращаем только указатели в памяти; отдельные файлы-бэкапы не создаются.
        for path, contents in previous.items():
            if contents is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(contents)


def publish_results(uri, minimum=30, unknown=True):
    """Версионная папка в том же бакете dataset; маркер готовности записываем последним."""
    source = source_guard(uri)
    checks = correctness(uri)
    if not all(row['Совпало'] for row in checks):
        raise AssertionError('Нельзя публиковать несовпавшие результаты')
    client = s3_client()
    folder = new_folder('publication')
    bucket, prefix = source['bucket'], 'results/' + folder.name
    rows = []
    for k in (1, 2, 3):
        mr = load(HERE / 'mr_work' / f'latest-k{k}.json')
        sql = load(HERE / 'duckdb_work' / f'latest-k{k}.json')
        work = Path(mr['directory'])
        check_report(load(work.parent / 'summary.json'), source, minimum, unknown)
        check_report(load(sql['summary']), source, minimum, unknown)
        files = [(p, f'{prefix}/k{k}/{p.name}', 'MapReduce')
                 for p in sorted(work.glob('part-[0-9][0-9][0-9][0-9][0-9]'))]
        files.append((Path(sql['parquet']), f'{prefix}/duckdb/k{k}/stacks_{k}.parquet', 'DuckDB'))
        for path, key, tool in files:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            client.upload_file(str(path), bucket, key, ExtraArgs={'Metadata': {'sha256': digest}})
            response = client.get_object(Bucket=bucket, Key=key)
            restored = hashlib.sha256()
            length = 0
            try:
                for block in response['Body'].iter_chunks(chunk_size=1024 * 1024):
                    restored.update(block)
                    length += len(block)
            finally:
                response['Body'].close()
            if restored.hexdigest() != digest or length != path.stat().st_size:
                raise AssertionError(f'Проверка загруженного объекта не пройдена: {key}')
            rows.append({'K': k, 'Метод': tool, 'Объект': f's3://{bucket}/{key}',
                         'Байт': length, 'SHA256': digest, 'Проверено скачиванием': True})
            print(f'Проверен s3://{bucket}/{key} ({length:,} байт)', flush=True)
    if describe_source(uri) != source:
        raise AssertionError('Исходный объект изменился перед публикацией')
    manifest = dict(source=source, prefix=prefix, files=rows,
                    min_salaries=minimum, unknown_gross_as_net=unknown,
                    ready=True)
    payload = json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8')
    client.put_object(Bucket=bucket, Key=prefix + '/manifest.json', Body=payload, ContentType='application/json')
    client.put_object(Bucket=bucket, Key=prefix + '/_SUCCESS', Body=b'')
    client.put_object(Bucket=bucket, Key='results/latest.json', Body=payload, ContentType='application/json')
    (folder / 'manifest.json').write_bytes(payload)
    write_csv(folder / 'uploaded_files.csv', rows)
    print(f'Готово: s3://{bucket}/{prefix}/; указатель s3://{bucket}/results/latest.json', flush=True)
    return rows
