"""Локальный MapReduce: сплиты 64 МиБ, multiprocessing, 8 редьюсеров.
Запуск: python step_mapreduce.py DATA.csv 1 2 3
Вход: локальный CSV или s3://bucket/key; одна запись на строку (проверяется).
"""
import argparse
import json
import subprocess
import sys
import csv
import io
from functools import lru_cache
from urllib.parse import urlsplit
from contextlib import contextmanager
import heapq
import multiprocessing
import os
from pathlib import Path
import time
import uuid
import threading
import psutil
import zlib
from contextlib import ExitStack

HERE = Path(__file__).resolve().parent
MAPPER = HERE / "jobs/mapper.py"
REDUCER = HERE / "jobs/reducer_stats.py"
SPLIT = 64 * 2**20
REDUCERS = 8
# Ограничиваем одновременные 64-МиБ задачи, чтобы не перегрузить ноутбук.
WORKERS = min(4, os.cpu_count() or 1)
S3_READ_ATTEMPTS = 4


class MemoryWatch:
    """Выборочный пик суммы памяти процесса и всех его потомков каждые 50 мс."""
    def __init__(self):
        self.peak = 0
        self.samples = 0
        self.metric = "private bytes" if os.name == "nt" else "RSS"
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def sample(self):
        parent = psutil.Process()
        total = 0
        for process in [parent] + parent.children(recursive=True):
            try:
                info = process.memory_info()
                total += info.private if os.name == "nt" else info.rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        self.peak = max(self.peak, total)
        self.samples += 1

    def run(self):
        while not self.stop_event.is_set():
            self.sample()
            self.stop_event.wait(0.05)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join()
        self.sample()


@lru_cache(maxsize=1)
def s3_client():
    # Каждый worker создаёт свой клиент; секреты остаются в окружении процесса.
    import boto3
    from botocore.config import Config
    endpoint = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        raise RuntimeError("Введите ключи RustFS в ячейке подключения")
    return boto3.client("s3", endpoint_url=endpoint,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      connect_timeout=10, read_timeout=60,
                      retries={"max_attempts": 3, "mode": "standard"}))


def object_args(source):
    args = dict(Bucket=source["bucket"], Key=source["key"], IfMatch=source["etag"])
    if source.get("version_id"):
        args["VersionId"] = source["version_id"]
    return args


def describe_source(location):
    if not location.startswith("s3://"):
        path = Path(location).resolve()
        return dict(kind="local", location=str(path), size=path.stat().st_size)
    parsed = urlsplit(location)
    if not parsed.netloc or not parsed.path.lstrip("/") or parsed.query or parsed.fragment:
        raise ValueError("Используйте s3://бакет/имя-файла без query и fragment")
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    head = s3_client().head_object(Bucket=bucket, Key=key)
    return dict(kind="s3", location=location, bucket=bucket, key=key,
                size=head["ContentLength"], etag=head["ETag"],
                version_id=head.get("VersionId"),
                endpoint=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"))


@contextmanager
def open_source_text(source):
    if source["kind"] == "local":
        with open(source["location"], encoding="utf-8-sig", newline="") as stream:
            yield stream
    else:
        # Проверка CSV проходит потоком прямо из объекта: локальной копии нет.
        response = s3_client().get_object(**object_args(source))
        body = response["Body"]
        try:
            with io.TextIOWrapper(body, encoding="utf-8-sig", newline="") as stream:
                yield stream
        finally:
            body.close()


def retry_s3_read(operation, description):
    """Повторяем весь незавершённый GET, включая ошибки чтения тела ответа.

    Повторы запросов SDK не охватывают body.read() после получения заголовков.
    Неполный ответ отбрасывается; ETag и VersionId остаются прежними.
    Ошибки прав доступа, смены ETag и неверного диапазона не скрываются.
    """
    from botocore.exceptions import (
        ResponseStreamingError, IncompleteReadError, ReadTimeoutError,
        ConnectTimeoutError, ConnectionClosedError, EndpointConnectionError,
    )
    transient = (ResponseStreamingError, IncompleteReadError, ReadTimeoutError,
                 ConnectTimeoutError, ConnectionClosedError, EndpointConnectionError)
    for attempt in range(1, S3_READ_ATTEMPTS + 1):
        try:
            return operation()
        except transient as error:
            if attempt == S3_READ_ATTEMPTS:
                raise RuntimeError(
                    f'RustFS: {description} не завершено после {attempt} попыток '
                    f'({type(error).__name__}). Проверьте состояние RustFS; '
                    'неполные данные не переданы mapper.'
                ) from error
            delay = 2 ** (attempt - 1)
            print(f'RustFS: {description}, попытка {attempt}/{S3_READ_ATTEMPTS} '
                  f'не удалась ({type(error).__name__}); повтор через {delay} с', flush=True)
            time.sleep(delay)


def _validate_csv_once(source, collect_skills=False):
    from jobs.mapper import COLUMNS, in_segment, skills_of
    csv.field_size_limit(100_000_000)
    rows = 0
    dictionary = set()
    with open_source_text(source) as stream:
        reader = csv.reader(stream, strict=True)
        if next(reader, None) != COLUMNS:
            raise ValueError("Заголовок CSV отличается от ожидаемого")
        previous = reader.line_num
        for row in reader:
            if reader.line_num != previous + 1:
                raise ValueError("Есть перенос внутри CSV-поля: разбиение по строкам неприменимо")
            if len(row) != len(COLUMNS):
                raise ValueError(f"Неверное число полей в записи {rows+1}")
            if collect_skills:
                record = dict(zip(COLUMNS, row))
                if in_segment(record):
                    dictionary.update(skills_of(record))
            previous = reader.line_num
            rows += 1
    return rows, sorted(dictionary)


def validate_csv(source, collect_skills=False):
    if source['kind'] == 's3':
        # Каждая попытка начинает проверку заново, без накопления строк прошлой попытки.
        return retry_s3_read(lambda: _validate_csv_once(source, collect_skills), 'проверка CSV')
    return _validate_csv_once(source, collect_skills)


def _get_range_once(source, start, end):
    """Одна полная попытка GET; при обрыве полученные байты не используются."""
    if start >= end:
        return b""
    response = s3_client().get_object(**object_args(source), Range=f"bytes={start}-{end-1}")
    body = response["Body"]
    try:
        if response["ResponseMetadata"]["HTTPStatusCode"] != 206:
            raise RuntimeError("Хранилище не выполнило Range GET (ожидался HTTP 206)")
        expected = f"bytes {start}-{end-1}/{source['size']}"
        if response.get("ContentRange") != expected:
            raise RuntimeError("Хранилище вернуло другой диапазон")
        data = body.read()
    finally:
        body.close()
    if len(data) != end-start:
        from botocore.exceptions import IncompleteReadError
        raise IncompleteReadError(actual_bytes=len(data), expected_bytes=end-start)
    return data


def get_range(source, start, end):
    """Чтение [start,end) через Range GET с повторами неполного ответа."""
    if start >= end:
        return b''
    return retry_s3_read(lambda: _get_range_once(source, start, end),
                         f'чтение bytes={start}-{end-1}')


def read_split_s3(source, start, end):
    """Тот же принцип хвоста строки, что у преподавателя, через Range GET."""
    size = source["size"]
    stop = min(end + 64*1024, size)
    data = bytearray(get_range(source, start, stop))
    boundary = end-start
    # Ищем конец строки начиная с границы, а не перед ней.
    newline = data.find(b"\n", boundary)
    while newline < 0 and stop < size:
        next_stop = min(stop + 64*1024, size)
        data.extend(get_range(source, stop, next_stop))
        stop = next_stop
        newline = data.find(b"\n", boundary)
    last = newline+1 if newline >= 0 else len(data)
    first = 0
    if start:
        first_newline = data.find(b"\n")
        if first_newline < 0:
            return b""
        first = first_newline+1
        if first > boundary:
            return b""
    return bytes(data[first:last])


def read_split(source, start, end):
    # Совместимость с прежними локальными проверками read_split(path,...).
    if not isinstance(source, dict):
        source = describe_source(str(source))
    if source["kind"] == "s3":
        return read_split_s3(source, start, end)
    with open(source["location"], "rb") as stream:
        stream.seek(start)
        if start:
            stream.readline()
        if stream.tell() > end:
            return b""
        return stream.read(end-stream.tell()) + stream.readline()


def map_task(task):
    index, path, start, end, k, work, unknown, fail_map, combine, dictionary_path = task
    data = read_split(path, start, end)
    env = dict(os.environ, K=str(k), UNKNOWN_GROSS_AS_NET=str(int(unknown)),
               MAP_COMBINE=str(int(combine)))
    if dictionary_path:
        env["SKILL_DICT"] = dictionary_path
    for attempt in range(1, 5):
        env["SIMULATE_MAPPER_FAILURE"] = str(int(index == fail_map and attempt == 1))
        result = subprocess.run([sys.executable, str(MAPPER)], input=data,
                                capture_output=True, env=env)
        Path(work, f"map-{index:04d}-attempt-{attempt}.log").write_bytes(result.stderr)
        if result.returncode == 0:
            break
        print(f"Map {index}: попытка {attempt} не удалась", flush=True)
    else:
        raise RuntimeError(result.stderr.decode("utf-8", "replace"))
    # Публикуем промежуточные данные только успешной попытки.
    buckets = [[] for _ in range(REDUCERS)]
    represented = 0
    for line in result.stdout.splitlines(keepends=True):
        key = line.split(b"\t", 1)[0]
        buckets[zlib.crc32(key) % REDUCERS].append(line)
        represented += int(line.split(b"\t", 2)[1]) if combine else 1
    for r, bucket in enumerate(buckets):
        bucket.sort()
        with open(Path(work, f"map-{index:04d}-r{r:02d}"), "wb") as stream:
            stream.writelines(bucket)
    return sum(map(len, buckets)), len(result.stdout), represented


def map_task_single_pass(task):
    """Один запуск mapper на блок; раскладываем K=1/2/3 по отдельным reduce-папкам."""
    index, source, start, end, ks, run_root, unknown, fail_map, combine, dictionary_path = task
    data = read_split(source, start, end)
    env = dict(os.environ, K_LIST=",".join(map(str, ks)),
               UNKNOWN_GROSS_AS_NET=str(int(unknown)), MAP_COMBINE=str(int(combine)))
    if dictionary_path:
        env["SKILL_DICT"] = dictionary_path
    for attempt in range(1, 5):
        env["SIMULATE_MAPPER_FAILURE"] = str(int(index == fail_map and attempt == 1))
        result = subprocess.run([sys.executable, str(MAPPER)], input=data,
                                capture_output=True, env=env)
        Path(run_root, f"map-{index:04d}-attempt-{attempt}.log").write_bytes(result.stderr)
        if result.returncode == 0:
            break
        print(f"Map {index}: попытка {attempt} не удалась", flush=True)
    else:
        raise RuntimeError(result.stderr.decode("utf-8", "replace"))

    buckets = {k: [[] for _ in range(REDUCERS)] for k in ks}
    produced = {k: [0, 0, 0] for k in ks}
    for line in result.stdout.splitlines(keepends=True):
        raw_k, separator, rest = line.partition(b"\t")
        if not separator:
            raise ValueError("Mapper не указал K")
        k = int(raw_k)
        if k not in buckets:
            raise ValueError(f"Mapper выдал неожиданный K={k}")
        key = rest.split(b"\t", 1)[0]
        buckets[k][zlib.crc32(key) % REDUCERS].append(rest)
        produced[k][0] += 1
        produced[k][1] += len(rest)
        produced[k][2] += int(rest.split(b"\t", 2)[1]) if combine else 1
    for k in ks:
        for r, bucket in enumerate(buckets[k]):
            bucket.sort()
            with open(Path(run_root, f"k{k}", f"map-{index:04d}-r{r:02d}"), "wb") as stream:
                stream.writelines(bucket)
    return produced


def reduce_task(task):
    r, work, minimum, combine, dictionary_path = task
    final = Path(work, f"part-{r:05d}")
    temporary = final.with_suffix(".tmp")
    with ExitStack() as stack:
        runs = [stack.enter_context(open(p, "rb")) for p in sorted(Path(work).glob(f"map-*-r{r:02d}"))]
        out = stack.enter_context(open(temporary, "wb"))
        err = stack.enter_context(open(Path(work, f"reduce-{r:02d}.log"), "wb"))
        env = dict(os.environ, MIN_SALARIES=str(minimum), MAP_COMBINE=str(int(combine)))
        if dictionary_path:
            env["SKILL_DICT"] = dictionary_path
        with subprocess.Popen([sys.executable, str(REDUCER)], stdin=subprocess.PIPE,
                              stdout=out, stderr=err, env=env) as process:
            try:
                for line in heapq.merge(*runs):
                    process.stdin.write(line)
                process.stdin.close()
            except BrokenPipeError:
                pass
            if process.wait() != 0:
                raise RuntimeError(f"Reducer {r} завершился с ошибкой; см. reduce-{r:02d}.log")
    temporary.replace(final)


def iter_results(work):
    if not Path(work, "_SUCCESS").exists():
        raise RuntimeError("Расчёт не завершён: нет _SUCCESS")
    for file in sorted(Path(work).glob("part-[0-9][0-9][0-9][0-9][0-9]")):
        with file.open(encoding="utf-8") as stream:
            for line in stream:
                key, vacancies, salaries, median = line.rstrip("\n").split("\t")
                yield key, int(vacancies), int(salaries), float(median) if median else None


def read_result(work):
    stacks = vacancies = salaries = 0
    median_sum = 0.0
    top = []
    for key, n, paid, median in iter_results(work):
        stacks += 1
        vacancies += n
        salaries += paid
        median_sum += median if median is not None else 0.0
        top.append((n, key, paid, median))
        if len(top) > 10000:
            top = heapq.nlargest(5, top, key=lambda row: (row[0], row[1]))
    return dict(stacks=stacks, vacancy_occurrences=vacancies, salary_occurrences=salaries, median_sum=median_sum), heapq.nlargest(5, top, key=lambda row: (row[0], row[1]))


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="Локальный CSV или s3://bucket/key")
    parser.add_argument("k", nargs="*", type=int, default=[1, 2, 3], choices=[1, 2, 3])
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--split-bytes", type=int, default=SPLIT, help="Для проверки границ; обычно 67108864")
    parser.add_argument("--min-salaries", type=int, default=30)
    parser.add_argument("--unknown-gross-as-net", type=int, choices=[0,1], default=1)
    parser.add_argument("--fail-map", type=int, default=-1, help="Имитировать сбой первой попытки Map")
    parser.add_argument("--combine", action="store_true", help="Локальная агрегация Map-вывода")
    parser.add_argument("--skill-ids", action="store_true", help="Числовые ID навыков в shuffle")
    parser.add_argument("--single-pass", action="store_true",
                        help="Один запуск mapper на блок сразу для всех выбранных K")
    args = parser.parse_args()
    if min(args.workers, args.split_bytes, args.min_salaries) < 1:
        parser.error("Числовые параметры должны быть положительными")
    if args.single_pass and len(set(args.k)) != len(args.k):
        parser.error("В режиме --single-pass значения K не должны повторяться")
    source = describe_source(args.path)
    print(f"Источник: {source['location']}; размер {source['size']:,} байт", flush=True)
    start_time = time.perf_counter()
    watch = MemoryWatch()
    watch.start()
    rows, skill_names = validate_csv(source, collect_skills=args.skill_ids)
    validation_seconds = time.perf_counter() - start_time
    print(f"CSV проверен: {rows} записей, переносов внутри записей нет", flush=True)
    run_root = HERE / "mr_work" / (time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6])
    run_root.mkdir(parents=True)
    dictionary_path = str(run_root / "skill-dictionary.json") if args.skill_ids else None
    if dictionary_path:
        Path(dictionary_path).write_text(json.dumps(skill_names, ensure_ascii=False), encoding="utf-8")
        print(f"Словарь: {len(skill_names):,} навыков", flush=True)
    report = dict(input=source["location"], source=source, input_rows=rows, split_bytes=args.split_bytes,
                  reducers=REDUCERS, workers=args.workers, results={},
                  min_salaries=args.min_salaries,
                  unknown_gross_as_net=bool(args.unknown_gross_as_net), fail_map=args.fail_map,
                  combine=args.combine, skill_ids=args.skill_ids,
                  dictionary_size=len(skill_names), dictionary_path=dictionary_path,
                  single_pass=args.single_pass, validation_seconds=validation_seconds)
    size = source["size"]
    map_seconds = reduce_seconds = 0.0
    with multiprocessing.get_context("spawn").Pool(args.workers) as pool:
        if args.single_pass:
            works = {k: run_root / f"k{k}" for k in args.k}
            for work in works.values():
                work.mkdir()
            tasks = [(i,source,start,min(start+args.split_bytes,size),tuple(args.k),
                      str(run_root),bool(args.unknown_gross_as_net),args.fail_map,
                      args.combine,dictionary_path)
                     for i,start in enumerate(range(0,size,args.split_bytes))]
            stage_start = time.perf_counter()
            produced = pool.map(map_task_single_pass, tasks, chunksize=1)
            map_seconds += time.perf_counter() - stage_start
            stage_start = time.perf_counter()
            pool.map(reduce_task,
                     [(r,str(works[k]),args.min_salaries,args.combine,dictionary_path)
                      for k in args.k for r in range(REDUCERS)], chunksize=1)
            reduce_seconds += time.perf_counter() - stage_start
            for k in args.k:
                work = works[k]
                Path(work, "_SUCCESS").touch()
                summary, top = read_result(work)
                summary.update(map_tasks=len(tasks),
                               map_records=sum(item[k][0] for item in produced),
                               shuffle_bytes=sum(item[k][1] for item in produced),
                               emitted=sum(item[k][2] for item in produced),
                               directory=str(work))
                if summary["emitted"] != summary["vacancy_occurrences"]:
                    raise RuntimeError(f"K={k}: число записей Map и Reduce не совпало")
                report["results"][str(k)] = summary
                print(f"K={k}: map-задач {len(tasks)}, вхождений {summary['emitted']:,}, "
                      f"строк shuffle {summary['map_records']:,}, "
                      f"shuffle {summary['shuffle_bytes']/2**20:.1f} МиБ, "
                      f"редьюсеров {REDUCERS}", flush=True)
                for n,key,paid,median in top:
                    print(f"  {key}: вакансий {n}, зарплат {paid}, медиана {median}", flush=True)
        else:
            for k in args.k:
                work = run_root / f"k{k}"
                work.mkdir()
                tasks = [(i,source,start,min(start+args.split_bytes,size),k,str(work),
                          bool(args.unknown_gross_as_net),args.fail_map,args.combine,dictionary_path)
                         for i,start in enumerate(range(0,size,args.split_bytes))]
                stage_start = time.perf_counter()
                produced = pool.map(map_task, tasks, chunksize=1)
                map_seconds += time.perf_counter() - stage_start
                stage_start = time.perf_counter()
                pool.map(reduce_task, [(r,str(work),args.min_salaries,args.combine,dictionary_path)
                                       for r in range(REDUCERS)], chunksize=1)
                reduce_seconds += time.perf_counter() - stage_start
                Path(work, "_SUCCESS").touch()
                summary, top = read_result(work)
                summary.update(map_tasks=len(tasks), map_records=sum(n for n,b,c in produced),
                               emitted=sum(c for n,b,c in produced),
                               shuffle_bytes=sum(b for n,b,c in produced), directory=str(work))
                if summary['emitted'] != summary['vacancy_occurrences']:
                    raise RuntimeError("Число записей Map и Reduce не совпало")
                report['results'][str(k)] = summary
                print(f"K={k}: map-задач {len(tasks)}, вхождений {summary['emitted']:,}, "
                      f"строк shuffle {summary['map_records']:,}, "
                      f"shuffle {summary['shuffle_bytes']/2**20:.1f} МиБ, редьюсеров {REDUCERS}", flush=True)
                for n,key,paid,median in top:
                    print(f"  {key}: вакансий {n}, зарплат {paid}, медиана {median}", flush=True)
    if source["kind"] == "s3" and describe_source(args.path) != source:
        raise RuntimeError("Объект RustFS изменился во время MapReduce; повторите расчёт")
    report['seconds'] = time.perf_counter()-start_time
    report.update(map_seconds=map_seconds, reduce_seconds=reduce_seconds)
    watch.stop()
    report.update(peak_memory_bytes=watch.peak, memory_metric=watch.metric,
                  memory_samples=watch.samples, sampling_interval_seconds=0.05)
    print(f"\nСвой MapReduce: {report['seconds']:.2f} с; "
          f"пик памяти {watch.peak/2**30:.2f} ГиБ ({watch.metric}, процесс и потомки)", flush=True)
    for k, result in report['results'].items():
        print(f"K={k}: стеков {result['stacks']:,}, вхождений вакансий {result['vacancy_occurrences']:,}, "
              f"зарплат {result['salary_occurrences']:,}, сумма медиан {result['median_sum']:,.1f}", flush=True)
    (run_root/'summary.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    # Указатель обновляется только после успешного окончания всех K.
    (HERE/'mr_work/latest.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    for k, result in report['results'].items():
        (HERE / 'mr_work' / f'latest-k{k}.json').write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"Результаты: {run_root}", flush=True)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
