"""Compare MapReduce optimizations using one unchanged object in RustFS.

Run from the notebook after entering RustFS keys, or from a shell with the
same S3 environment variables:
    python benchmark_rustfs.py s3://dataset/it_vacancies_2024.csv --workers 8 --rounds 3
"""
import argparse
import csv
import json
import statistics
import subprocess
import sys
from pathlib import Path

from step_mapreduce import describe_source, iter_results


HERE = Path(__file__).resolve().parent
MODES = {
    "Базовый": (),
    "Агрегация": ("--combine",),
    "ID навыков": ("--skill-ids",),
    "Агрегация + ID": ("--combine", "--skill-ids"),
    "Один проход": ("--single-pass",),
}


def read_results(folder):
    return {
        k: {key: (vacancies, salaries, median)
            for key, vacancies, salaries, median in iter_results(folder / f"k{k}")}
        for k in (1, 2, 3)
    }


def run(source, workers, flags):
    command = [sys.executable, str(HERE / "step_mapreduce.py"), source,
               "1", "2", "3", "--workers", str(workers), *flags]
    process = subprocess.run(command, cwd=HERE, capture_output=True, text=True,
                             encoding="utf-8", errors="replace")
    if process.returncode:
        raise RuntimeError("MapReduce завершился с ошибкой:\n"
                           + process.stdout[-4000:] + process.stderr[-4000:])
    marker = "Результаты: "
    folder = next((Path(line[len(marker):]) for line in process.stdout.splitlines()
                   if line.startswith(marker)), None)
    if folder is None:
        raise RuntimeError("MapReduce не указал папку результатов")
    report = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
    return folder, report


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="Только s3://bucket/key из RustFS")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if not args.source.startswith("s3://"):
        parser.error("Для итогового сравнения требуется объект s3://... из RustFS")
    if args.workers < 1 or args.rounds < 1:
        parser.error("workers и rounds должны быть положительными")

    source = describe_source(args.source)
    if source["kind"] != "s3":
        raise AssertionError("Источник должен быть RustFS")
    print(f"Объект: {source['location']}; {source['size']} байт; "
          f"ETag {source['etag']}", flush=True)

    pointers = [HERE / "mr_work" / name for name in
                ("latest.json", "latest-k1.json", "latest-k2.json", "latest-k3.json")]
    previous = {path: path.read_bytes() if path.exists() else None for path in pointers}
    out = HERE / "rustfs_bonus_benchmark.csv"
    out.unlink(missing_ok=True)
    rows = []
    overall_reference = None
    try:
        for round_number in range(1, args.rounds + 1):
            # Меняем порядок, чтобы первый запуск не всегда приходился на базовый режим.
            order = list(MODES)
            shift = (round_number - 1) % len(order)
            order = order[shift:] + order[:shift]
            reference = None
            for mode in order:
                folder, report = run(args.source, args.workers, MODES[mode])
                if report["source"] != source:
                    raise AssertionError(f"{mode}: объект RustFS изменился между запусками")
                data = read_results(folder)
                if reference is None:
                    reference = data
                elif data != reference:
                    for k in (1, 2, 3):
                        if data[k] != reference[k]:
                            raise AssertionError(f"Раунд {round_number}, {mode}, K={k}: "
                                                 "различаются ключи, частоты или медианы")
                if overall_reference is None:
                    overall_reference = data
                elif data != overall_reference:
                    raise AssertionError(f"Раунд {round_number}, {mode}: "
                                         "результат отличается от первого раунда")
                shuffle = sum(report["results"][str(k)]["shuffle_bytes"]
                              for k in (1, 2, 3))
                rows.append({
                    "Раунд": round_number,
                    "Вариант": mode,
                    "Источник": source["location"],
                    "Endpoint": source["endpoint"],
                    "Размер, байт": source["size"],
                    "ETag": source["etag"],
                    "Процессов": args.workers,
                    "Время, с": round(report["seconds"], 3),
                    "Проверка CSV, с": round(report["validation_seconds"], 3),
                    "Map, с": round(report["map_seconds"], 3),
                    "Reduce, с": round(report["reduce_seconds"], 3),
                    "Пик памяти, ГиБ": round(report["peak_memory_bytes"] / 2**30, 3),
                    "Shuffle, МиБ": round(shuffle / 2**20, 2),
                    "Запусков Map": (report["results"]["1"]["map_tasks"]
                                    if report["single_pass"] else
                                    sum(report["results"][str(k)]["map_tasks"]
                                        for k in (1, 2, 3))),
                    "Все результаты совпали": True,
                    "Папка": str(folder),
                })
                print(f"Раунд {round_number}, {mode}: {report['seconds']:.2f} с; "
                      f"пик {report['peak_memory_bytes']/2**30:.2f} ГиБ; "
                      f"shuffle {shuffle/2**20:.1f} МиБ", flush=True)
            print(f"Раунд {round_number}: каждый ключ, частота и медиана K=1,2,3 "
                  "совпали во всех пяти режимах", flush=True)
            if describe_source(args.source) != source:
                raise AssertionError("Объект RustFS изменился в ходе сравнения")
    finally:
        for path, contents in previous.items():
            if contents is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(contents)

    with out.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0])
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nМедианы {args.rounds} раундов:", flush=True)
    for mode in MODES:
        values = [row["Время, с"] for row in rows if row["Вариант"] == mode]
        print(f"  {mode}: {statistics.median(values):.2f} с", flush=True)
    print(f"Таблица: {out}", flush=True)


if __name__ == "__main__":
    main()