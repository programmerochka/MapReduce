"""Отсортированный ключ<TAB>зарплата → частота, число зарплат, медиана."""
import math
import os
import statistics
import sys
import json
from collections import Counter


def weighted_median(salaries, count):
    targets = ((count - 1) // 2, count // 2)
    seen = 0
    selected = []
    for salary, occurrences in sorted(salaries.items()):
        while len(selected) < 2 and seen <= targets[len(selected)] < seen + occurrences:
            selected.append(salary)
        seen += occurrences
        if len(selected) == 2:
            return (selected[0] + selected[1]) / 2
    raise ValueError("Недостаточно зарплат для медианы")


def emit(stdout, key, vacancies, salaries, minimum, combine=False, names=None):
    paid = sum(salaries.values()) if combine else len(salaries)
    value = weighted_median(salaries, paid) if combine and paid >= minimum else (
        statistics.median(salaries) if paid >= minimum else None)
    median = format(value, ".17g") if value is not None else ""
    if names is not None:
        key = "|".join(names[int(part)].replace("%", "%25").replace("|", "%7C")
                       for part in key.split("|"))
    stdout.write(f"{key}\t{vacancies}\t{paid}\t{median}\n")


def main():
    minimum = int(os.environ.get("MIN_SALARIES", "30"))
    if minimum < 1:
        raise ValueError("MIN_SALARIES должен быть положительным")
    sys.stdin.reconfigure(encoding="utf-8", newline="")
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    combine = os.environ.get("MAP_COMBINE", "0") == "1"
    dictionary_path = os.environ.get("SKILL_DICT")
    with open(dictionary_path, encoding="utf-8") if dictionary_path else open(os.devnull) as stream:
        names = json.load(stream) if dictionary_path else None
    current, vacancies, salaries = None, 0, Counter() if combine else []
    for line in sys.stdin:
        fields = line.rstrip("\r\n").split("\t")
        key = fields[0]
        value = fields[-1]
        tab = len(fields) == (3 if combine else 2)
        if not tab or not key:
            raise ValueError("Ожидалось ключ<TAB>зарплата")
        if key != current:
            if current is not None:
                emit(sys.stdout, current, vacancies, salaries, minimum, combine, names)
            current, vacancies, salaries = key, 0, Counter() if combine else []
        vacancies += int(fields[1]) if combine else 1
        if value:
            for item in value.split(";") if combine else [value]:
                raw_salary, raw_count = item.rsplit(":", 1) if combine else (item, "1")
                salary = float(raw_salary)
                if not math.isfinite(salary):
                    raise ValueError("Некорректная зарплата")
                if combine:
                    salaries[salary] += int(raw_count)
                else:
                    salaries.append(salary)
    if current is not None:
        emit(sys.stdout, current, vacancies, salaries, minimum, combine, names)
    sys.stdout.flush()


if __name__ == "__main__":
    main()