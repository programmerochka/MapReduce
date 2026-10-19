"""Mapper: предобработка одной вакансии. Без Pandas.

Читает CSV из stdin, печатает ключ<TAB>зарплата в stdout.
"""
import math
import csv
import sys
import os
import re
import unicodedata
import json
from collections import Counter

PATTERN_1C = r"(?<!\w)1\s*[сc](?![a-zа-яё])"

PATTERN_DEV = r"программист|разработчик|developer|programmer"

INVISIBLE = r"[\u200b\u200c\u200d\ufeff\u2060]"

ALIASES = {
    "1с erp": "1с:erp",
    "1с зуп": "1с:зарплата и управление персоналом",
    "1с:зуп": "1с:зарплата и управление персоналом",
    "1с упп": "1с:управление производственным предприятием",
    "1с:упп": "1с:управление производственным предприятием",
    "1с ух": "1с:управление холдингом",
    "1с:ух": "1с:управление холдингом",
    "ms sql": "microsoft sql server",
    "ms sql server": "microsoft sql server",
    "с#": "c#",
}

def normalize_skill(text):
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(INVISIBLE, "", text)
    text = re.sub(r"\s+", " ", text).strip().casefold()
    text = re.sub(r"^(?:[-–—•·*]|\.)\s+", "", text)
    text = text.rstrip(" ,;.")
    text = re.sub(PATTERN_1C, "1с", text)
    text = re.sub(r"\s*:\s*", ":", text)
    return ALIASES.get(text, text)


def in_segment(row):
    """Наш текущий вариант: роль программиста, 1С и разработка в названии."""
    title = row.get("name") or ""
    return (
        (row.get("role") or "").strip() == "Программист, разработчик"
        and re.search(PATTERN_1C, title, re.IGNORECASE) is not None
        and re.search(PATTERN_DEV, title, re.IGNORECASE) is not None
    )


def skills_of(row):
    """Уникальные очищенные навыки, отсортированные внутри вакансии."""
    return sorted({
        skill for raw in (row.get("key_skills") or "").split(";")
        if (skill := normalize_skill(raw))
    })


def salary_net(row, unknown_gross_as_net=True):
    """Нижняя граница в рублях; None означает отсутствие пригодной зарплаты."""
    bounds = []
    for column in ("salary_from", "salary_to"):
        raw = (row.get(column) or "").strip()
        if not raw:
            bounds.append(None)
            continue
        try:
            value = float(raw)
        except ValueError:
            return None
        if not math.isfinite(value) or value <= 0:
            return None
        bounds.append(value)
    lower, upper = bounds
    if lower is None or (upper is not None and lower > upper):
        return None
    if (row.get("currency") or "").strip().upper() != "RUR":
        return None
    gross = (row.get("gross") or "").strip().lower()
    if gross == "true":
        return lower * 87 // 100
    if gross == "false" or (not gross and unknown_gross_as_net):
        return lower
    return None


def prepare_row(row, unknown_gross_as_net=True):
    """Одна CSV-запись → подготовленная вакансия либо None вне сегмента."""
    if not in_segment(row):
        return None
    return {
        "source": row.get("source", ""),
        "vacancy_id": row.get("vacancy_id", ""),
        "skills": skills_of(row),
        "net": salary_net(row, unknown_gross_as_net),
    }


import itertools

COLUMNS = "source vacancy_id status snapshot_date published_at area employer_id employer_inn employer_ogrn employer_name name role sphere salary_from salary_to currency gross experience schedule employment key_skills description url".split()


def encode_skill(skill):
    # Разделитель внутри самого навыка не должен смешивать разные стеки.
    return skill.replace("%", "%25").replace("|", "%7C")


def main():
    sys.stderr.reconfigure(encoding="utf-8")
    if os.environ.get("SIMULATE_MAPPER_FAILURE") == "1":
        print("Имитирован отказ mapper до публикации результата", file=sys.stderr)
        raise SystemExit(1)
    k_list = os.environ.get("K_LIST")
    ks = tuple(int(value) for value in k_list.split(",")) if k_list else (
        int(os.environ.get("K", "1")),)
    if not ks or len(set(ks)) != len(ks) or any(k not in (1, 2, 3) for k in ks):
        raise ValueError("K_LIST должен содержать уникальные значения из 1, 2, 3")
    single_pass = k_list is not None
    unknown = os.environ.get("UNKNOWN_GROSS_AS_NET", "1") == "1"
    combine = os.environ.get("MAP_COMBINE", "0") == "1"
    dictionary_path = os.environ.get("SKILL_DICT")
    skill_ids = None
    if dictionary_path:
        with open(dictionary_path, encoding="utf-8") as stream:
            skill_ids = {skill: index for index, skill in enumerate(json.load(stream))}
    combined = {}

    def flush():
        for group_key, (count, paid) in combined.items():
            prefix, key = (f"{group_key[0]}\t", group_key[1]) if single_pass else ("", group_key)
            payload = ";".join(f"{salary}:{n}" for salary, n in paid.items())
            sys.stdout.write(f"{prefix}{key}\t{count}\t{payload}\n")
        combined.clear()

    sys.stdin.reconfigure(encoding="utf-8-sig", newline="")
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    csv.field_size_limit(100_000_000)
    vacancies = 0
    for fields in csv.reader(sys.stdin, strict=True):
        if not fields or fields == COLUMNS:
            continue
        if len(fields) != len(COLUMNS):
            raise ValueError(f"Ожидалось {len(COLUMNS)} столбцов, получено {len(fields)}")
        prepared = prepare_row(dict(zip(COLUMNS, fields)), unknown)
        if prepared is None or not prepared["skills"]:
            continue
        vacancies += 1
        salary = "" if prepared["net"] is None else format(prepared["net"], ".17g")
        for k in ks:
            for stack in itertools.combinations(prepared["skills"], k):
                key = ("|".join(str(skill_ids[skill]) for skill in stack)
                       if skill_ids is not None else
                       "|".join(encode_skill(skill) for skill in stack))
                if combine:
                    group_key = (k, key) if single_pass else key
                    entry = combined.get(group_key)
                    if entry is None:
                        entry = [0, Counter()]
                        combined[group_key] = entry
                    entry[0] += 1
                    if salary:
                        entry[1][salary] += 1
                    if len(combined) >= 10000:
                        flush()
                else:
                    prefix = f"{k}\t" if single_pass else ""
                    sys.stdout.write(f"{prefix}{key}\t{salary}\n")
    if combine:
        flush()
    sys.stdout.flush()
    print(f"reporter:counter:skills,vacancies_with_skills,{vacancies}", file=sys.stderr)


if __name__ == "__main__":
    main()