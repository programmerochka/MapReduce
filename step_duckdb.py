"""Независимый SQL-расчёт навыков, пар и троек; CSV → SQL → Parquet."""
import argparse
import json
import os
from pathlib import Path
import sys
import time
import unicodedata
import uuid
from urllib.parse import urlsplit
import duckdb
# Только измерение ресурсов, не обработка вакансий и не результаты MapReduce.
from step_mapreduce import MemoryWatch

HERE = Path(__file__).resolve().parent


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def configure_s3(con):
    endpoint = urlsplit(os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"))
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    # Секрет непостоянный, хранится только в памяти соединения. SQL не печатаем.
    access = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access or not secret:
        raise RuntimeError("Сначала введите ключи в ячейке подключения к RustFS")
    statement = ("CREATE OR REPLACE SECRET rustfs (TYPE s3, KEY_ID " + literal(access)
        + ", SECRET " + literal(secret) + ", REGION "
        + literal(os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        + ", ENDPOINT " + literal(endpoint.netloc)
        + ", URL_STYLE 'path', USE_SSL " + str(endpoint.scheme == 'https').lower() + ")")
    try:
        con.execute(statement)
    except Exception:
        raise RuntimeError("Не удалось настроить доступ DuckDB к RustFS; проверьте параметры подключения") from None


def object_identity(uri):
    # HEAD используется только для идентификации объекта, сам CSV читает DuckDB.
    import boto3
    from botocore.config import Config
    parsed=urlsplit(uri)
    client=boto3.client('s3',endpoint_url=os.environ.get('S3_ENDPOINT_URL','http://localhost:9000'),
        region_name=os.environ.get('AWS_DEFAULT_REGION','us-east-1'),
        config=Config(signature_version='s3v4',s3={'addressing_style':'path'}))
    head=client.head_object(Bucket=parsed.netloc,Key=parsed.path.lstrip('/'))
    return dict(etag=head['ETag'],size=head['ContentLength'],version_id=head.get('VersionId'),
                endpoint=os.environ.get('S3_ENDPOINT_URL','http://localhost:9000'))


def configure_sql(con):
    # Unicode NFKC и casefold отсутствуют в таком сочетании в SQL DuckDB.
    # Эти две UDF — только операции над строкой; бизнес-правила ниже реализованы в SQL.
    con.create_function('unicode_nfkc',lambda s:unicodedata.normalize('NFKC',s),['VARCHAR'],'VARCHAR')
    con.create_function('unicode_fold',lambda s:s.casefold(),['VARCHAR'],'VARCHAR')
    aliases = [
        ('1с erp','1с:erp'),('1с зуп','1с:зарплата и управление персоналом'),
        ('1с:зуп','1с:зарплата и управление персоналом'),
        ('1с упп','1с:управление производственным предприятием'),
        ('1с:упп','1с:управление производственным предприятием'),
        ('1с ух','1с:управление холдингом'),('1с:ух','1с:управление холдингом'),
        ('ms sql','microsoft sql server'),('ms sql server','microsoft sql server'),('с#','c#')]
    con.execute('CREATE TABLE aliases(src VARCHAR, dst VARCHAR)')
    con.executemany('INSERT INTO aliases VALUES (?,?)',aliases)
    invisible='[\u200b\u200c\u200d\ufeff\u2060]'
    # Unicode-пробелы Python \s явно перечислены: RE2 \s уже по составу.
    spaces='[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]'
    con.execute(f"CREATE MACRO clean_spaces(s) AS regexp_replace(s, {literal(spaces+'+')}, ' ', 'g')")
    con.execute(f"CREATE MACRO remove_invisible(s) AS regexp_replace(s, {literal(invisible)}, '', 'g')")
    con.execute("CREATE MACRO strip_text(s) AS trim(clean_spaces(s))")
    con.execute(r"CREATE MACRO clean_edges(s) AS rtrim(regexp_replace(s, '^(?:[-–—•·*]|\.) +', ''), ' ,;.')")
    con.execute(r"CREATE MACRO clean_1c(s) AS regexp_replace(s, '(^|[^\p{L}\p{N}_])1 *[сc]([^a-zа-яё]|$)', '\11с\2', 'g')")


def prepared_cte(path, unknown):
    """Единые SQL-правила; одна строка prepared — одна вакансия, в том числе без навыков."""
    return f"""
WITH raw AS (
    SELECT row_number() OVER () AS row_id, *
    FROM read_csv({literal(path)}, header=true, all_varchar=true, sample_size=-1)
), selected AS (
    SELECT *, nullif(strip_text(salary_from),'') AS lo_text,
              nullif(strip_text(salary_to),'') AS hi_text,
              lower(strip_text(coalesce(gross,''))) AS gross_clean
    FROM raw
    WHERE strip_text(role) = 'Программист, разработчик'
      AND regexp_matches(lower(name), '(^|[^\\p{{L}}\\p{{N}}_])1\\s*[сc]([^a-zа-яё]|$)')
      AND regexp_matches(lower(name), 'программист|разработчик|developer|programmer')
), bounds AS (
    SELECT *, try_cast(lo_text AS DOUBLE) AS lo, try_cast(hi_text AS DOUBLE) AS hi
    FROM selected
), vacancies AS (
    SELECT row_id, area, experience, name, key_skills,
      CASE WHEN upper(strip_text(currency))='RUR' AND lo>0 AND isfinite(lo)
        AND (hi_text IS NULL OR (hi>0 AND isfinite(hi) AND lo<=hi))
        AND (gross_clean IN ('true','false') OR ({str(unknown).lower()} AND gross_clean=''))
      THEN CASE WHEN gross_clean='true' THEN floor(lo*87/100) ELSE lo END END AS salary
    FROM bounds
), expanded AS (
    SELECT row_id, salary, unnest(string_split(key_skills,';')) AS raw_skill FROM vacancies
), cleaned AS (
    SELECT row_id, salary,
      regexp_replace(clean_1c(clean_edges(unicode_fold(trim(clean_spaces(remove_invisible(unicode_nfkc(raw_skill))))))),
                     ' *: *', ':', 'g') AS skill
    FROM expanded
), normalized AS (
    SELECT DISTINCT row_id, salary, coalesce(aliases.dst,skill) AS skill
    FROM cleaned LEFT JOIN aliases ON skill=aliases.src WHERE skill<>''
), lists AS (
    SELECT row_id, salary, list_sort(list(skill)) AS skills
    FROM normalized GROUP BY row_id,salary
), prepared AS (
    SELECT v.row_id, v.area, v.experience, v.name, v.salary,
           coalesce(l.skills, []::VARCHAR[]) AS skills
    FROM vacancies v LEFT JOIN lists l USING(row_id)
)
"""


def stack_expression(k):
    """Индексы i<j<l исключают повторы и перестановки внутри вакансии."""
    cross = 'CROSS JOIN unnest(range(1, len(skills)+1)) AS a(i)'
    terms = ['skills[i]']
    if k >= 2:
        cross += ' CROSS JOIN unnest(range(i+1,len(skills)+1)) AS b(j)'
        terms += ['skills[j]']
    if k == 3:
        cross += ' CROSS JOIN unnest(range(j+1,len(skills)+1)) AS c(l)'
        terms += ['skills[l]']
    stack = " || '|' || ".join(
        "replace(replace(" + term + ", '%', '%25'), '|', '%7C')" for term in terms)
    return stack, cross


def query(path, k, minimum, unknown):
    stack, cross = stack_expression(k)
    return prepared_cte(path, unknown) + f"""
, stacks AS (
    SELECT {stack} AS stack, salary FROM prepared {cross}
)
SELECT stack, count(*) AS vacancies, count(salary) AS salaries,
       CASE WHEN count(salary)>={minimum} THEN median(salary) END AS median_net
FROM stacks GROUP BY stack
"""


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path')
    parser.add_argument('k',nargs='*',type=int,choices=[1,2,3],default=[1,2])
    parser.add_argument('--min-salaries',type=int,default=30)
    parser.add_argument('--unknown-gross-as-net',type=int,choices=[0,1],default=1)
    args=parser.parse_args()
    if args.min_salaries<1:parser.error('min-salaries должен быть положительным')
    location=args.path if args.path.startswith('s3://') else str(Path(args.path).resolve())
    folder=HERE/'duckdb_work'/(time.strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:6])
    folder.mkdir(parents=True)
    con=duckdb.connect()
    extensions=HERE/'.duckdb_extensions'
    extensions.mkdir(exist_ok=True)
    con.execute('SET extension_directory = '+literal(extensions.as_posix()))
    limit=os.environ.get('DUCKDB_MEMORY','2GB')
    con.execute('SET memory_limit = '+literal(limit))
    con.execute('SET threads = 4')
    con.execute('SET temp_directory = '+literal((folder/'tmp').as_posix()))
    identity=None
    if location.startswith('s3://'):
        configure_s3(con)
        identity=object_identity(location)
    configure_sql(con)
    watch=MemoryWatch(); watch.start(); started=time.perf_counter()
    report=dict(input=location,source=identity,memory_limit=limit,threads=4,
                tool='DuckDB',results={},min_salaries=args.min_salaries,
                unknown_gross_as_net=bool(args.unknown_gross_as_net))
    try:
        for k in args.k:
            sql=query(location,k,args.min_salaries,bool(args.unknown_gross_as_net))
            (folder/f'query-k{k}.sql').write_text(sql,encoding='utf-8')
            con.execute('CREATE OR REPLACE TABLE result AS '+sql)
            out=folder/f'stacks_{k}.parquet'
            con.execute('COPY result TO '+literal(out.as_posix())+' (FORMAT PARQUET)')
            a,b,c,d=con.execute('SELECT count(*), coalesce(sum(vacancies),0), coalesce(sum(salaries),0), coalesce(sum(median_net),0) FROM result').fetchone()
            report['results'][str(k)]=dict(stacks=int(a),vacancy_occurrences=int(b),salary_occurrences=int(c),median_sum=float(d),parquet=str(out))
            print(f'\nТоп стеков K={k}:',flush=True)
            for row in con.execute('SELECT * FROM result ORDER BY vacancies DESC,stack LIMIT 5').fetchall():print(row,flush=True)
        if identity is not None and object_identity(location)!=identity:
            raise RuntimeError('Объект изменился во время SQL-расчёта; повторите запуск на неизменяемом CSV')
    finally:
        watch.stop()
        con.close()
    report.update(seconds=time.perf_counter()-started,peak_memory_bytes=watch.peak,memory_metric=watch.metric)
    print(f"\nDuckDB (лимит {limit}): {report['seconds']:.2f} с; пик памяти {watch.peak/2**30:.2f} ГиБ ({watch.metric})",flush=True)
    for k,r in report['results'].items():print(f"K={k}: стеков {r['stacks']:,}, вхождений {r['vacancy_occurrences']:,}, зарплат {r['salary_occurrences']:,}, сумма медиан {r['median_sum']:,.1f}",flush=True)
    (folder/'summary.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    for k,r in report['results'].items():
        (HERE/'duckdb_work'/f'latest-k{k}.json').write_text(json.dumps(dict(summary=str(folder/'summary.json'),**r),ensure_ascii=False,indent=2),encoding='utf-8')
    print('Результаты:',folder,flush=True)


if __name__=='__main__':main()