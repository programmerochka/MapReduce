"""Сравнение двух методов по всем ключам; Pandas используется только для показа таблиц."""
import json
from pathlib import Path
import duckdb

HERE=Path(__file__).resolve().parent


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def compare_files(mr_folder, parquet):
    """Полное сравнение конкретных результатов, а не поиск произвольного последнего запуска."""
    folder = Path(mr_folder)
    if not (folder / '_SUCCESS').exists():
        raise ValueError('MapReduce не завершён')
    files = [str(p) for p in sorted(folder.glob('part-[0-9][0-9][0-9][0-9][0-9]'))]
    if len(files) != 8:
        raise ValueError('Ожидались 8 результатов редьюсеров')
    con = duckdb.connect()
    try:
        nonempty = [name for name in files if Path(name).stat().st_size]
        if nonempty:
            con.read_csv(nonempty, header=False, delimiter='\t', quotechar='', auto_detect=False,
                columns={'stack':'VARCHAR','vacancies':'BIGINT','salaries':'BIGINT','median_net':'DOUBLE'},
                na_values='').create_view('mr')
        else:
            con.execute('CREATE TEMP TABLE mr(stack VARCHAR, vacancies BIGINT, salaries BIGINT, median_net DOUBLE)')
        con.read_parquet(str(parquet)).create_view('db')
        for table in ('mr', 'db'):
            if not con.execute(f'SELECT count(*)=count(DISTINCT stack) FROM {table}').fetchone()[0]:
                raise ValueError(f'{table}: повтор ключа')
        return con.execute("""SELECT count(*) FROM mr FULL OUTER JOIN db USING(stack)
            WHERE mr.vacancies IS DISTINCT FROM db.vacancies
               OR mr.salaries IS DISTINCT FROM db.salaries
               OR mr.median_net IS DISTINCT FROM db.median_net""").fetchone()[0]
    finally:
        con.close()


def correctness(expected_input, require_same_source=True):
    rows=[]
    for k in (1,2,3):
        mr=load(HERE/'mr_work'/f'latest-k{k}.json')
        sql=load(HERE/'duckdb_work'/f'latest-k{k}.json')
        folder=Path(mr['directory'])
        mr_report=load(folder.parent/'summary.json')
        db_report=load(sql['summary'])
        if require_same_source:
            if mr_report['input']!=expected_input or db_report['input']!=expected_input:
                raise ValueError(f'K={k}: сначала выполните оба метода для {expected_input}')
            if expected_input.startswith('s3://'):
                for field in ('etag','size','version_id','endpoint'):
                    if mr_report['source'].get(field)!=db_report['source'].get(field):
                        raise ValueError(f'K={k}: методы прочитали разные объекты ({field})')
            for field, default in [('min_salaries', 30), ('unknown_gross_as_net', True)]:
                if mr_report.get(field, default) != db_report.get(field, default):
                    raise ValueError(f'K={k}: различаются правила расчёта ({field})')
        bad=compare_files(folder, sql['parquet'])
        rows.append({'K':k,'Стеков MapReduce':mr['stacks'],'Стеков DuckDB':sql['stacks'],
            'Вхождений MapReduce':mr['vacancy_occurrences'],'Вхождений DuckDB':sql['vacancy_occurrences'],
            'Несовпавших ключей/метрик':bad,'Совпало':bad==0})
    return rows


def performance(expected_input):
    rows=[]
    for tool, directory in [('MapReduce','mr_work'),('DuckDB','duckdb_work')]:
        for sizes,label in [({'1','2'},'Навыки + пары'),({'3'},'Тройки')]:
            matching=[]
            for file in (HERE/directory).glob('*/summary.json'):
                report=load(file)
                if report['input']==expected_input and set(report['results'])==sizes:
                    matching.append((file.stat().st_mtime,report))
            report=max(matching,key=lambda x:x[0])[1] if matching else None
            rows.append({'Метод':tool,'Расчёт':label,
                'Время, с':round(report['seconds'],2) if report else None,
                'Пик памяти, ГиБ':round(report['peak_memory_bytes']/2**30,3) if report else None,
                'Метрика памяти':report.get('memory_metric') if report else None})
    return rows