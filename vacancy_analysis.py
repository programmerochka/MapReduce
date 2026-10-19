"""Зарплатные базы, проверка состава групп и региональные сочетания навыков."""
from itertools import combinations
from pathlib import Path
from urllib.parse import unquote

import duckdb
import pandas as pd

import step_duckdb


def decode_stack(key):
    return ' + '.join(unquote(part) for part in key.split('|'))


def salary_lifts(tables, k, minimum=30):
    """Пара: наиболее частый навык; тройка: наиболее частая входящая пара.

    Частота определяется по всем вакансиям, до отбора по зарплате.
    При равной частоте выбираем ключ по алфавиту. Если у выбранной базы
    недостаточно зарплат, пропускаем сочетание, а не меняем базу на менее частую.
    """
    if k not in (2, 3):
        raise ValueError('Сравнение определено для пар и троек')
    bases = tables[k - 1].set_index('stack')
    rows = []
    for row in tables[k].itertuples(index=False):
        parts = row.stack.split('|')
        candidates = ['|'.join(items) for items in combinations(parts, k - 1)]
        if any(key not in bases.index for key in candidates):
            raise ValueError(f'Нет подгруппы для {row.stack}; нужны полные таблицы K=1,2,3')
        chosen = min(candidates, key=lambda key: (-int(bases.loc[key, 'vacancies']), key))
        base = bases.loc[chosen]
        if (row.salaries < minimum or base['salaries'] < minimum
                or pd.isna(row.median_net) or pd.isna(base['median_net'])
                or base['median_net'] <= 0):
            continue
        delta = row.median_net - base['median_net']
        added = [part for part in parts if part not in chosen.split('|')]
        rows.append(dict(stack=row.stack, vacancies=int(row.vacancies), salaries=int(row.salaries),
                         median_net=float(row.median_net), base_stack=chosen,
                         base_vacancies=int(base['vacancies']), base_salaries=int(base['salaries']),
                         base_median=float(base['median_net']), added=decode_stack('|'.join(added)),
                         delta=float(delta), delta_pct=float(delta / base['median_net'] * 100)))
    return pd.DataFrame(rows, columns=[
        'stack', 'vacancies', 'salaries', 'median_net', 'base_stack', 'base_vacancies',
        'base_salaries', 'base_median', 'added', 'delta', 'delta_pct'])


def open_analysis(uri, unknown=True):
    """Один SQL-проход из RustFS; следующие анализы используют эту версию данных."""
    if not uri.startswith('s3://'):
        raise ValueError('Для этапов 5 и 6 требуется источник RustFS')
    source = step_duckdb.object_identity(uri)
    here = Path(__file__).resolve().parent
    con = duckdb.connect()
    try:
        con.execute('SET extension_directory = ' + step_duckdb.literal((here / '.duckdb_extensions').as_posix()))
        con.execute("SET memory_limit='2GB'")
        con.execute('SET threads=4')
        step_duckdb.configure_s3(con)
        step_duckdb.configure_sql(con)
        con.execute('CREATE TEMP TABLE analysis_vacancies AS '
                    + step_duckdb.prepared_cte(uri, unknown) + ' SELECT * FROM prepared')
        if step_duckdb.object_identity(uri) != source:
            raise ValueError('Объект изменился при подготовке анализа')
        return con, source
    except Exception:
        con.close()
        raise


def context_table(con, stack, base, column, minimum=30):
    """Состав зарплатных групп по опыту или территории; один объект — одна вакансия."""
    if column not in ('experience', 'area'):
        raise ValueError('Допустимы experience и area')
    parts = [unquote(s) for s in stack.split('|')]
    base_parts = [unquote(s) for s in base.split('|')]
    table = con.execute(f"""
        SELECT coalesce(nullif(trim({column}),''),'Не указано') AS group_name,
               count(salary) AS base_paid,
               count(salary) FILTER (WHERE list_has_all(skills, ?)) AS combo_paid,
               median(salary) AS base_median,
               median(salary) FILTER (WHERE list_has_all(skills, ?)) AS combo_median
        FROM analysis_vacancies WHERE salary IS NOT NULL AND list_has_all(skills, ?)
        GROUP BY group_name
        ORDER BY combo_paid DESC, group_name
    """, [parts, parts, base_parts]).df()
    table['base_share_pct'] = table['base_paid'] / table['base_paid'].sum() * 100
    table['combo_share_pct'] = table['combo_paid'] / table['combo_paid'].sum() * 100
    table['share_delta_pp'] = table['combo_share_pct'] - table['base_share_pct']
    table.loc[table['base_paid'] < minimum, 'base_median'] = float('nan')
    table.loc[table['combo_paid'] < minimum, 'combo_median'] = float('nan')
    table['delta'] = table['combo_median'] - table['base_median']
    return table


def context_observation(table, overall_delta, label):
    """Количественное наблюдение с проверкой одной из гипотез о более низкой медиане."""
    if table.empty:
        return f'{label}: нет данных для сравнения состава групп.'
    shifted = table.loc[table['share_delta_pp'].abs().idxmax()]
    text = (f"{label}: доля группы «{shifted['group_name']}» среди вакансий с зарплатой "
            f"в сочетании — {shifted['combo_share_pct']:.1f}%, в базе — "
            f"{shifted['base_share_pct']:.1f}% "
            f"(разница {shifted['share_delta_pp']:+.1f} п. п.). ")
    reliable = table.dropna(subset=['delta']).sort_values('combo_paid', ascending=False)
    if reliable.empty:
        return text + 'Внутри отдельных групп недостаточно зарплат, поэтому проверить объяснение составом групп нельзя.'
    row = reliable.iloc[0]
    text += (f"В наиболее представленной сопоставимой группе «{row['group_name']}» "
             f"разница медиан составляет {row['delta']:+,.0f} ₽ "
             f"({int(row['combo_paid'])} зарплат сочетания против {int(row['base_paid'])} в базе). ")
    if abs(row['delta']) < abs(overall_delta):
        if table['share_delta_pp'].abs().max() >= 5:
            text += ('Внутри этой группы разрыв меньше общего и доли групп различаются: '
                     'различие состава может объяснять часть снижения. Это гипотеза, а не доказанная причина.')
        else:
            text += ('Внутри этой группы разрыв меньше общего, но различия долей групп меньше 5 п. п.; '
                     'объяснение разницы составом по этому признаку пока не подтверждено.')
    elif row['delta'] < 0:
        text += ('Снижение сохраняется и внутри этой группы; только составом по этому признаку '
                 'его объяснить нельзя. Нужна также проверка работодателей и обязанностей.')
    else:
        text += ('Внутри этой группы снижения нет; общая и внутригрупповая картины различаются. '
                 'Связывать снижение с самим добавленным навыком было бы неверно.')
    return text


def regional_tables(con, minimum=30):
    con.execute("""CREATE OR REPLACE TEMP TABLE chosen_regions AS
        SELECT area, count(*) AS region_vacancies FROM analysis_vacancies
        WHERE area IS NOT NULL AND trim(area)<>''
        GROUP BY area ORDER BY region_vacancies DESC, area LIMIT 3""")
    summary = con.execute(f"""
        SELECT area, count(*) AS vacancies, count(salary) AS salaries,
               count(*) FILTER (WHERE len(skills)>0) AS with_skills,
               CASE WHEN count(salary)>={int(minimum)} THEN median(salary) END AS median_net
        FROM analysis_vacancies JOIN chosen_regions USING(area)
        GROUP BY area ORDER BY vacancies DESC,area
    """).df()
    if len(summary) != 3:
        raise ValueError('В данных меньше трёх территорий')
    summary['paid_pct'] = summary['salaries'] / summary['vacancies'] * 100
    summary['skills_pct'] = summary['with_skills'] / summary['vacancies'] * 100
    results = {}
    for k in (1, 2, 3):
        expression, cross = step_duckdb.stack_expression(k)
        table = con.execute(f"""
            WITH stacks AS (
                SELECT area, {expression} AS stack, salary
                FROM analysis_vacancies JOIN chosen_regions USING(area) {cross}
            )
            SELECT area, stack, count(*) AS vacancies, count(salary) AS salaries,
                   CASE WHEN count(salary)>={int(minimum)} THEN median(salary) END AS median_net
            FROM stacks GROUP BY area,stack
        """).df()
        table = table.merge(summary[['area', 'vacancies', 'median_net']].rename(
            columns={'vacancies': 'region_vacancies', 'median_net': 'region_median'}), on='area')
        if not (table['vacancies'] <= table['region_vacancies']).all():
            raise AssertionError('Сочетание учтено более одного раза на вакансию')
        table['share_pct'] = table['vacancies'] / table['region_vacancies'] * 100
        table['delta_region'] = table['median_net'] - table['region_median']
        table['Сочетание'] = table['stack'].map(decode_stack)
        results[k] = table
    return summary, results


def regional_observations(summary, results, minimum=30):
    """Наблюдения о зарплатах одного сочетания, пересечениях и покрытии зарплатами."""
    notes = []
    paid = summary.dropna(subset=['median_net']).sort_values('median_net')
    if len(paid) >= 2:
        low, high = paid.iloc[0], paid.iloc[-1]
        notes.append(f"Между {high['area']} и {low['area']} разница общей медианы — "
                     f"{high['median_net']-low['median_net']:,.0f} ₽ "
                     f"({high['median_net']:,.0f} против {low['median_net']:,.0f} ₽). "
                     f"Зарплата известна для {high['paid_pct']:.1f}% и {low['paid_pct']:.1f}% "
                     'вакансий соответственно; выборки имеют разное покрытие зарплатами.')
    for k, name in ((2, 'пар'), (3, 'троек')):
        table = results[k]
        top_sets = {}
        for area in summary['area']:
            top_sets[area] = set(table.loc[table['area'].eq(area)].sort_values(
                ['vacancies', 'stack'], ascending=[False, True]).head(10)['stack'])
        shared = set.intersection(*top_sets.values())
        if shared:
            key = min(shared, key=lambda s: (-table.loc[table['stack'].eq(s), 'vacancies'].sum(), s))
            facts = table.loc[table['stack'].eq(key)].set_index('area')
            notes.append(f"Для K={k} в топ-10 по частоте во всех трёх территориях входит "
                         f"«{decode_stack(key)}». Доли вакансий: " + '; '.join(
                             f"{area} — {facts.loc[area, 'share_pct']:.1f}%" for area in summary['area'])
                         + '. Это общий профиль спроса; абсолютные количества скрыли бы разницу размеров рынков.')
        else:
            notes.append(f'Для K={k} общего сочетания в трёх топ-10 нет: востребованность {name} '
                         'различается между выбранными территориями.')
        eligible = table.loc[table['salaries'].ge(minimum) & table['median_net'].notna()]
        comparable = eligible.groupby('stack')['area'].nunique()
        keys = comparable[comparable.eq(3)].index
        if len(keys):
            ranges = eligible.loc[eligible['stack'].isin(keys)].groupby('stack')['median_net'].agg(['min', 'max'])
            key = (ranges['max'] - ranges['min']).sort_values(ascending=False).index[0]
            same = eligible.loc[eligible['stack'].eq(key)].sort_values(['median_net', 'area'])
            low, high = same.iloc[0], same.iloc[-1]
            if high['median_net'] > low['median_net']:
                notes.append(f"Среди {name} с {minimum}+ зарплатами в каждой территории максимальный "
                             f"разброс для одного сочетания у «{decode_stack(key)}»: "
                             f"{high['area']} — {high['median_net']:,.0f} ₽ "
                             f"({int(high['salaries'])} зарплат), {low['area']} — "
                             f"{low['median_net']:,.0f} ₽ ({int(low['salaries'])} зарплат. "
                             'Одинаковый набор навыков сам по себе не выравнивает региональные медианы.')
            else:
                notes.append(f'Для K={k} медианы всех сочетаний с достаточной выборкой '
                             'во всех трёх территориях совпали между территориями; '
                             'регионального разрыва внутри этих сочетаний не обнаружено.')
        else:
            notes.append(f'Для K={k} нет сочетаний с {minimum}+ зарплатами одновременно '
                         'во всех трёх территориях; надёжное сравнение одной комбинации невозможно.')
    return notes