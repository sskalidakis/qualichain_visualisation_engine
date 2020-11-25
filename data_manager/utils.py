import datetime
import tempfile

import pandas as pd
import requests
import json

import sqlalchemy
from django.conf import settings

from data_manager.data_manager_settings import CURR_DESIGNER_PORT, QC_HOST, KBZ_HOST, CAREER_ADVISOR_PORT


def read_sql_tmpfile(query, db_engine):
    """This function is used to load a tmpfile as DataFrame"""
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        conn = db_engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        cur.close()
        df = pd.read_csv(tmpfile)
        return df


def get_table(**kwargs):
    """
    This function is used to load the provided table as a Pandas DataFrame

    :param kwargs: provided kwargs
    :return: pandas DataFrame
    """
    db_engine = sqlalchemy.create_engine(settings.ENGINE_STRING)
    if 'sql_command' in kwargs.keys():
        sql_command = kwargs['sql_command']
        table_df = read_sql_tmpfile(sql_command, db_engine)
        # table_df = pd.read_sql_query(sql_command, settings.ENGINE_STRING)
    elif 'table' in kwargs.keys():
        table = kwargs['table']
        sql_command = """SELECT * FROM {table}""".format(table=table)
        table_df = read_sql_tmpfile(sql_command, db_engine)
        # table_df = pd.read_sql_table(table, settings.ENGINE_STRING)
    else:
        table_df = pd.DataFrame()
    db_engine.dispose()
    return table_df


def date_to_unix(date):
    """This function is used to transform provided date to unix timestamp"""
    unix_timestamp = int(datetime.datetime.strptime(date, "%Y-%m-%d").timestamp()) * 1000
    return unix_timestamp


def trajectory_score_computing(max_parent, score):
    result = max_parent * score / 100
    return result


def convert_list_to_string_tuple(list):
    string = "("
    for el in list:
        string = string + "'" + str(el) + "',"
    string = string[:-1] + ")"
    return string


def recursive_search_trajectory(data, trajectory_data):
    for job in data:
        if len(job['nextpositions']) != 0:
            for el in job['nextpositions']:
                trajectory_data.append({"from": job["position"], "to": el["position"], "value": job['score']})
            trajectory_data = recursive_search_trajectory(job['nextpositions'], trajectory_data)
    return trajectory_data


def find_overlap_percentage(nominator, denominator):
    """This function is used to find overlap percentage between 2 lists"""
    common_skills_set = set(nominator).intersection(denominator)
    common_skills_list = list(common_skills_set)
    if common_skills_list:
        overlap_percentage = len(common_skills_list) / len(denominator) * 100
    else:
        overlap_percentage = 0
    return overlap_percentage


def format_bar_chart_input(dataframe, list_of_columns, group_by_columns, aggregation, new_columns=None,
                           fill_na_value=None, orient='index'):
    """This function is used to format a dataframe according the provided parameters"""
    if group_by_columns == 'specialization_id':
        df_chunk = dataframe[list_of_columns]
        specializations = get_table(table='specialization')
        merged_data = pd.merge(df_chunk, specializations, left_on='specialization_id', right_on='id')
        group = merged_data.groupby('title').agg(aggregation).reset_index().rename(
            columns={'specialization_id': 'count', 'title': 'specialization_id'})
    else:
        group = dataframe[list_of_columns].groupby(group_by_columns).agg(aggregation).reset_index()
        if new_columns:
            group = group.rename(columns=new_columns)
    if fill_na_value:
        group = group.fillna(fill_na_value)
    values = group.to_dict(orient=orient).values()
    values_to_list = list(values)
    return values_to_list


def curriculum_up_to_date():
    url = 'http://{}:{}/curriculum_skill_coverage'.format(QC_HOST, CURR_DESIGNER_PORT)
    headers = {
        'Content-Type': "application/json",
        'Postman-Token': "53181693-dfea-47df-8a4e-2d7124aeb47a",
        'Cache-Control': "no-cache"
    }
    response = requests.request("POST", url, data={}, headers=headers)
    return json.loads(response.text)['curriculum_skill_coverage'] * 100


def career_path_trajectory():
    url = 'http://{}:{}/career_path_trajectory'.format(KBZ_HOST, CAREER_ADVISOR_PORT)
    headers = {
        'Content-Type': "application/json",
        'Postman-Token': "53181693-dfea-47df-8a4e-2d7124aeb47a",
        'Cache-Control': "no-cache"
    }
    response = requests.request("GET", url, data={}, headers=headers)
    return response


def get_specialization_data(**kwargs):
    """This function is used to take parts of specialization data"""
    titles = kwargs['titles']
    if len(titles) == 1:
        fetch_specialization = """SELECT * FROM specialization WHERE title={}""".format(titles[0])
    else:
        fetch_specialization = """SELECT * FROM specialization WHERE title in {}""".format(titles)
    specialization_data = pd.read_sql_query(fetch_specialization, settings.ENGINE_STRING)

    specialization_title_values = specialization_data.set_index('title')
    specialization_id_values = specialization_data.set_index('id')

    specialization_title_values = list(specialization_title_values.to_dict().values())[0]
    specialization_id_values = list(specialization_id_values.to_dict().values())[0]
    return specialization_title_values, specialization_id_values
