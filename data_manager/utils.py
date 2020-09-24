import datetime

import pandas as pd

from data_manager.settings import ENGINE_STRING


def get_table(**kwargs):
    """
    This function is used to load the provided table as a Pandas DataFrame

    :param kwargs: provided kwargs
    :return: pandas DataFrame
    """
    if 'sql_command' in kwargs.keys():
        sql_command = kwargs['sql_command']
        table_df = pd.read_sql_query(sql_command, ENGINE_STRING)
    elif 'table' in kwargs.keys():
        table = kwargs['table']
        table_df = pd.read_sql_table(table, ENGINE_STRING)
    else:
        table_df = pd.DataFrame()
    return table_df


def date_to_unix(date):
    """This function is used to transform provided date to unix timestamp"""
    unix_timestamp = int(datetime.datetime.strptime(date, "%Y-%m-%d").timestamp()) * 1000
    return unix_timestamp


def trajectory_score_computing(max_parent, score):
    result = max_parent * score / 100
    return result


def recursive_search_trajectory(data, trajectory_data):
    for job in data:
        if len(job['nextpositions']) != 0:
            for el in job['nextpositions']:
                trajectory_data.append({"from": job["position"], "to": el["position"], "value": job['score']})
            trajectory_data = recursive_search_trajectory(job['nextpositions'], trajectory_data)
        else:
            pass
    return trajectory_data