import datetime

import pandas as pd

from django.conf import settings


def get_table(**kwargs):
    """
    This function is used to load the provided table as a Pandas DataFrame

    :param kwargs: provided kwargs
    :return: pandas DataFrame
    """
    if 'sql_command' in kwargs.keys():
        sql_command = kwargs['sql_command']
        table_df = pd.read_sql_query(sql_command, settings.ENGINE_STRING)
    elif 'table' in kwargs.keys():
        table = kwargs['table']
        table_df = pd.read_sql_table(table, settings.ENGINE_STRING)
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
    group = dataframe[list_of_columns].groupby(group_by_columns).agg(aggregation).reset_index()
    if new_columns:
        group = group.rename(columns=new_columns)
    if fill_na_value:
        group = group.fillna(fill_na_value)
    values = group.to_dict(orient=orient).values
    values_to_list = list(values)
    return values_to_list
