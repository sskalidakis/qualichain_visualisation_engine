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
