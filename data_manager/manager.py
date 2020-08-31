from django.shortcuts import render
from django.apps import apps
import logging
import sys
import json

import pandas as pd

from data_manager.settings import ENGINE_STRING
from visualiser.visualiser_settings import DATA_TABLES_APP

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def create_heatmap_data(dataset, row_categorisation_dataset, col_order, row_order, dataset_type):
    final_data = []
    ranges_data = []
    if dataset_type == 'file':
        final_data = heatmap_chart_data_from_file(dataset)

    elif dataset_type == 'db':
        try:
            dataset = Dataset.objects.get(dataset_name=dataset)
            data_table = apps.get_model(DATA_TABLES_APP, dataset.dataset_django_model)
            data = data_table.objects.all()
            variables = Variable.objects.filter(dataset_relation=dataset.id).order_by('id')
        except Exception as e:
            log.error('Dataset or corresponding variables not found in order to complete the 2d histogram.')
            log.error(e)
            return e, 400

        # The order of the variables is decided to be like this: column, row, value.
        try:
            col_ordering = heatmap_ordering(col_order, variables, 0)
            row_ordering = heatmap_ordering(row_order, variables, 1)
            data = heatmap_ordering_method(col_ordering, data, row_ordering)
        except Exception as e:
            log.error('Error while ordering the columns or rows for the histogram 2d')
            log.error(e)
            return e, 400

        final_data = reformat_heatmap_data(data, variables)
        # If guides/ranges are used the dataset of the guides has to be declared explicitly in the request
        # TODO: We may need to change that (not sure how though)
        # TODO: Also we may need to include column categorisation
        ranges_data = heatmap_row_categorisation(row_categorisation_dataset)

    return final_data, ranges_data


def heatmap_row_categorisation(row_categorisation_dataset):
    ranges_data = []
    if row_categorisation_dataset != '':
        row_ranges_table = apps.get_model(DATA_TABLES_APP, row_categorisation_dataset).objects.all()
        for el in row_ranges_table:
            dict_el = {'guide_from': el.guide_from, 'guide_to': el.guide_to, 'value': el.value}
            ranges_data.append(dict_el)
    return ranges_data


def heatmap_chart_data_from_file(dataset):
    final_data = []
    with open('static/' + dataset, 'r') as f:
        data = f.read()
    diction = json.loads(data)
    for model, vars in diction.items():
        for var, val in vars.items():
            final_data.append({"model": model, "variable": var, "value": val})
    print(final_data)
    return final_data


def reformat_heatmap_data(data, variables):
    final_data = []
    for el in data:
        dictionary = {}
        for var in variables:
            if var.variable_table_name is None:
                dictionary[var.var_name] = getattr(el, var.var_name)
            else:
                var_table = apps.get_model(DATA_TABLES_APP, var.variable_table_name)
                var_table_obj = var_table.objects.get(id=getattr(el, var.var_name).id)
                value = var_table_obj.title
                dictionary[var.var_name] = value
        final_data.append(dictionary)
    return final_data


def heatmap_ordering_method(col_ordering, data, row_ordering):
    if (col_ordering is None) and (row_ordering is None):
        pass
    elif col_ordering is None:
        data = data.order_by(row_ordering)
    elif row_ordering is None:
        data = data.order_by(col_ordering)
    else:
        data = data.order_by(col_ordering, row_ordering)
    return data


def heatmap_ordering(order, variables, var_position):
    ordering = None
    django_model = apps.get_model(DATA_TABLES_APP, variables[var_position].variable_table_name)
    fields = django_model._meta.get_fields()
    for field in fields:
        if order == field.name:
            ordering = str(variables[var_position].var_name) + "__" + str(field.name)
    return ordering


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


def group_users_per_column(column, aggregation="count"):
    """This function is used to group users table according to provided column"""

    users_df = get_table(table='users')
    group = users_df[[column, 'id']].groupby(column).agg(aggregation).reset_index()
    final_values = list(group.to_dict('index').values())
    return final_values


def user_jobs_groups(column, aggregation="count"):
    """This function is used to aggregate jobs data for a specific user"""
    sql_command = """SELECT job_id from user_applications where user_id={}""".format(1)
    user_applied_jobs = get_table(sql_command=sql_command)
    job_ids = user_applied_jobs["job_id"].to_list()
    if len(job_ids) == 1:
        fetch_jobs = """SELECT * from jobs where id in ({})""".format(job_ids[0])
    else:
        job_tuples = tuple(job_ids)
        fetch_jobs = """SELECT * from jobs where id in {}""".format(job_tuples)
    related_jobs = get_table(sql_command=fetch_jobs)
    group = related_jobs[[column, "id"]].groupby(column).agg(aggregation).reset_index()
    values = list(group.to_dict('index').values())
    return values


def build_bar_chart(base_query, x_axis_name, **kwargs):
    """This abstract function is used to call submethods/specific model"""
    if base_query == 'group_users':
        bar_chart_input = group_users_per_column(x_axis_name)
    elif base_query == 'group_job_user':
        bar_chart_input = user_jobs_groups(x_axis_name)
    return bar_chart_input

# def courses_most_popular_skills(column, aggregation="count", number_of_courses=15):
#     """This function is used to find courses with the most popular skills"""
#
#     job_skills_df = pd.read_sql_table('job_skills', ENGINE_STRING)
#     group = job_skills_df[[column, 'id']].groupby('skill_id').agg(aggregation).reset_index()
#     import pdb
#     pdb.set_trace()
#     final_values = list(group.to_dict('index').values())
#     return final_values
