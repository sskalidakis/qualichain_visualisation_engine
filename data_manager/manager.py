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


def group_users_per_column(column, aggregation="count"):
    """This function is used to group users table according to provided column"""
    users_df = pd.read_sql_table('users', ENGINE_STRING)
    group = users_df[[column, 'id']].groupby(column).agg(aggregation).reset_index()
    final_values = list(group.to_dict('index').values())
    return final_values


def build_bar_chart(base_query, x_axis_name, **kwargs):
    """This abstract function is used to call submethods/specific model"""
    bar_chart_input = []
    if base_query == 'group_users':
        bar_chart_input = group_users_per_column(x_axis_name)
    elif base_query == 'popular_skills':
        bar_chart_input = popular_skills()
    elif base_query == 'popular_courses':
        bar_chart_input = popular_courses()
    return bar_chart_input





def popular_skills(most_popular_skills=10):
    """This function is used to find the most popular skills"""

    job_skills_df = pd.read_sql_table('job_skills', ENGINE_STRING)
    grouped_job_skills_df = job_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
        name='count').sort_values('count')
    skills_df = pd.read_sql_table('skills', ENGINE_STRING).rename(columns={'id': 'skill_id', 'name': 'skill_name'})
    popular_skills_df = pd.merge(grouped_job_skills_df, skills_df, how='left', on='skill_id')[['skill_name', 'count']]
    final_values = list(popular_skills_df.to_dict('index').values())
    print(final_values)
    return final_values


def popular_courses(number_of_courses=10):
    """This function is used to find the most popular courses according to job market"""

    job_skills_df = pd.read_sql_table('job_skills', ENGINE_STRING)
    grouped_job_skills_df = job_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
        name='count').sort_values('count').tail()
    skills_df = pd.read_sql_table('skills', ENGINE_STRING).rename(columns={'id': 'skill_id'})
    popular_skills_df = pd.merge(grouped_job_skills_df, skills_df, how='left', on='skill_id')[
        ['skill_id', 'count']]
    skills_courses_df = pd.read_sql_table('skills_courses', ENGINE_STRING)
    skills_courses_count = pd.merge(popular_skills_df, skills_courses_df, how='left', on='skill_id')[
        ['course_id', 'count']].groupby(['course_id'])['count'].agg('max')
    courses_df = pd.read_sql_table('courses', ENGINE_STRING).rename(columns={'id': 'course_id', 'name': 'course_name'})
    popular_courses = pd.merge(skills_courses_count, courses_df, how='left', on='course_id')[
        ['course_name', 'count']].sort_values('course_name').tail(number_of_courses)
    final_values = list(popular_courses.to_dict('index').values())
    print(final_values)
    return final_values


def api_most_popular_courses(number_of_courses=100, most_popular_skills=10):
    """This api is used to find courses with the most popular skills"""

    job_skills_df = pd.read_sql_table('job_skills', ENGINE_STRING)
    grouped_job_skills_df = job_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
        name='count').sort_values('count').tail(most_popular_skills)
    skills_df = pd.read_sql_table('skills', ENGINE_STRING).rename(columns={'id': 'skill_id', 'name': 'skill_name'})
    popular_skills_df = pd.merge(grouped_job_skills_df, skills_df, how='left', on='skill_id')[
        ['skill_id', 'skill_name', 'count']]
    skills_courses_df = pd.read_sql_table('skills_courses', ENGINE_STRING)
    skills_courses_count = pd.merge(popular_skills_df, skills_courses_df, how='left', on='skill_id')[
        ['course_id', 'skill_name', 'count']].sort_values('count').tail(number_of_courses)
    courses_df = pd.read_sql_table('courses', ENGINE_STRING).rename(columns={'id': 'course_id', 'name': 'course_name'})
    popular_courses = pd.merge(skills_courses_count, courses_df, how='left', on='course_id')[
        ['course_name', 'skill_name', 'count']].sort_values('course_name')
    final_values = []
    course_name = ''
    new_dict = {}
    for index, row in popular_courses.iterrows():
        if course_name == '' or course_name != row['course_name']:
            if course_name != '':
                final_values.append(new_dict)
            course_name = row['course_name']
            new_dict['course'] = course_name
            new_dict[row['skill_name']] = row['count']
        else:
            new_dict[row['skill_name']] = row['count']

    return final_values
