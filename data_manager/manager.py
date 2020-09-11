from django.shortcuts import render
from django.apps import apps
import logging
import sys
import json

import pandas as pd

from data_manager.settings import ENGINE_STRING
from visualiser.utils import convert_string_to_boolean
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


def get_user_applied_jobs(user_id):
    """This function is used to retrieve user applied jobs"""
    if user_id:
        sql_get_applications = """SELECT job_id from user_applications where user_id={}""".format(user_id)
        user_applied_jobs = get_table(sql_command=sql_get_applications)
        job_ids = user_applied_jobs["job_id"].to_list()
        return job_ids
    else:
        return None


def get_job_application_stats(sql_command, column, aggregation="count"):
    """This function is used to retrieve job application insights"""
    if sql_command:
        related_jobs = get_table(sql_command=sql_command)
        group = related_jobs[[column, "id"]].groupby(column).agg(aggregation).reset_index()
        values = list(group.to_dict('index').values())
        return values
    else:
        return None


def user_jobs_groups(column, user_id):
    """This function is used to aggregate jobs data for a specific user"""
    job_ids = get_user_applied_jobs(user_id)

    if job_ids:
        job_ids_cnt = len(job_ids)
        if job_ids_cnt == 1:
            fetch_jobs = """SELECT * from jobs where id in ({})""".format(job_ids[0])
        elif job_ids_cnt > 1:
            job_tuples = tuple(job_ids)
            fetch_jobs = """SELECT * from jobs where id in {}""".format(job_tuples)
        else:
            fetch_jobs = None
    else:
        if user_id is not None:
            fetch_jobs = None
        else:
            fetch_jobs = """SELECT * from jobs"""

    data = get_job_application_stats(sql_command=fetch_jobs, column=column)
    return data


def calculate_salary_insights(sql_command, aggregation, column="country"):
    """This function is used to calculate salary insights"""
    info = get_table(sql_command=sql_command)
    if column:
        group = info[["level_value", "exp_salary", column]].groupby(['level_value', column]).agg(aggregation)
        gr = group.reset_index(column).pivot(columns=column, values='exp_salary').reset_index().fillna(0)
        values = gr.to_dict('index').values()
    else:
        group = info[["level_value", "exp_salary"]].groupby(['level_value']).agg(aggregation)
        values = group.reset_index().to_dict('index').values()
    return values


def salary_information(aggregation, y_column=None, data=None):
    """This function is used to fetch insights about Qualichain job applications"""
    if data:
        column = y_column
        if len(data) == 1:
            salary_command = """SELECT * from jobs JOIN user_applications ON jobs.id=user_applications.job_id where jobs.{}='{}'""".format(
                y_column, data[0])
        else:
            salary_command = """SELECT * from jobs JOIN user_applications ON jobs.id=user_applications.job_id where jobs.{} in {}""".format(
                y_column, tuple(data))
    else:
        column = None
        salary_command = """SELECT * FROM jobs JOIN user_applications ON jobs.id=user_applications.job_id"""
    values = list(calculate_salary_insights(sql_command=salary_command, column=column, aggregation=aggregation))
    return values


def build_bar_chart(x_axis_name, request, **kwargs):
    """This abstract function is used to call submethods/specific model"""
    base_query = request.GET.get("base_query", None)

    bar_chart_input = []
    if base_query == 'group_users':
        bar_chart_input = group_users_per_column(x_axis_name)
    elif base_query == 'group_job_user':
        user_id = request.GET.get("user_id", None)
        bar_chart_input = user_jobs_groups(x_axis_name, user_id)
    elif base_query == 'popular_skills_market':
        limit_skills = int(request.GET.get("limit_skills", 10))
        asc = request.GET.get("asc", "False")
        bar_chart_input = popular_skills(asc, limit_skills)
    elif base_query == 'popular_courses_market':
        limit_skills = int(request.GET.get("limit_courses", 10))
        asc = request.GET.get("asc", "False")
        bar_chart_input = popular_courses(asc, limit_skills)
    elif base_query == 'popular_skills_users':
        limit_skills = int(request.GET.get("limit_skills", 10))
        asc = request.GET.get("asc", "False")
        bar_chart_input = popular_user_skills(asc, limit_skills)
    elif base_query == 'popular_courses_users':
        limit_skills = int(request.GET.get("limit_courses", 10))
        asc = request.GET.get("asc", "False")
        bar_chart_input = popular_user_courses(asc, limit_skills)
    elif base_query == 'group_course_professor':
        limit_professors = int(request.GET.get("limit_professors", 10))
        asc_ordering = request.GET.get("asc", "False")
        bar_chart_input = group_courses_users(limit_professors, asc_ordering)
    elif base_query == 'salary_info':
        y_column = request.GET.get('y_column', None)
        y_var_names = request.GET.getlist("y_var_names[]", [])
        agg = request.GET.get('agg', 'mean')

        if y_column and y_var_names:
            bar_chart_input = salary_information(data=y_var_names, y_column=y_column, aggregation=agg)
            print(bar_chart_input)
        else:
            bar_chart_input = salary_information(aggregation=agg)
    elif base_query == 'skill_demand_per_column':
        limit_results = int(request.GET.get("limit_results", 10))
        asc_ordering = request.GET.get("asc", "False")
        y_var_names = request.GET.getlist("y_var_names[]", [])
        column = request.GET.get("x_axis_name", "specialization")
        bar_chart_input = skill_demand_per_column(asc_ordering, y_var_names, limit_results, column)
    return bar_chart_input


def build_pie_chart(category_name, request, **kwargs):
    """This abstract function is used to call submethods/specific model"""
    base_query = request.GET.get("base_query", None)

    bar_chart_input = []
    if base_query == 'group_users':
        bar_chart_input = group_users_per_column(category_name)
    elif base_query == 'group_job_user':
        user_id = request.GET.get("user_id", None)
        bar_chart_input = user_jobs_groups(category_name, user_id)
    elif base_query == 'group_course_professor':
        bar_chart_input = group_courses_users(999, 'True')
    return bar_chart_input


def skill_demand_per_column(asc, skill_names, limit, column):
    """This function is used to find the demand of a cv's skillset in different specialisations"""

    asc = convert_string_to_boolean(asc)
    job_skills_df = pd.read_sql_table('job_skills', ENGINE_STRING)
    jobs_df = pd.read_sql_table('jobs', ENGINE_STRING).rename(columns={'id': 'job_id'})
    final_values = {}
    column_values = []
    results = []
    skills_df = pd.read_sql_table('skills', ENGINE_STRING)
    skills_df = pd.read_sql_table('skills', ENGINE_STRING)[skills_df['name'].isin(skill_names)].rename(columns={'id': 'skill_id', 'name': 'skill_title'})[
        ['skill_id', 'skill_title']]
    skill_ids = skills_df[['skill_id', 'skill_title']].set_index(
        'skill_id').to_dict(orient='index')

    if len(skill_ids) != 0:
        print(skill_ids)
        for skill_key, skill_obj in skill_ids.items():
            sel_job_skills_df = job_skills_df[(job_skills_df['skill_id'] == int(skill_key))]
            skill_demand_df = pd.merge(sel_job_skills_df, jobs_df, how='left', on='job_id')[['job_id', column]]
            demand_per_column = skill_demand_df.groupby([column])['job_id'].size().reset_index(
                name='count').sort_values('count', ascending=asc).tail(limit)
            for el in demand_per_column[column].values.tolist():
                if el not in column_values:
                    column_values.append(el)
            part_final_values = list(demand_per_column.to_dict('index').values())
            final_values[skill_key] = part_final_values
        for col_val in column_values:
            dict = {}
            dict[column] = col_val
            for key, value in final_values.items():
                for el in value:
                    if el[column] == col_val:
                        dict[skill_ids[key]['skill_title']] = el['count']
                        break
            results.append(dict)

        return results
    else:
        return []


def group_courses_users(limit, asc):
    """This function is used to find the number of courses per professor"""
    asc = convert_string_to_boolean(asc)
    user_courses_df = pd.read_sql_table('user_courses', ENGINE_STRING)
    professor_courses_df = user_courses_df.where(user_courses_df['status_value'] == 'taught')
    grouped_professor_courses_df = professor_courses_df[['user_id', 'id']].groupby('user_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(limit)
    users_df = pd.read_sql_table('users', ENGINE_STRING).rename(columns={'id': 'user_id', 'fullName': 'user_name'})
    professors_courses = pd.merge(grouped_professor_courses_df, users_df, how='left', on='user_id')[
        ['user_name', 'count']].sort_values('count', ascending=asc)
    final_values = list(professors_courses.to_dict('index').values())
    print(final_values)
    return final_values


def popular_user_skills(asc, most_popular_skills):
    """This function is used to find the most popular skills in the Qualichain users (appearing in cvs)"""
    asc = convert_string_to_boolean(asc)
    cv_skills_df = pd.read_sql_table('cv_skills', ENGINE_STRING)
    grouped_cv_skills_df = cv_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(most_popular_skills)
    skills_df = pd.read_sql_table('skills', ENGINE_STRING).rename(columns={'id': 'skill_id', 'name': 'skill_name'})
    popular_skills_df = pd.merge(grouped_cv_skills_df, skills_df, how='left', on='skill_id')[['skill_name', 'count']]
    final_values = list(popular_skills_df.to_dict('index').values())
    print(final_values)
    return final_values


def popular_user_courses(asc, number_of_courses):
    """This function is used to find the most popular courses in the Qualichain users"""
    asc = convert_string_to_boolean(asc)
    user_courses_df = pd.read_sql_table('user_courses', ENGINE_STRING)
    user_courses_df = user_courses_df.where(user_courses_df['status_value'] != 'taught')
    grouped_user_courses_df = user_courses_df[['course_id', 'id']].groupby('course_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(number_of_courses)
    courses_df = pd.read_sql_table('courses', ENGINE_STRING).rename(columns={'id': 'course_id', 'name': 'course_name'})
    popular_courses = pd.merge(grouped_user_courses_df, courses_df, how='left', on='course_id')[
        ['course_name', 'count']].sort_values('count', ascending=asc).tail(number_of_courses)
    final_values = list(popular_courses.to_dict('index').values())
    print(final_values)
    return final_values


def popular_skills(asc, most_popular_skills):
    """This function is used to find the most popular skills"""
    asc = convert_string_to_boolean(asc)
    job_skills_df = pd.read_sql_table('job_skills', ENGINE_STRING)
    grouped_job_skills_df = job_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(most_popular_skills)
    skills_df = pd.read_sql_table('skills', ENGINE_STRING).rename(columns={'id': 'skill_id', 'name': 'skill_name'})
    popular_skills_df = pd.merge(grouped_job_skills_df, skills_df, how='left', on='skill_id')[['skill_name', 'count']]
    final_values = list(popular_skills_df.to_dict('index').values())
    print(final_values)
    return final_values


def popular_courses(asc, number_of_courses):
    """This function is used to find the most popular courses according to job market"""
    asc = convert_string_to_boolean(asc)
    job_skills_df = pd.read_sql_table('job_skills', ENGINE_STRING)
    grouped_job_skills_df = job_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(number_of_courses)
    skills_df = pd.read_sql_table('skills', ENGINE_STRING).rename(columns={'id': 'skill_id'})
    popular_skills_df = pd.merge(grouped_job_skills_df, skills_df, how='left', on='skill_id')[
        ['skill_id', 'count']]

    skills_courses_df = pd.read_sql_table('skills_courses', ENGINE_STRING)
    skills_courses_count = pd.merge(popular_skills_df, skills_courses_df, how='left', on='skill_id')[
        ['course_id', 'count']].groupby(['course_id'])['count'].agg('max')

    courses_df = pd.read_sql_table('courses', ENGINE_STRING).rename(columns={'id': 'course_id', 'name': 'course_name'})
    popular_courses = pd.merge(skills_courses_count, courses_df, how='left', on='course_id')[
        ['course_name', 'count']].sort_values('count', ascending=asc).tail(number_of_courses)
    final_values = list(popular_courses.to_dict('index').values())
    print(final_values)
    return final_values

# TODO: Move this API to the correct module if it is useful

# def api_most_popular_courses(number_of_courses=100, most_popular_skills=10):
#     """This api is used to find courses with the most popular skills"""
#
#     job_skills_df = pd.read_sql_table('job_skills', ENGINE_STRING)
#     grouped_job_skills_df = job_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
#         name='count').sort_values('count').tail(most_popular_skills)
#     skills_df = pd.read_sql_table('skills', ENGINE_STRING).rename(columns={'id': 'skill_id', 'name': 'skill_name'})
#     popular_skills_df = pd.merge(grouped_job_skills_df, skills_df, how='left', on='skill_id')[
#         ['skill_id', 'skill_name', 'count']]
#     skills_courses_df = pd.read_sql_table('skills_courses', ENGINE_STRING)
#     skills_courses_count = pd.merge(popular_skills_df, skills_courses_df, how='left', on='skill_id')[
#         ['course_id', 'skill_name', 'count']].sort_values('count').tail(number_of_courses)
#     courses_df = pd.read_sql_table('courses', ENGINE_STRING).rename(columns={'id': 'course_id', 'name': 'course_name'})
#     popular_courses = pd.merge(skills_courses_count, courses_df, how='left', on='course_id')[
#         ['course_name', 'skill_name', 'count']].sort_values('course_name')
#     final_values = []
#     course_name = ''
#     new_dict = {}
#     for index, row in popular_courses.iterrows():
#         if course_name == '' or course_name != row['course_name']:
#             if course_name != '':
#                 final_values.append(new_dict)
#             course_name = row['course_name']
#             new_dict['course'] = course_name
#             new_dict[row['skill_name']] = row['count']
#         else:
#             new_dict[row['skill_name']] = row['count']
#
#     return final_values
