import pandas as pd
from data_manager.projections import get_user_applied_jobs
from django.conf import settings
from data_manager.utils import get_table, format_bar_chart_input, convert_list_to_string_tuple
from visualiser.utils import convert_string_to_boolean
from operator import is_not
from functools import partial


def group_users_per_column(column, aggregation="count"):
    """This function is used to group users table according to provided column"""

    users_df = get_table(table='users')
    final_values = format_bar_chart_input(
        dataframe=users_df,
        list_of_columns=[column, 'id'],
        group_by_columns=column,
        aggregation=aggregation
    )
    return final_values


def get_job_application_stats(sql_command, column, aggregation="count"):
    """This function is used to retrieve job application insights"""
    if sql_command:
        related_jobs = get_table(sql_command=sql_command)
        values = format_bar_chart_input(
            dataframe=related_jobs,
            list_of_columns=[column, 'id'],
            group_by_columns=column,
            aggregation=aggregation
        )
        return values
    else:
        return None


def group_jobs_per_column(column):
    """This function is used to group jobs table according to provided column"""
    jobs_df = get_table(table='jobs')
    final_values = format_bar_chart_input(
        dataframe=jobs_df,
        list_of_columns=[column, 'id'],
        group_by_columns=column,
        aggregation='count',
        new_columns={'id': 'count'}
    )
    return final_values


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
        group = info[["level", "exp_salary", column]].groupby(['level', column]).agg(aggregation)
        gr = group.reset_index(column).pivot(columns=column, values='exp_salary').reset_index().fillna(0)
        values = gr.to_dict('index').values()
    else:
        group = info[["level", "exp_salary"]].groupby(['level']).agg(aggregation)
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


def skill_demand_per_column(asc, skill_names, limit, column):
    """This function is used to find the demand of a cv's skillset in different specialisations"""
    asc = convert_string_to_boolean(asc)
    job_skills_df = get_table(table='job_skills')
    jobs_query = """SELECT id as job_id, {} FROM jobs""".format(column)
    jobs_df = get_table(sql_command=jobs_query)
    skill_tuple = convert_list_to_string_tuple(skill_names)
    skill_query = """SELECT id as skill_id, name as skill_title FROM skills WHERE name IN {}""".format(skill_tuple)

    skills_df = get_table(sql_command=skill_query)
    joined_jobs_skills = pd.merge(job_skills_df, skills_df, how='inner', on='skill_id')
    joined_job_skills_column = pd.merge(joined_jobs_skills, jobs_df, how='left', on='job_id')[
        ['job_id', 'skill_title', column]]
    final_df = joined_job_skills_column.groupby(['skill_title', column]).agg({'job_id': ['count']})
    final_df.columns = ['count']
    ordered_final_df = final_df.reset_index().sort_values('count', ascending=asc).tail(limit)
    temp = list(
        ordered_final_df.pivot(index='specialization', columns='skill_title', values='count').reset_index().fillna(
            0).to_dict('index').values())
    return temp



def courses_avg_grades(courses):
    """This function is used to find average grades for provided courses"""
    if len(courses) > 1:
        course_grades_command = """SELECT grade, course_id FROM user_courses WHERE course_id in {courses_tuple}""". \
            format(**{'courses_tuple': tuple(courses)})
    else:
        course_grades_command = """SELECT grade, course_id FROM user_courses WHERE course_id={}""".format(courses[0])
    grades_df = get_table(sql_command=course_grades_command)
    course_grades = grades_df.groupby('course_id').agg('mean').reset_index()
    return course_grades
