import pandas as pd
from django.conf import settings

from data_manager.utils import get_table
from visualiser.utils import convert_string_to_boolean


def limit_ops_general_groups(dataframe, list_of_columns, group_column, sort_by_column, ascending, reset_index_val):
    """This function is used to summarize groups provided in limit ops"""
    grouped_df = dataframe[list_of_columns].groupby(group_column).size().reset_index(name=reset_index_val). \
        sort_values(sort_by_column, ascending=ascending)
    return grouped_df


def load_and_rename(table_name, new_columns):
    """This function is used to load a new table and rename columns"""
    table_df = get_table(table=table_name).rename(columns=new_columns)
    return table_df


def limit_ops_formatter(first_df, second_df, how, on, keep_columns, orient='index', sorted_by=None, asc=None,
                        tail_num=None):
    """This function is used to format input for limit ops"""
    merged_df = pd.merge(first_df, second_df, how=how, on=on)[keep_columns]
    if sorted_by:
        merged_df = merged_df.sort_values(sorted_by, ascending=asc)
    if tail_num:
        merged_df = merged_df.tail(tail_num)
    result = list(merged_df.to_dict(orient=orient).values())
    return result


def limit_ops_skeleton(**kwargs):
    """This function provides a skeleton for limit ops calculations"""
    group_phase = kwargs['group_phase']
    tail = kwargs['tail']
    loading_phase = kwargs['loading_phase']
    final_phase = kwargs['final_phase']

    grouped_df = limit_ops_general_groups(
        **group_phase
    )
    grouped_df = grouped_df.tail(tail)
    loaded_table = load_and_rename(**loading_phase)

    final_phase['first_df'] = grouped_df
    final_phase['second_df'] = loaded_table

    final_values = limit_ops_formatter(**final_phase)
    return final_values


def popular_user_skills(asc, most_popular_skills):
    """This function is used to find the most popular skills in the QualiChain users (appearing in cvs)"""
    asc = convert_string_to_boolean(asc)
    cv_skills_df = get_table(table='cv_skills')

    final_values = limit_ops_skeleton(
        group_phase={
            'dataframe': cv_skills_df,
            'list_of_columns': ['skill_id', 'id'],
            'group_column': 'skill_id',
            'sort_by_column': 'count',
            'reset_index_val': 'count',
            'ascending': asc
        },
        tail=most_popular_skills,
        loading_phase={'table_name': 'skills', 'new_columns': {'id': 'skill_id', 'name': 'skill_name'}
                       },
        final_phase={
            'how': 'left',
            'on': 'skill_id',
            'keep_columns': ['skill_name', 'count']
        }
    )
    return final_values


def popular_user_courses(asc, number_of_courses):
    """This function is used to find the most popular courses in the Qualichain users"""
    asc = convert_string_to_boolean(asc)
    user_courses_df = get_table(table='user_courses')
    user_courses_df = user_courses_df.where(user_courses_df['course_status'] != 'taught')

    final_values = limit_ops_skeleton(
        group_phase={
            'dataframe': user_courses_df,
            'list_of_columns': ['course_id', 'id'],
            'group_column': 'course_id',
            'sort_by_column': 'count',
            'reset_index_val': 'count',
            'ascending': asc
        },
        tail=number_of_courses,
        loading_phase={'table_name': 'courses', 'new_columns': {'id': 'course_id', 'name': 'course_name'}
                       },
        final_phase={
            'how': 'left',
            'on': 'course_id',
            'keep_columns': ['course_name', 'count'],
            'sorted_by': 'count',
            'asc': asc,
            'tail_num': number_of_courses
        }
    )
    return final_values


def popular_skills(asc, most_popular_skills):
    """This function is used to find the most popular skills"""
    asc = convert_string_to_boolean(asc)
    job_skills_df = get_table(table='job_skills')

    final_values = limit_ops_skeleton(
        group_phase={
            'dataframe': job_skills_df,
            'list_of_columns': ['skill_id', 'id'],
            'group_column': 'skill_id',
            'sort_by_column': 'count',
            'ascending': asc,
            'reset_index_val': 'count'
        },
        tail=most_popular_skills,
        loading_phase={'table_name': 'skills', 'new_columns': {'id': 'skill_id', 'name': 'skill_name'}},
        final_phase={
            'how': 'left',
            'on': 'skill_id',
            'keep_columns': ['skill_name', 'count']
        }
    )
    return final_values


def popular_courses(asc, number_of_courses):
    """This function is used to find the most popular courses according to job market"""
    asc = convert_string_to_boolean(asc)
    job_skills_df = get_table(table='job_skills')
    grouped_job_skills_df = job_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(number_of_courses)
    skills_df = get_table(table='skills').rename(columns={'id': 'skill_id'})
    popular_skills_df = pd.merge(grouped_job_skills_df, skills_df, how='left', on='skill_id')[
        ['skill_id', 'count']]

    skills_courses_df = get_table(table='skills_courses')
    skills_courses_count = pd.merge(popular_skills_df, skills_courses_df, how='left', on='skill_id')[
        ['course_id', 'count']].groupby(['course_id'])['count'].agg('max')

    courses_df = get_table(table='courses').rename(columns={'id': 'course_id', 'name': 'course_name'})
    popular_courses = pd.merge(skills_courses_count, courses_df, how='left', on='course_id')[
        ['course_name', 'count']].sort_values('count', ascending=asc).tail(number_of_courses)
    final_values = list(popular_courses.to_dict('index').values())
    return final_values
