import pandas as pd
from django.conf import settings

from data_manager.utils import get_table
from visualiser.utils import convert_string_to_boolean


def popular_user_skills(asc, most_popular_skills):
    """This function is used to find the most popular skills in the Qualichain users (appearing in cvs)"""
    asc = convert_string_to_boolean(asc)
    cv_skills_df = get_table(table='cv_skills')
    grouped_cv_skills_df = cv_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(most_popular_skills)
    skills_df = get_table(table='skills').rename(columns={'id': 'skill_id', 'name': 'skill_name'})
    popular_skills_df = pd.merge(grouped_cv_skills_df, skills_df, how='left', on='skill_id')[['skill_name', 'count']]
    final_values = list(popular_skills_df.to_dict('index').values())
    return final_values


def popular_user_courses(asc, number_of_courses):
    """This function is used to find the most popular courses in the Qualichain users"""
    asc = convert_string_to_boolean(asc)
    user_courses_df = get_table(table='user_courses')
    user_courses_df = user_courses_df.where(user_courses_df['status_value'] != 'taught')
    grouped_user_courses_df = user_courses_df[['course_id', 'id']].groupby('course_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(number_of_courses)
    courses_df = get_table(table='courses').rename(columns={'id': 'course_id', 'name': 'course_name'})
    popular_courses = pd.merge(grouped_user_courses_df, courses_df, how='left', on='course_id')[
        ['course_name', 'count']].sort_values('count', ascending=asc).tail(number_of_courses)
    final_values = list(popular_courses.to_dict('index').values())
    return final_values


def popular_skills(asc, most_popular_skills):
    """This function is used to find the most popular skills"""
    asc = convert_string_to_boolean(asc)
    job_skills_df = get_table(table='job_skills')
    grouped_job_skills_df = job_skills_df[['skill_id', 'id']].groupby('skill_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(most_popular_skills)
    skills_df = get_table(table='skills').rename(columns={'id': 'skill_id', 'name': 'skill_name'})
    popular_skills_df = pd.merge(grouped_job_skills_df, skills_df, how='left', on='skill_id')[['skill_name', 'count']]
    final_values = list(popular_skills_df.to_dict('index').values())
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
