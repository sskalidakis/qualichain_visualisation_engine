from data_manager.projections import retrieve_user_skills
from data_manager.settings import ENGINE_STRING
from data_manager.utils import get_table
import pandas as pd
import math


def covered_skills_from_user(user_id, job_id):
    """This function is used find user's coverage percentage for a specific job"""
    users_skills_list = retrieve_user_skills(user_id)
    jobs_skills_sql_command = "SELECT skill_id FROM job_skills WHERE job_id={job_id}".format(
        **{'job_id': job_id}
    )
    job_skills_df = get_table(sql_command=jobs_skills_sql_command)
    job_skills_list = list(job_skills_df.skill_id)
    common_skills = list(set(users_skills_list).intersection(job_skills_list))
    if common_skills:
        overlap_percentage = len(common_skills) / len(job_skills_list) * 100
    else:
        overlap_percentage = 0
    return overlap_percentage


def covered_cv_skills_from_course(user_id, course_id):
    """This function is used find the relation of a course to a user cv"""
    cv_df = pd.read_sql_table('CVs', ENGINE_STRING)
    cv_id_float = cv_df.loc[cv_df['user_id'] == int(user_id)]['id']
    if len(cv_id_float) > 0:
        cv_id = int(cv_id_float[0])
        cv_skills_df = pd.read_sql_table('cv_skills', ENGINE_STRING)
        cv_skills = cv_skills_df.loc[cv_skills_df['cv_id'] == int(cv_id)][['skill_id']]['skill_id'].to_list()

        courses_skills_df = pd.read_sql_table('skills_courses', ENGINE_STRING)
        courses_skills = courses_skills_df.loc[courses_skills_df['course_id'] == int(course_id)][['skill_id']][
            'skill_id'].to_list()

        common_skills = set(courses_skills).intersection(cv_skills)
        print(cv_skills)
        print(courses_skills)
        print(common_skills)
        if common_skills:
            overlap_percentage = len(common_skills) / len(courses_skills) * 100
        else:
            overlap_percentage = 0
        return overlap_percentage


def covered_application_skills_from_course(user_id, course_id):
    """This function is used find the relation of a course to the user's interests (job applications)"""
    user_apps_df = pd.read_sql_table('user_applications', ENGINE_STRING)
    job_ids = user_apps_df.loc[user_apps_df['user_id'] == int(user_id)].job_id.to_list()

    if len(job_ids) > 0:
        job_skills_df = pd.read_sql_table('job_skills', ENGINE_STRING)
        job_skills = job_skills_df[job_skills_df['job_id'].isin(job_ids)][['skill_id']]['skill_id'].to_list()

        courses_skills_df = pd.read_sql_table('skills_courses', ENGINE_STRING)
        courses_skills = courses_skills_df.loc[courses_skills_df['course_id'] == int(course_id)][['skill_id']][
            'skill_id'].to_list()

        common_skills = set(courses_skills).intersection(job_skills)
        print(job_skills)
        print(courses_skills)
        print(common_skills)
        if common_skills:
            overlap_percentage = len(common_skills) / len(courses_skills) * 100
        else:
            overlap_percentage = 0
        return overlap_percentage
