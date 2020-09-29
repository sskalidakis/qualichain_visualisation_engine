from data_manager.groups_per_column import courses_avg_grades
from data_manager.projections import retrieve_user_skills, fetch_user_cv_skills, fetch_job_skills
from django.conf import settings
from data_manager.utils import get_table, find_overlap_percentage
import pandas as pd


def covered_skills_from_user(user_id, job_id):
    """This function is used find user's coverage percentage for a specific job"""
    users_skills_list = retrieve_user_skills(user_id)
    jobs_skills_sql_command = "SELECT skill_id FROM job_skills WHERE job_id={job_id}".format(
        **{'job_id': job_id}
    )
    job_skills_df = get_table(sql_command=jobs_skills_sql_command)
    job_skills_list = list(job_skills_df.skill_id)
    overlap_percentage = find_overlap_percentage(nominator=job_skills_list, denominator=job_skills_list)
    return overlap_percentage


def covered_cv_skills_from_course(user_id, course_id):
    """This function is used find the relation of a course to a user cv"""
    # cv_df = pd.read_sql_table('CVs', settings.ENGINE_STRING)
    # cv_id_float = cv_df.loc[cv_df['user_id'] == int(user_id)]['id']
    fetch_cv_command = """SELECT DISTINCT id FROM "CVs" WHERE user_id={user_id}""""".format(**{'user_id': user_id})
    cv_df = get_table(sql_command=fetch_cv_command)
    if len(cv_df) > 0:
        cv_id = cv_df['id'].tolist()[0]
        cv_skills_df = get_table(table='cv_skills')
        cv_skills = cv_skills_df.loc[cv_skills_df['cv_id'] == int(cv_id)][['skill_id']]['skill_id'].to_list()

        courses_skills_df = get_table(table='skills_courses')
        courses_skills = courses_skills_df.loc[courses_skills_df['course_id'] == int(course_id)][['skill_id']][
            'skill_id'].to_list()

        overlap_percentage = find_overlap_percentage(nominator=courses_skills, denominator=cv_skills)
        return overlap_percentage
    else:
        return 0


def covered_application_skills_from_course(user_id, course_id):
    """This function is used find the relation of a course to the user's interests (job applications)"""
    user_apps_df = pd.read_sql_table('user_applications', settings.ENGINE_STRING)
    job_ids = user_apps_df.loc[user_apps_df['user_id'] == int(user_id)].job_id.to_list()

    if len(job_ids) > 0:
        job_skills_df = pd.read_sql_table('job_skills', settings.ENGINE_STRING)
        job_skills = job_skills_df[job_skills_df['job_id'].isin(job_ids)][['skill_id']]['skill_id'].to_list()

        courses_skills_df = pd.read_sql_table('skills_courses', settings.ENGINE_STRING)
        courses_skills = courses_skills_df.loc[courses_skills_df['course_id'] == int(course_id)][['skill_id']][
            'skill_id'].to_list()

        overlap_percentage = find_overlap_percentage(nominator=courses_skills, denominator=job_skills)
        return overlap_percentage
    else:
        return 0


def skill_relation_with_user_applications(user_id, skill_id):
    """This function is used find the relation of a skill to the user's interests (job applications)"""
    user_apps_df = pd.read_sql_table('user_applications', settings.ENGINE_STRING)
    job_ids = user_apps_df.loc[user_apps_df['user_id'] == int(user_id)].job_id.to_list()
    if len(job_ids) > 0:
        job_skills_df = pd.read_sql_table('job_skills', settings.ENGINE_STRING)
        common_count = 0
        for job_id in job_ids:
            job_skills = job_skills_df[job_skills_df['job_id'] == job_id][['skill_id']]['skill_id'].to_list()
            if int(skill_id) in job_skills:
                common_count = common_count + 1

        overlap_percentage = common_count / len(job_ids) * 100
        return overlap_percentage
    else:
        return 0


def get_user_skills_for_job(user_id, job_id):
    user_skills_df = fetch_user_cv_skills(user_id)
    job_skills_df = fetch_job_skills(job_id)

    joined_df = pd.merge(user_skills_df, job_skills_df, on='skill_id', how='outer')
    joined_df = joined_df.fillna(0.0)

    skill_ids = joined_df['skill_id'].values.tolist()
    if len(skill_ids) > 1:
        fetch_skill_names = """SELECT name, id as skill_id FROM skills WHERE id in {skills_tuple}""".format(
            **{'skills_tuple': tuple(skill_ids)})
    elif len(skill_ids) == 1:
        fetch_skill_names = """SELECT name, id as skill_id FROM skills WHERE id={skill_id}""".format(
            **{'skill_id': skill_ids[0]})
    else:
        fetch_skill_names = None
    if fetch_skill_names:
        skill_details = get_table(sql_command=fetch_skill_names)
        enhanced_joined_skills = pd.merge(joined_df, skill_details, on='skill_id')
        results = list(enhanced_joined_skills[['name', 'skil_level']].to_dict(orient='index').values())
    else:
        results = []
    return results


def user_grades(user_id):
    """This function is used to fetch user grades"""
    user_grades_command = """SELECT course_id, grade FROM user_courses WHERE status_value='{status}' and user_id={user_id}""".format(
        **{'status': 'done', 'user_id': user_id})
    user_grades_df = get_table(sql_command=user_grades_command)
    course_ids = user_grades_df['course_id'].tolist()
    if len(course_ids) > 1:
        select_course_command = """SELECT id as course_id, name FROM courses WHERE id in {}""".format(tuple(course_ids))
    elif len(course_ids) == 1:
        select_course_command = """SELECT id as course_id, name FROM courses WHERE id={}""".format(course_ids[0])
    else:
        select_course_command = None
    if select_course_command:
        courses_names = get_table(sql_command=select_course_command)
        joined_courses_grades = pd.merge(courses_names, user_grades_df, on='course_id')[['name', 'grade']]
        results = list(joined_courses_grades.to_dict(orient='index').values())
    else:
        results = []
    return results


def get_avg_course_names(courses):
    """This function is used to summarize details for courses grades"""
    course_grades = courses_avg_grades(courses)
    if len(courses) > 1:
        course_details = """SELECT id as course_id, name FROM courses WHERE id in {}""".format(tuple(courses))
    elif len(courses) == 1:
        course_details = """SELECT id as course_id, name FROM courses WHERE id={}""".format(courses[0])
    else:
        course_details = None
    if course_details:
        courses_df = get_table(sql_command=course_details)
        joined_grades_names = pd.merge(course_grades, courses_df, on='course_id')[['name', 'grade']]
        results = list(joined_grades_names.to_dict(orient='index').values())
    else:
        results = []
    return results
