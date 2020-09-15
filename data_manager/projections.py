import pandas as pd

from data_manager.settings import ENGINE_STRING
from data_manager.utils import get_table, date_to_unix
from visualiser.utils import convert_string_to_boolean


def get_user_applied_jobs(user_id):
    """This function is used to retrieve user applied jobs"""
    if user_id:
        sql_get_applications = """SELECT job_id from user_applications where user_id={}""".format(user_id)
        user_applied_jobs = get_table(sql_command=sql_get_applications)
        job_ids = user_applied_jobs["job_id"].to_list()
        return job_ids
    else:
        return None


def retrieve_user_skills(user_id):
    """This function is used to retrieve user skills"""
    skills_sql_command = """
        SELECT user_id, skill_id FROM
        (SELECT * FROM "CVs" WHERE user_id={user_id}) user_cv
        JOIN cv_skills ON user_cv.id=cv_skills.cv_id
        """.format(**{'user_id': user_id})
    skills_data = get_table(sql_command=skills_sql_command)
    user_skills_list = list(skills_data.skill_id)
    return user_skills_list


def skill_demand_in_time(skill_id, specialization):
    """This function is used to find skill demand in a specific specialization during time"""
    sql_command = """
    SELECT job_skill.skill_id, jobs.specialization, jobs.date
    FROM 
    (SELECT * from job_skills WHERE skill_id={skill_id}) as job_skill
    JOIN jobs
    ON job_skill.job_id=jobs.id
    WHERE specialization='{specialization}'
    """.format(**{'skill_id': skill_id, 'specialization': specialization})
    skills_jobs_data = get_table(sql_command=sql_command)
    grouped_dates = skills_jobs_data.groupby('date').count().reset_index().rename(
        columns={'date': 'time_0', 'skill_id': 'skill_demand'})
    grouped_dates['time_0'] = grouped_dates['time_0'].apply(
        lambda row: date_to_unix(row))
    values = list(grouped_dates.to_dict('index').values())
    return values


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
    return final_values
