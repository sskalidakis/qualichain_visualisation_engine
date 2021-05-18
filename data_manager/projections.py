import pandas as pd
import datetime

from django.conf import settings
from data_manager.utils import get_table, date_to_unix, find_overlap_percentage, get_specialization_data
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
    """This function is used to find skill demand in a specific specialization in function of time"""
    if specialization:
        sql_command = """
        SELECT job_skill.skill_id, jobs.specialization_id, jobs.date_published
        FROM 
        (SELECT * from job_skills WHERE skill_id={skill_id}) as job_skill
        JOIN jobs
        ON job_skill.job_id=jobs.id
        WHERE specialization_id={specialization}
        """.format(**{'skill_id': skill_id, 'specialization': int(specialization)})
    else:
        sql_command = """
                SELECT job_skill.skill_id, jobs.date_published
                FROM 
                (SELECT * from job_skills WHERE skill_id={skill_id}) as job_skill
                JOIN jobs
                ON job_skill.job_id=jobs.id
                """.format(**{'skill_id': skill_id})
    skills_jobs_data = get_table(sql_command=sql_command)
    grouped_dates = skills_jobs_data.groupby('date_published').count().reset_index().rename(
        columns={'date_published': 'time', 'skill_id': 'skill_demand'})
    grouped_dates['time'] = grouped_dates['time'].apply(
        lambda row: int(datetime.datetime.strptime(row, '%Y-%m-%d %H:%M:%S.%f').timestamp()) * 1000)
    values = list(grouped_dates.to_dict('index').values())
    return values


def specialization_demand_in_time(specializations):
    """This function is used to find specialization demand in function of time"""
    specialization_title_values, specialization_id_values = get_specialization_data(titles=specializations)
    map_specializations = tuple(map(lambda x: specialization_title_values[x], tuple(specializations)))

    sql_command = """
    SELECT jobs.specialization_id, jobs.date_published, count(jobs.id) as count
    FROM jobs
    WHERE jobs.specialization_id IN {specializations}
    GROUP BY jobs.specialization_id, jobs.specialization_id, jobs.date_published
    ORDER BY jobs.specialization_id, jobs.date_published
    """.format(**{"specializations": map_specializations})
    specialization_demand_data = get_table(sql_command=sql_command).rename(
        columns={'date_published': 'time'})
    specialization_demand_data['time'] = specialization_demand_data['time'].apply(
        lambda row: int(datetime.datetime.strptime(row, '%Y-%m-%d %H:%M:%S.%f').timestamp()) * 1000)

    specialization_demand_data['specialization_id'] = specialization_demand_data['specialization_id'].apply(
        lambda x: specialization_id_values[x]
    )
    values = list(
        specialization_demand_data.pivot(index='time', columns='specialization_id', values='count').reset_index().fillna(
            0).to_dict('index').values())
    return values


def group_courses_users(limit, asc):
    """This function is used to find the number of courses per professor"""
    asc = convert_string_to_boolean(asc)
    user_courses_df = get_table(table='user_courses')
    professor_courses_df = user_courses_df.where(user_courses_df['course_status'] == 'taught')
    grouped_professor_courses_df = professor_courses_df[['user_id', 'id']].groupby('user_id').size().reset_index(
        name='count').sort_values('count', ascending=asc).tail(limit)
    users_df = get_table(table='users').rename(columns={'id': 'user_id', 'fullName': 'user_name'})
    professors_courses = pd.merge(grouped_professor_courses_df, users_df, how='left', on='user_id')[
        ['user_name', 'count']].sort_values('count', ascending=asc)
    final_values = list(professors_courses.to_dict('index').values())
    return final_values


def get_user_enrolled_courses_skills(user_id):
    """This function is used to retrieve a list of skills provided in user's enrolled courses"""
    user_enrolled_courses_df = get_table(
        sql_command="SELECT * FROM user_courses WHERE user_id={user_id} AND course_status='{status}'".format(
            **{'user_id': user_id, 'status': 'enrolled'})
    )
    courses = user_enrolled_courses_df['course_id'].tolist()
    if len(courses) > 1:
        skill_courses_df = get_table(
            sql_command="SELECT * FROM skills_courses WHERE course_id in {courses_tuple}".format(
                **{'courses_tuple': tuple(courses)})
        )
        enrolled_courses_skills = skill_courses_df['skill_id'].tolist()
    elif len(courses) == 1:
        skill_courses_df = get_table(
            sql_command="SELECT * FROM skills_courses WHERE course_id={course_id}".format(**{'course_id': courses[0]})
        )
        enrolled_courses_skills = skill_courses_df['skill_id'].tolist()
    else:
        enrolled_courses_skills = []
    return enrolled_courses_skills


def get_applied_job_skills(user_id):
    """This function is used to get the skills from jobs in which a user have applied"""
    user_job_applications_df = get_table(
        sql_command="SELECT * FROM user_applications WHERE user_id={user_id}".format(**{'user_id': user_id})
    )
    applied_jobs = user_job_applications_df['job_id'].tolist()
    if len(applied_jobs) > 1:
        applied_job_skills_df = get_table(
            sql_command="SELECT * FROM job_skills WHERE job_id in {job_tuple}".format(
                **{'job_tuple': tuple(applied_jobs)})
        )
        applied_jobs_skills = applied_job_skills_df['skill_id'].tolist()
    elif len(applied_jobs) == 1:
        applied_job_skills_df = get_table(
            sql_command="SELECT * FROM job_skills WHERE job_id={job_id}".format(**{'job_id': applied_jobs[0]})
        )
        applied_jobs_skills = applied_job_skills_df['skill_id'].tolist()
    else:
        applied_jobs_skills = []
    return applied_jobs_skills


def enrolled_courses_applications_coverage(user_id):
    """
    This function is used to find percentage coverage between user's enrolled courses and job skills that has applied
    """
    enrolled_courses_skills = get_user_enrolled_courses_skills(user_id)
    applied_jobs_skills = get_applied_job_skills(user_id)
    overlap_percentage = find_overlap_percentage(nominator=enrolled_courses_skills, denominator=applied_jobs_skills)
    return overlap_percentage


def fetch_user_cv_skills(user_id):
    """This function is used to fetch user skills info"""
    user_skills_command = """
        SELECT cv_id, skill_id, skil_level FROM cv_skills
        JOIN (
            SELECT id FROM "CVs" WHERE user_id={user_id}
            ) AS user_cv
        ON cv_skills.cv_id=user_cv.id
    """.format(**{'user_id': user_id})
    user_skills_df = get_table(sql_command=user_skills_command)
    return user_skills_df


def fetch_job_skills(job_id):
    """This function is used to fetch job skills"""
    job_skills_command = """SELECT skill_id FROM job_skills WHERE job_id={job_id}""".format(**{'job_id': job_id})
    job_skills_df = get_table(sql_command=job_skills_command)
    return job_skills_df
