from data_manager.projections import retrieve_user_skills
from data_manager.utils import get_table


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