import logging
import sys

from data_manager.groups_per_column import group_users_per_column, user_jobs_groups, salary_information, \
    skill_demand_per_column, group_jobs_per_column
from data_manager.joined_ops import covered_skills_from_user, covered_cv_skills_from_course, \
    covered_application_skills_from_course, skill_relation_with_user_applications, get_user_skills_for_job, user_grades, \
    get_avg_course_names
from data_manager.limit_ops import popular_user_courses, popular_user_skills, popular_courses, popular_skills
from data_manager.projections import skill_demand_in_time, group_courses_users, enrolled_courses_applications_coverage
from data_manager.utils import recursive_search_trajectory
from visualiser.fake_data.fake_data import SANKEY_DATA_3

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)



def build_sankey_chart(request, **kwargs):
    """This function is used to build sankey charts"""
    base_query = request.GET.get('base_query', None)
    if base_query == 'career_path_trajectory':
        data = SANKEY_DATA_3
        trajectory_data = []
        trajectory_data = recursive_search_trajectory(data, trajectory_data)
        node_list = []
        for el in trajectory_data:
            if el["from"] not in node_list:
                node_list.append(el["from"])
            if el["to"] not in node_list:
                node_list.append(el["to"])
        return trajectory_data, node_list

def build_line_chart(request, **kwargs):
    """This function is used to build line charts"""
    base_query = request.GET.get('base_query', None)
    if base_query == 'skill_demand_in_time':
        skill_id = request.GET.get('skill_id', None)
        specialization = request.GET.get('specialization', None)
        if skill_id and specialization:
            print("i am here")
            values = skill_demand_in_time(skill_id=skill_id, specialization=specialization)
            return values


def build_circular_gauge(request, **kwargs):
    """This function is used as an abstract builder for Circular gauge"""
    base_query = request.GET.get('base_query', None)
    if base_query == 'skills_coverage':

        user_id = request.GET.get('user_id', None)
        job_id = request.GET.get('job_id', None)
        if user_id and job_id:
            values = covered_skills_from_user(user_id, job_id)
            return values
    elif base_query == 'skills_courses_skills_coverage':
        user_id = request.GET.get('user_id', None)
        if user_id:
            values = enrolled_courses_applications_coverage(user_id)
            return values
    elif base_query == 'course_match_cv':
        user_id = request.GET.get('user_id', None)
        course_id = request.GET.get('course_id', None)
        if user_id and course_id:
            value = covered_cv_skills_from_course(user_id, course_id)
            return value
    elif base_query == 'course_match_applications':
        user_id = request.GET.get('user_id', None)
        course_id = request.GET.get('course_id', None)
        if user_id and course_id:
            value = covered_application_skills_from_course(user_id, course_id)
            return value
    elif base_query == 'skill_match_applications':
        user_id = request.GET.get('user_id', None)
        skill_id = request.GET.get('skill_id', None)
        if user_id and skill_id:
            value = skill_relation_with_user_applications(user_id, skill_id)
            return value


def build_cylinder_gauge(request, **kwargs):
    """This function is used as an abstract builder for cylinder gauge"""
    base_query = request.GET.get('base_query', None)
    if base_query == 'skills_coverage':
        user_id = request.GET.get('user_id', None)
        job_id = request.GET.get('job_id', None)
        if user_id and job_id:
            values = covered_skills_from_user(user_id, job_id)
            print(values)
            return values
    elif base_query == 'course_match_cv':
        user_id = request.GET.get('user_id', None)
        course_id = request.GET.get('course_id', None)
        if user_id and course_id:
            value = covered_cv_skills_from_course(user_id, course_id)
            return value
    elif base_query == 'course_match_applications':
        user_id = request.GET.get('user_id', None)
        course_id = request.GET.get('course_id', None)
        if user_id and course_id:
            value = covered_application_skills_from_course(user_id, course_id)
            return value
    elif base_query == 'skill_match_applications':
        user_id = request.GET.get('user_id', None)
        skill_id = request.GET.get('skill_id', None)
        if user_id and skill_id:
            value = skill_relation_with_user_applications(user_id, skill_id)
            return value


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
    elif base_query == 'user_grades':
        user_id = request.GET.get('user_id', None)
        bar_chart_input = user_grades(user_id)
    elif base_query == 'courses_avg_grades':
        courses = request.GET.getlist('courses[]', [])
        print(courses)
        if courses:
            bar_chart_input = get_avg_course_names(courses)
    return bar_chart_input


def build_pie_chart(category_name, request, **kwargs):
    """This abstract function is used to call submethods/specific model"""
    base_query = request.GET.get("base_query", None)

    pie_chart_input = []
    if base_query == 'group_users':
        pie_chart_input = group_users_per_column(category_name)
    elif base_query == 'group_job_user':
        user_id = request.GET.get("user_id", None)
        pie_chart_input = user_jobs_groups(category_name, user_id)
    elif base_query == 'group_course_professor':
        limit_professors = int(request.GET.get("limit_professors", 10))
        asc_ordering = request.GET.get("asc", "False")
        pie_chart_input = group_courses_users(limit_professors, asc_ordering)
    elif base_query == 'group_jobs':
        column = request.GET.get("x_axis_name", "specialization")
        pie_chart_input = group_jobs_per_column(column)
    return pie_chart_input


def build_radar_chart(request, **kwargs):
    """This method is used to create radar chart"""
    base_query = request.GET.get('base_query', None)
    if base_query == 'user_job_skills':
        user_id = request.GET.get('user_id', None)
        job_id = request.GET.get('job_id', None)
        if user_id and job_id:
            values = get_user_skills_for_job(user_id=user_id, job_id=job_id)
            return values

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
