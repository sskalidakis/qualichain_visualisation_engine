import logging
import sys
from django.shortcuts import render

from data_manager.manager import build_bar_chart, build_pie_chart, build_circular_gauge, build_line_chart, \
    build_cylinder_gauge, build_sankey_chart, build_radar_chart
from visualiser.fake_data.fake_data import COLUMNCHART_DATA, RADAR_CHART_DATA, BAR_HEATMAP_DATA_2, \
    BAR_RANGE_CHART_DATA_2, SANKEYCHORD_DATA_2, HEAT_MAP_DATA_FOR_MAP, GAUGE_DATA, SANKEYCHORD_DATA, SANKEY_DATA_3

from django.views.decorators.csrf import csrf_exempt

from visualiser.utils import *
import json

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


class XYZ_chart:
    def __init__(self, request, x_axis_name, x_axis_title, x_axis_unit, x_sec_axis, y_axis_name, y_axis_title,
                 y_axis_unit, z_axis_name, z_axis_title, z_axis_unit, chart_data, color_list, minmax_z_value, distinct,
                 ranges, chart_type):
        """
        :param request: Contains all request data needed to render the HTML page. (Request Object)
        :param x_axis_name: The unique name of the selected variable of the X-Axis as used in the code (String)
        :param x_axis_title: The title of the variable of the X-Axis as displayed in the user interfaces (String)
        :param x_sec_axis: True if second X-Axis exists (otherwies false) (String)
        :param x_axis_unit: The unit of the selected variable of the X-Axis (String)
        :param y_axis_name: The unique name of the selected variable of the Y-Axis as used in the code (String)
        :param y_axis_title: The title of the variable of the Y-Axis as displayed in the user interfaces (String)
        :param y_axis_unit: The unit of the selected variable of the Y-Axis (String)
        :param z_axis_name: The unique name of the selected variable of the Z-Axis as used in the code (String)
        :param z_axis_title: The title of the variable of the Z-Axis as displayed in the user interfaces (String)
        :param z_axis_unit: The unit of the selected variable of the Z-Axis (String)
        :param chart_data: A JSON object in the appropriate format  that contains the data that will displayed. (JSON Object)
        :param color_list: List of colours (for each series) or a colour couple (for heatmaps. If one color is given in
                a heatmap, then the couple is created using white as the other colour). (List of Strings)
                Colours: “light_blue, blue, violet, purple, fuchsia, red, ceramic, light_brown, mustard, gold,
                light_green, green, cyan, black, gray, white”
                Colour couples: "blue_red, green_red, beige_purple, purple_orange, cyan_green, yellow_gold, skin_red,
                grey_darkblue, lightblue_green"
        :param minmax_z_value: A two-element list that contains the min and max value of the variables on the Z-Axis. (List of Numbers)
        :param chart_type: The type of the chart. Options : heat_map_chart
        :param distinct: defines a list of distinct values that will be presented with different colors on the heatmap (list of values)
        :param ranges: Used in case a dataset in the from-to-value format is given including the data used to create guidelines or ranges on the chart
        """
        self.x_axis_name = x_axis_name
        self.x_axis_title = x_axis_title
        self.x_axis_unit = x_axis_unit
        self.x_sec_axis = x_sec_axis
        self.y_axis_name = y_axis_name
        self.y_axis_title = y_axis_title
        self.y_axis_unit = y_axis_unit
        self.z_axis_name = z_axis_name
        self.z_axis_title = z_axis_title
        self.z_axis_unit = z_axis_unit
        self.chart_data = chart_data
        self.request = request
        self.chart_type = chart_type
        self.color_list = color_list
        self.minmax_z_value = minmax_z_value
        self.distinct = distinct
        self.ranges = ranges
        self.content = {'x_axis_title': self.x_axis_title, 'x_axis_unit': self.x_axis_unit,
                        'x_axis_name': self.x_axis_name, 'x_sec_axis': self.x_sec_axis,
                        'y_axis_title': self.y_axis_title, 'y_axis_unit': self.y_axis_unit,
                        'y_axis_name': self.y_axis_name, 'z_axis_name': z_axis_name, 'z_axis_title': z_axis_title,
                        'z_axis_unit': z_axis_unit, 'color_list': self.color_list,
                        'minmax_z_value': self.minmax_z_value,
                        'distinct': distinct, 'ranges': self.ranges, 'chart_data': self.chart_data}

    def show_chart(self):
        """
        :return: Returns visualisation HTML.
        """
        if self.chart_type == 'heat_map_chart':
            return render(self.request, 'visualiser/heat_map_chart.html',
                          self.content)


class XY_chart:
    def __init__(self, request, x_axis_name, x_axis_title, x_axis_unit, y_var_names, y_var_titles, y_var_units,
                 x_axis_type, y_axis_title, chart_data, color_list, use_default_colors, chart_3d, minmax_y_value,
                 chart_type):
        """
        :param request: Contains all request data needed to render the HTML page. (Request Object)
        :param x_axis_name: The unique name of the selected variable of the X-Axis as used in the code (String)
        :param x_axis_title: The title of the variable of the X-Axis as displayed in the user interfaces (String)
        :param x_axis_unit: The unit of the selected variable of the X-Axis (String)
        :param y_var_names: A list of names of the variables presented on the Y-Axis as used in the code (List of Strings)
        :param y_var_titles:A list of titles of the variables presented on the Y-Axis as displayed in the interfaces. (List of Strings)
        :param y_var_units: A list of units of the variables presented on the Y-Axis as displayed in the user interfaces (List of Strings)
        :param x_axis_type: The type of the X-Axis. Options: “Time”, “Text”, “Number”. (String)
        :param y_axis_title: General title of the Y-Axis (common for all variables) (String)
        :param chart_data: A JSON object in the appropriate format  that contains the data that will displayed. (JSON Object)
        :param color_list: List of colours (for each series) or a colour couple (for heatmaps. If one color is given in
                a heatmap, then the couple is created using white as the other colour). (List of Strings)
                Colours: “light_blue, blue, violet, purple, fuchsia, red, ceramic, light_brown, mustard, gold,
                light_green, green, cyan, black, gray, white”
                Colour couples: "blue_red, green_red, beige_purple, purple_orange, cyan_green, yellow_gold, skin_red,
                grey_darkblue, lightblue_green"
        :param use_default_colors: If “true”, the default colours are used for the chosen chart (String: "true" or "false")
        :param chart_3d: If “true”, the chart is displayed in three dimensions. (not all visualisations support 3D) (String: "true" or "false")
        :param minmax_y_value: A two-element list that contains the min and max value of the variables on the Y-Axis. (List of Numbers)
        :param chart_type: The type of the chart. Options : line_chart, column_chart, range_chart, bar_range_chart,
                stacked_column_chart, column_heatmap_chart, pie_chart, radar_chart
        """
        self.x_axis_name = x_axis_name
        self.x_axis_title = x_axis_title
        self.x_axis_unit = x_axis_unit
        self.y_var_names = y_var_names
        self.y_var_titles = y_var_titles
        self.y_var_units = y_var_units
        self.chart_data = chart_data
        self.request = request
        self.chart_type = chart_type
        self.x_axis_type = x_axis_type
        self.y_axis_title = y_axis_title
        self.color_list = color_list
        self.use_default_colors = use_default_colors
        self.chart_3d = chart_3d
        self.minmax_y_value = minmax_y_value
        print(minmax_y_value)
        self.content = {'x_axis_title': self.x_axis_title, 'x_axis_unit': self.x_axis_unit,
                        'x_axis_name': self.x_axis_name, 'y_var_titles': self.y_var_titles,
                        'y_var_units': self.y_var_units, 'y_var_names': self.y_var_names,
                        'x_axis_type': self.x_axis_type, 'y_axis_title': self.y_axis_title,
                        'color_list': self.color_list, 'use_default_colors': self.use_default_colors,
                        'chart_3d': self.chart_3d, 'minmax_y_value': self.minmax_y_value, 'chart_data': self.chart_data}

    def show_chart(self):
        """
        :return: Returns visualisation HTML.
        """
        if self.chart_type == 'line_chart':
            return render(self.request, 'visualiser/line_chart_am4.html',
                          self.content)
        elif self.chart_type == 'column_chart':
            return render(self.request, 'visualiser/column_chart_am4.html',
                          self.content)
        elif self.chart_type == 'ma_column_chart':
            return render(self.request, 'visualiser/multi_axial_column_chart_am4.html',
                          self.content)
        elif self.chart_type == 'range_chart':
            return render(self.request, 'visualiser/range_chart_am4.html',
                          self.content)
        elif self.chart_type == 'bar_range_chart':
            return render(self.request, 'visualiser/bar_range_chart_am4.html',
                          self.content)
        elif self.chart_type == 'stacked_column_chart':
            return render(self.request, 'visualiser/stacked_column_chart_am4.html',
                          self.content)
        elif self.chart_type == 'bar_heat_map_chart':
            return render(self.request, 'visualiser/bar_heat_map.html',
                          self.content)
        elif self.chart_type == 'pie_chart':
            return render(self.request, 'visualiser/pie_chart.html',
                          self.content)
        elif self.chart_type == 'radar_chart':
            return render(self.request, 'visualiser/radar_chart.html',
                          self.content)


class X_chart:
    def __init__(self, request, x_axis_name, x_axis_title, x_axis_unit, chart_data, color_list, use_default_colors,
                 chart_3d, minmax_y_value, chart_type):
        """
        :param request: Contains all request data needed to render the HTML page. (Request Object)
        :param x_axis_name: The unique name of the selected variable of the X-Axis as used in the code (String)
        :param x_axis_title: The title of the variable of the X-Axis as displayed in the user interfaces (String)
        :param x_axis_unit: The unit of the selected variable of the X-Axis (String)
        :param chart_data: A JSON object in the appropriate format  that contains the data that will displayed. (JSON Object)
        :param color_list: Colour (for cylinder gauge) or a colour couple (for circular_gauge). (List of Strings)
                Colours: “light_blue, blue, violet, purple, fuchsia, red, ceramic, light_brown, mustard, gold,
                light_green, green, cyan, black, gray, white”
                Colour couples: "blue_red, green_red, beige_purple, purple_orange, cyan_green, yellow_gold, skin_red,
                grey_darkblue, lightblue_green"
        :param use_default_colors: If “true”, the default colours are used for the chosen chart (String: "true" or "false")
        :param chart_3d: If “true”, the chart is displayed in three dimensions. (not all visualisations support 3D) (String: "true" or "false")
        :param minmax_y_value: A two-element list that contains the min and max value of the variables on the Y-Axis. (List of Numbers)
        :param chart_type: circular_gauge_chart, cylinder_gauge_chart
        """
        self.x_axis_name = x_axis_name
        self.x_axis_title = x_axis_title
        self.x_axis_unit = x_axis_unit
        self.chart_data = chart_data
        self.request = request
        self.chart_type = chart_type
        self.color_list = color_list
        self.use_default_colors = use_default_colors
        self.chart_3d = chart_3d
        self.minmax_y_value = minmax_y_value
        print(minmax_y_value)
        self.content = {'x_axis_title': self.x_axis_title, 'x_axis_unit': self.x_axis_unit,
                        'x_axis_name': self.x_axis_name, 'color_list': self.color_list,
                        'use_default_colors': self.use_default_colors, 'chart_3d': self.chart_3d,
                        'min_max_y_value': self.minmax_y_value, 'chart_data': self.chart_data}

    def show_chart(self):
        """
        :return: Returns visualisation HTML.
        """
        if self.chart_type == 'circular_gauge_chart':
            return render(self.request, 'visualiser/circular_gauge_chart.html',
                          self.content)
        elif self.chart_type == 'cylinder_gauge_chart':
            return render(self.request, 'visualiser/cylinder_gauge_chart.html',
                          self.content)


class FlowChart:
    """
    Sankey chart and Chord diagram have the same format of data
    """

    def __init__(self, request, data, node_list, color_node_list, use_def_colors, chart_title, chart_type):
        """
        :param request: Contains all request data needed to render the HTML page. (Request Object)
        :param data: The JSON Object containing the data to be visualised (JSON Object)
        :param node_list: A list of names of all the nodes in the diagrams as used in the JSON Object.(list of Strings)
        :param color_node_list: A list of colours, one for each node. (List of Strings)
        :param use_def_colors: If true colour list is ignored and default colours are used. (String: "true" or "false")
        :param chart_title: The title of the created chart. (String)
        :param chart_type: The type of the chart. (String: "sankey_diagram" or "chord_diagram")
        """
        self.request = request
        self.chart_type = chart_type
        self.node_list = node_list
        self.color_node_list = color_node_list
        self.use_def_colors = use_def_colors
        self.chart_title = chart_title
        self.data = data
        self.content = {"data": self.data, "node_list": self.node_list,
                        "color_node_list": self.color_node_list, "use_default_colors": self.use_def_colors,
                        "chart_title": self.chart_title}

    def show_chart(self):
        """
        :return: Returns visualisation HTML.
        """
        if (self.chart_type == "chord_diagram"):
            return render(self.request, 'visualiser/chord_diagram.html', self.content)
        elif (self.chart_type == "sankey_diagram"):
            return render(self.request, 'visualiser/sankey_diagram.html', self.content)


class MapChart:
    """
    This class contains all map visualisations
    """

    def __init__(self, request, map_data, projection, color_couple, map_var_name, map_var_title, map_var_unit,
                 min_max_value, chart_type):
        """
        :param chart_type:
        :param data:
        """

        self.request = request
        self.chart_type = chart_type
        self.map_data = map_data
        self.projection = projection
        self.color_couple = color_couple
        self.map_var_name = map_var_name
        self.map_var_title = map_var_title
        self.map_var_unit = map_var_unit
        self.min_max_value = min_max_value
        self.content = {"map_data": map_data, "projection": projection, "color_list": color_couple,
                        "map_var_name": map_var_name, "map_var_title": map_var_title, "map_var_unit": map_var_unit,
                        "minmax_y_value": min_max_value}

    def show_chart(self):
        """
        :return: Returns visualisation HTML.
        """
        if (self.chart_type == "heatmap_on_map"):
            return render(self.request, 'visualiser/heat_map_on_map.html', self.content)


@csrf_exempt
def get_response_data_XY(request):
    if request.method == "GET":
        json_response = {
            "y_var_names": request.GET.getlist("y_var_names[]", []),
            "y_var_titles": request.GET.getlist("y_var_titles[]", []),
            "y_var_units": request.GET.getlist("y_var_units[]", []),
            "x_axis_type": request.GET.get("x_axis_type", ""),
            "x_axis_name": request.GET.get("x_axis_name", ""),
            "x_axis_title": request.GET.get("x_axis_title", ""),
            "x_axis_unit": request.GET.get("x_axis_unit", ""),
            "x_sec_axis": request.GET.get("x_sec_axis", ""),
            "y_axis_title": request.GET.get("y_axis_title", ""),
            "color_list_request": request.GET.getlist("color_list_request[]", []),
            "use_default_colors": request.GET.get("use_default_colors", "true"),
            "chart_3d": request.GET.get("chart_3d", "false"),
            "min_max_y_value": request.GET.getlist("min_max_y_value[]", []),
            "dataset": request.GET.get("dataset", ""),
            "dataset_type": request.GET.get("dataset_type", "file"),
            "distinct": request.GET.getlist("distinct[]", [])
        }
    else:
        json_response = json.loads(request.body.decode('utf-8'))
    return json_response


@csrf_exempt
def show_circular_gauge_chart(request):
    response_data = get_response_data_XY(request)
    print(response_data)
    x_axis_name = response_data['x_axis_name']
    x_axis_title = response_data['x_axis_title']
    x_axis_unit = response_data['x_axis_unit']
    color_list_request = response_data['color_list_request']
    use_default_colors = response_data['use_default_colors']
    chart_3d = ""
    min_max_y_value = response_data['min_max_y_value']
    dataset = response_data['dataset']
    # TODO: Create a method for getting the actual data from DBs, CSV files, dataframes??
    data = build_circular_gauge(request)

    color_list = define_color_code_list(color_list_request)

    circular_gauge_chart = X_chart(request, x_axis_name, x_axis_title, x_axis_unit, data, color_list,
                                   use_default_colors,
                                   chart_3d, min_max_y_value, 'circular_gauge_chart')
    return circular_gauge_chart.show_chart()


@csrf_exempt
def show_cylinder_gauge_chart(request):
    response_data = get_response_data_XY(request)
    print(response_data)
    x_axis_name = response_data['x_axis_name']
    x_axis_title = response_data['x_axis_title']
    x_axis_unit = response_data['x_axis_unit']
    color_list_request = response_data['color_list_request']
    use_default_colors = response_data['use_default_colors']
    min_max_y_value = response_data['min_max_y_value']
    chart_3d = "false"
    dataset = response_data['dataset']
    data = build_cylinder_gauge(request)
    # data = GAUGE_DATA
    color_list = define_color_code_list(color_list_request)

    cylinder_gauge_chart = X_chart(request, x_axis_name, x_axis_title, x_axis_unit, data, color_list,
                                   use_default_colors,
                                   chart_3d, min_max_y_value, 'cylinder_gauge_chart')
    return cylinder_gauge_chart.show_chart()


@csrf_exempt
def show_line_chart(request):
    response_data = get_response_data_XY(request)
    print(response_data)
    y_var_names = response_data['y_var_names']
    y_var_titles = response_data['y_var_titles']
    y_var_units = response_data['y_var_units']
    x_axis_type = response_data['x_axis_type']
    x_axis_name = response_data['x_axis_name']
    x_axis_title = response_data['x_axis_title']
    x_axis_unit = response_data['x_axis_unit']
    y_axis_title = response_data['y_axis_title']
    color_list_request = response_data['color_list_request']
    use_default_colors = response_data['use_default_colors']
    chart_3d = ""
    min_max_y_value = response_data['min_max_y_value']
    dataset = response_data['dataset']

    # TODO: Create a method for getting the actual data from DBs, CSV files, dataframes??
    # data = FAKE_DATA
    data = build_line_chart(request)

    color_list = define_color_code_list(color_list_request)

    line_chart = XY_chart(request, x_axis_name, x_axis_title, x_axis_unit, y_var_names, y_var_titles, y_var_units,
                          x_axis_type, y_axis_title, data, color_list, use_default_colors, chart_3d, min_max_y_value,
                          'line_chart')
    return line_chart.show_chart()


@csrf_exempt
def show_column_chart(request):
    # Use get_response_data_XY to get the same variables
    response_data = get_response_data_XY(request)
    y_var_names = response_data["y_var_names"]
    y_var_titles = response_data["y_var_titles"]
    y_var_units = response_data["y_var_units"]
    x_axis_type = response_data["x_axis_type"]
    x_axis_name = response_data["x_axis_name"]
    x_axis_title = response_data["x_axis_title"]
    x_axis_unit = response_data["x_axis_unit"]
    y_axis_title = response_data["y_axis_title"]
    min_max_y_value = response_data["min_max_y_value"]
    color_list_request = response_data["color_list_request"]
    use_default_colors = response_data["use_default_colors"]
    chart_3d = response_data["chart_3d"]
    # TODO: Create a method for getting the actual data from DBs, CSV files, dataframes??
    data = build_bar_chart(x_axis_name, request=request)

    color_list = define_color_code_list(color_list_request)
    column_chart = XY_chart(request, x_axis_name, x_axis_title, x_axis_unit, y_var_names, y_var_titles, y_var_units,
                            x_axis_type, y_axis_title, data, color_list, use_default_colors, chart_3d, min_max_y_value,
                            'column_chart')
    return column_chart.show_chart()


def show_ma_column_chart(request):
    # Use get_response_data_XY to get the same variables
    response_data = get_response_data_XY(request)
    y_var_names = response_data["y_var_names"]
    y_var_titles = response_data["y_var_titles"]
    y_var_units = response_data["y_var_units"]
    x_axis_type = response_data["x_axis_type"]
    x_axis_name = response_data["x_axis_name"]
    x_axis_title = response_data["x_axis_title"]
    x_axis_unit = response_data["x_axis_unit"]
    y_axis_title = response_data["y_axis_title"]
    min_max_y_value = response_data["min_max_y_value"]
    color_list_request = response_data["color_list_request"]
    use_default_colors = response_data["use_default_colors"]
    chart_3d = response_data["chart_3d"]
    # TODO: Create a method for getting the actual data from DBs, CSV files, dataframes??
    data = build_bar_chart(x_axis_name, request)
    data = COLUMNCHART_DATA
    color_list = define_color_code_list(color_list_request)
    column_chart = XY_chart(request, x_axis_name, x_axis_title, x_axis_unit, y_var_names, y_var_titles, y_var_units,
                            x_axis_type, y_axis_title, data, color_list, use_default_colors, chart_3d, min_max_y_value,
                            'ma_column_chart')
    return column_chart.show_chart()


@csrf_exempt
def show_pie_chart(request):
    response_data = get_response_data_XY(request)
    variable_name = response_data["y_var_names"]
    variable_title = response_data["y_var_titles"]
    variable_unit = response_data["y_var_units"]
    category_name = response_data["x_axis_name"]
    category_title = response_data["x_axis_title"]
    category_unit = response_data["x_axis_unit"]
    x_axis_type = response_data["x_axis_type"]
    y_axis_title = response_data["y_axis_title"]
    color_list_request = response_data["color_list_request"]
    min_max_y_value = response_data["min_max_y_value"]
    chart_3d = response_data["chart_3d"]
    use_default_colors = response_data["use_default_colors"]
    data = build_pie_chart(category_name, request)
    color_list = define_color_code_list(color_list_request)

    pie_chart = XY_chart(request, category_name, category_title, category_unit, variable_name, variable_title,
                         variable_unit,
                         x_axis_type, y_axis_title, data, color_list, use_default_colors, chart_3d, min_max_y_value,
                         'pie_chart')
    return pie_chart.show_chart()


def show_radar_chart(request):
    response_data = get_response_data_XY(request)
    variable_name = response_data["y_var_names"]
    variable_title = response_data["y_var_titles"]
    variable_unit = response_data["y_var_units"]
    category_name = response_data["x_axis_name"]
    category_title = response_data["x_axis_title"]
    category_unit = response_data["x_axis_unit"]
    x_axis_type = response_data["x_axis_type"]
    y_axis_title = response_data["y_axis_title"]
    color_list_request = response_data["color_list_request"]
    min_max_y_value = response_data["min_max_y_value"]
    chart_3d = response_data["chart_3d"]
    use_default_colors = response_data["use_default_colors"]
    data = build_radar_chart(request)
    color_list = define_color_code_list(color_list_request)
    radar_chart = XY_chart(request, category_name, category_title, category_unit, variable_name, variable_title,
                           variable_unit, x_axis_type, y_axis_title, data, color_list, use_default_colors, chart_3d,
                           min_max_y_value, 'radar_chart')
    return radar_chart.show_chart()


def show_range_chart(request):
    response_data_xy = get_response_data_XY(request)
    y_var_names = response_data_xy['y_var_names']
    y_var_titles = response_data_xy['y_var_titles']
    y_var_units = response_data_xy['y_var_units']
    x_axis_type = response_data_xy['x_axis_type']
    x_axis_name = response_data_xy['x_axis_name']
    x_axis_title = response_data_xy['x_axis_title']
    x_axis_unit = response_data_xy['x_axis_unit']
    y_axis_title = response_data_xy['y_axis_title']
    color_list_request = response_data_xy['color_list_request']
    use_default_colors = response_data_xy['use_default_colors']
    min_max_y_value = response_data_xy["min_max_y_value"]
    chart_3d = response_data_xy["chart_3d"]
    # data = FAKE_DATA
    data = generate_data_for_range_chart()
    color_list = define_color_code_list(color_list_request)
    range_chart = XY_chart(request, x_axis_name, x_axis_title, x_axis_unit, y_var_names, y_var_titles, y_var_units,
                           x_axis_type, y_axis_title, data, color_list, use_default_colors, chart_3d, min_max_y_value,
                           'range_chart')
    return range_chart.show_chart()


def show_bar_range_chart(request):
    response_data_xy = get_response_data_XY(request)
    y_var_names = response_data_xy['y_var_names']
    y_var_titles = response_data_xy['y_var_titles']
    y_var_units = response_data_xy['y_var_units']
    x_axis_type = response_data_xy['x_axis_type']
    x_axis_name = response_data_xy['x_axis_name']
    x_axis_title = response_data_xy['x_axis_title']
    x_axis_unit = response_data_xy['x_axis_unit']
    y_axis_title = response_data_xy['y_axis_title']
    color_list_request = response_data_xy['color_list_request']
    use_default_colors = response_data_xy['use_default_colors']
    min_max_y_value = response_data_xy["min_max_y_value"]
    chart_3d = response_data_xy["chart_3d"]
    # data = BAR_RANGE_CHART_DATA
    data = BAR_RANGE_CHART_DATA_2
    color_list = define_color_code_list(color_list_request)
    bar_range_chart = XY_chart(request, x_axis_name, x_axis_title, x_axis_unit, y_var_names, y_var_titles, y_var_units,
                               x_axis_type, y_axis_title, data, color_list, use_default_colors, chart_3d,
                               min_max_y_value, 'bar_range_chart')
    return bar_range_chart.show_chart()


def show_stacked_column_chart(request):
    response_data_xy = get_response_data_XY(request)
    y_var_names = response_data_xy['y_var_names']
    y_var_titles = response_data_xy['y_var_titles']
    y_var_units = response_data_xy['y_var_units']
    x_axis_type = response_data_xy['x_axis_type']
    x_axis_name = response_data_xy['x_axis_name']
    x_axis_title = response_data_xy['x_axis_title']
    x_axis_unit = response_data_xy['x_axis_unit']
    y_axis_title = response_data_xy['y_axis_title']
    color_list_request = response_data_xy['color_list_request']
    use_default_colors = response_data_xy['use_default_colors']
    min_max_y_value = response_data_xy["min_max_y_value"]
    chart_3d = response_data_xy["chart_3d"]
    data = COLUMNCHART_DATA
    print(data)
    color_list = define_color_code_list(color_list_request)
    stacked_column_chart = XY_chart(request, x_axis_name, x_axis_title, x_axis_unit, y_var_names, y_var_titles,
                                    y_var_units,
                                    x_axis_type, y_axis_title, data, color_list, use_default_colors, chart_3d,
                                    min_max_y_value,
                                    'stacked_column_chart')
    return stacked_column_chart.show_chart()


def show_bar_heat_map(request):
    response_data_xy = get_response_data_XY(request)
    y_var_names = response_data_xy['y_var_names']
    y_var_titles = response_data_xy['y_var_titles']
    y_var_units = response_data_xy['y_var_units']
    x_axis_type = response_data_xy['x_axis_type']
    x_axis_name = response_data_xy['x_axis_name']
    x_axis_title = response_data_xy['x_axis_title']
    x_axis_unit = response_data_xy['x_axis_unit']
    y_axis_title = response_data_xy['y_axis_title']
    color_list_request = response_data_xy['color_list_request'][0]
    use_default_colors = response_data_xy['use_default_colors']
    min_max_y_value = response_data_xy["min_max_y_value"]
    chart_3d = response_data_xy["chart_3d"]
    data = BAR_HEATMAP_DATA_2
    # TODO check this color_list_request
    color_couple = AM_CHARTS_COLOR_HEATMAP_COUPLES[color_list_request]
    bar_heat_map_chart = XY_chart(request, x_axis_name, x_axis_title, x_axis_unit, y_var_names, y_var_titles,
                                  y_var_units, x_axis_type, y_axis_title, data, color_couple, use_default_colors,
                                  chart_3d, min_max_y_value, 'bar_heat_map_chart')

    return bar_heat_map_chart.show_chart()


@csrf_exempt
def get_response_heat_map(request):
    if request.method == "GET":
        json_response = {
            "z_axis_name": request.GET.get("z_axis_name", ""),
            "z_axis_title": request.GET.get("z_axis_title", ""),
            "z_axis_unit": request.GET.get("z_axis_unit", ""),
            "min_max_z_value": request.GET.getlist("min_max_z_value[]", []),
        }
    else:
        json_response = json.loads(request.body)
        print(json_response)
    return json_response


@csrf_exempt
def show_heat_map_chart(request):
    response_data_xy = get_response_data_XY(request)
    y_axis_name = response_data_xy['y_var_names'][0]
    y_axis_unit = response_data_xy['y_var_units'][0]
    x_axis_name = response_data_xy['x_axis_name']
    x_axis_title = response_data_xy['x_axis_title']
    x_axis_unit = response_data_xy['x_axis_unit']
    x_sec_axis = response_data_xy['x_sec_axis']
    y_axis_title = response_data_xy['y_axis_title']
    response_heat_map = get_response_heat_map(request)
    z_axis_name = response_heat_map["z_axis_name"]
    z_axis_title = response_heat_map["z_axis_title"]
    z_axis_unit = response_heat_map["z_axis_unit"]
    min_max_z_value = response_heat_map["min_max_z_value"]
    distinct = response_data_xy['distinct']
    dataset = response_data_xy['dataset']
    dataset_type = response_data_xy['dataset_type']
    row_categorisation_dataset = request.GET.get("row_categorisation_dataset", "")
    col_order = request.GET.get("col_order", "default")
    row_order = request.GET.get("row_order", "default")
    if len(distinct) == 0:
        color_list_request = response_data_xy['color_list_request'][0]
        color_list = AM_CHARTS_COLOR_HEATMAP_COUPLES.get(color_list_request,
                                                         define_color_code_list([color_list_request]))
    else:
        color_list = define_color_code_list(response_data_xy['color_list_request'])
    data, ranges = create_heatmap_data(dataset, row_categorisation_dataset, col_order, row_order, dataset_type)
    heat_map_chart = XYZ_chart(request, x_axis_name, x_axis_title, x_axis_unit, x_sec_axis, y_axis_name, y_axis_title,
                               y_axis_unit, z_axis_name, z_axis_title, z_axis_unit, data, color_list,
                               min_max_z_value, distinct, ranges, 'heat_map_chart')

    return heat_map_chart.show_chart()


@csrf_exempt
def get_response_flow_diagram(request):
    if request.method == "GET":
        json_response = {
            "use_def_colors": request.GET.get("use_def_colors", "false"),
            "chart_title": request.GET.get("chart_title", ""),
            "node_list": request.GET.getlist("node_list[]", []),
            "color_node_list": request.GET.getlist("color_node_list[]", []),
        }
    else:
        json_response = json.loads(request.body)
        print(json_response)
    return json_response


def sankey_diagram(request):
    """
    As input we will take a dict with key the begin and value a list with first element end and second the value
    :param request:
    :return:
    """
    response_sankey_diagram = get_response_flow_diagram(request)
    use_def_colors = response_sankey_diagram["use_def_colors"]
    chart_title = response_sankey_diagram["chart_title"]
    color_node_list = response_sankey_diagram["color_node_list"]
    # From utils use AM_CHARTS_COLOR_CODES_LIST to convert colors' names to hex code of given colors
    color_node_list = [AM_CHARTS_COLOR_CODES_LIST[color_name] for color_name in color_node_list]
    data, node_list = build_sankey_chart(request)
    sankey_diagram = FlowChart(request, data, node_list, color_node_list, use_def_colors, chart_title, 'sankey_diagram')
    return sankey_diagram.show_chart()


def chord_diagram(request):
    """
    As in put we will take a dict with key the begin and value a list with first element end and second the value
    :param request:
    :return:
    """
    response_sankey_diagram = get_response_flow_diagram(request)
    node_list = response_sankey_diagram["node_list"]
    use_def_colors = response_sankey_diagram["use_def_colors"]
    chart_title = response_sankey_diagram["chart_title"]
    color_node_list = response_sankey_diagram["color_node_list"]
    # From utils use AM_CHARTS_COLOR_CODES_LIST to convert colors' names to hex code of given colors
    color_node_list = [AM_CHARTS_COLOR_CODES_LIST[color_name] for color_name in color_node_list]
    data = SANKEYCHORD_DATA_2
    chord_diagram = FlowChart(request, data, node_list, color_node_list, use_def_colors, chart_title, 'chord_diagram')
    return chord_diagram.show_chart()


#
# @csrf_exempt
# def get_response_parallel_coordinates_chart(request):
#     if request.method == "GET":
#         json_response = {
#             "y_axes": request.GET.getlist("y_axes[]", []),
#             "title": request.GET.get("title", ""),
#             "about_title": request.GET.get("title", ""),
#             "about_text": request.GET.get("text", ""),
#             "groups_title": request.GET.get("groups_title", ""),
#             "samples_size": request.GET.get("samples_size", "10"),
#
#         }
#     else:
#         json_response = json.loads(request.body)
#         print(json_response)
#     return json_response
#
#
# def parallel_coordinates_chart(request):
#     """
#     y_axes the name of columns
#     data a list of lists, each list must have the same length of y_axes
#     slice_size define how much rows be visualid, in table below graph
#
#     :param request:
#     :return:
#     """
#     response_parallel_coordinates_chart = get_response_parallel_coordinates_chart(request)
#     y_axes = response_parallel_coordinates_chart["y_axes"]
#     slice_size = response_parallel_coordinates_chart["samples_size"]
#     data = PARALLEL_COORDINATES_DATA
#     return render(request, 'visualiser/parallel_coordinates_chart.html',
#                   {"y_axes": y_axes, "data": data, "slice_size": slice_size})


@csrf_exempt
def get_response_heat_map_on_map(request):
    if request.method == "GET":
        json_response = {
            "projection": request.GET.get("projection", ""),
            "map_var_name": request.GET.get("map_var_name", ""),
            "map_var_title": request.GET.get("map_var_title", ""),
            "map_var_unit": request.GET.get("map_var_unit", "")
        }
    else:
        json_response = json.loads(request.body)
        print(json_response)
    return json_response


def heat_map_on_map(request):
    response_heat_map_on_map = get_response_heat_map_on_map(request)
    projection = response_heat_map_on_map["projection"]
    map_var_name = response_heat_map_on_map["map_var_name"]
    map_var_title = response_heat_map_on_map["map_var_title"]
    map_var_unit = response_heat_map_on_map["map_var_unit"]
    response_data_xy = get_response_data_XY(request)
    color_list_request = response_data_xy["color_list_request"][0]
    min_max_value = response_data_xy["min_max_y_value"]
    map_data = HEAT_MAP_DATA_FOR_MAP
    # map_data = generate_data_for_heat_map()
    color_couple = AM_CHARTS_COLOR_HEATMAP_COUPLES[color_list_request]
    heatmap_on_map = MapChart(request, map_data, projection, color_couple, map_var_name, map_var_title, map_var_unit,
                              min_max_value, 'heatmap_on_map')
    return heatmap_on_map.show_chart()

# def thermometer_chart(request):
#     recordData = {}
#     for i in range(1, 11):
#         temp = []
#         for j in THERMOMETER:
#             t = {"date": j["date"], "value": j["value"] * i}
#             temp.append(t)
#         recordData[i] = temp
#     response_thermometer_chart = get_response_data_XY(request)
#     min_max_temp = response_thermometer_chart["min_max_y_value"]
#     min_temp = min_max_temp[0]
#     max_temp = min_max_temp[1]
#     return render(request, 'visualiser/thermometer_chart.html', {"data": THERMOMETER, "recordData": recordData,
#                                                                  "min_temp": min_temp, "max_temp": max_temp})
#
#
# def parallel_coordinates_chart2(request):
#     """
#     :param request:
#     :return:
#     """
#     response_parallel_coordinates_chart2 = get_response_parallel_coordinates_chart(request)
#     y_axes = response_parallel_coordinates_chart2["y_axes"]
#     title = response_parallel_coordinates_chart2["title"]
#     about_title = response_parallel_coordinates_chart2["about_title"]
#     about_text = response_parallel_coordinates_chart2["about_text"]
#     groups_title = response_parallel_coordinates_chart2["groups_title"]
#     sample_size = response_parallel_coordinates_chart2["samples_size"]
#     # data = PARALLEL_COORDINATES_DATA_2
#     data = generate_data_for_parallel_coordinates_chart2()
#     samples_title = "Sample of %s entries" % sample_size
#     # Create the variable colored_groups
#     # First get the unique groups of give data
#     groups_list = list(set(map(lambda x: x[1], data)))
#     colored_groups = {}
#     for k, group in enumerate(groups_list):
#         colored_groups[group] = D3_PARALLEL_COORDINATES_COLORS[k]
#     # Greate a dict with keys the name of groups and value a list which represent the HSL color
#     return render(request, 'visualiser/parallel_coordinates_chart2.html', {
#         "data": data,
#         "y_axes": y_axes,
#         "title": title,
#         "about": about_title,
#         "about_text": about_text,
#         "groups": groups_title,
#         "samples": samples_title,
#         "samples_size": sample_size,
#         "colored_groups": colored_groups
#     })
