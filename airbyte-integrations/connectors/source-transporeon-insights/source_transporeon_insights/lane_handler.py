from typing import List, Mapping, Any
from datetime import datetime
import requests
from dateutil.relativedelta import relativedelta


def calculate_request_slices(from_date, date_list: list = None):
    """
    Recursive function that generates a list of 24-month intervals between the from_date and now

    :param from_date: The date from which you want to start fetching data
    :param date_list: list = None
    :type date_list: list
    :return: A list of tuples, each tuple containing a start date and an end date.
    """
    if date_list is None:
        date_list = []
    date_format = '%Y-%m-%d'
    start_date = datetime.strptime(from_date, date_format).date()
    today = datetime.today().date()
    delta = start_date + relativedelta(months=+24)

    end_date = delta if delta <= today else today
    date_list.append({'from_time': from_date, 'to_time': str(end_date)})

    if end_date < datetime.today().date():
        return calculate_request_slices(str(end_date + relativedelta(days=1)), date_list)
    else:
        return date_list


def check_date(start_date):
    """
    If the start date is within 24 months of today's date, return the start date plus 24 months

    :param start_date: The date you want to start the forecast from
    :return: The date 24 months from the start date.
    """
    if start_date + relativedelta(months=+24) >= datetime.today().date:
        return start_date + relativedelta(months=24)


def parse_input_list(lanes: str) -> List[dict]:
    """
    Takes a string of lanes, splits it into a list of lanes, and then splits each lane into a dictionary of from_lvl1 and to_lvl1

    :param lanes: The lanes that the user wants to use
    :type lanes: str
    :return: A list of dictionaries.
    """
    lane = lanes.split(", ")
    return [{"from_lvl1": a, "to_lvl1": b} for a, b in [entries.split("-") for entries in lane]]


def filter_lanes(lanes, lanes_lvl2):
    """
    If the user has selected a level 2 lane, return all lanes. Otherwise, filter entries, where lvl2 lanes are present

    :param lanes: a list of dictionaries, each dictionary representing a lane
    :param lanes_lvl2: boolean, whether to include lanes that are not on the main level of the map
    """
    if lanes_lvl2:
        return lanes
    else:
        return [e for e in lanes if not any((k == "from_lvl2" or k == "to_lvl2") and v != "ALL" for k, v in e.items())]


def match_lanes(all_lanes: list, parsed_lanes: list) -> List[dict]:
    """
    > For each lane in the parsed lanes, find the corresponding lane in the all lanes list and add the from_lvl2 and to_lvl2 attributes to
    the parsed lane

    :param all_lanes: list of dicts, each dict has the following keys:
    :type all_lanes: list
    :param parsed_lanes: list of dicts, each dict has the following keys:
    :type parsed_lanes: list
    :return: A list of dictionaries.
    """
    return [{**d, "from_lvl2": e["from_lvl2"], "to_lvl2": e["to_lvl2"]}
            for d in parsed_lanes for e in all_lanes if d["from_lvl1"] == e["from_lvl1"] and d["to_lvl1"] == e["to_lvl1"]]


def get_lanes(config: Mapping[str, Any], metric: str) -> List[dict]:
    headers = {"Authorization": f"Bearer {config['bearer_token']}"}
    url = f"https://insights.transporeon.com/v1/metrics/{metric}"
    try:
        request = requests.get(url, headers=headers)
        available_lanes = request.json()['lanes']
    except requests.exceptions.RequestException as ex:
        raise RuntimeError("Could not collect available lanes from API metrics, so sync can not be executed") from ex

    available_lanes = filter_lanes(available_lanes, config['lanes_lvl2'])
    if type(config["lanes"]["lane"]) is bool:
        return available_lanes
    else:
        parsed_lanes = parse_input_list(config["lanes"])
        return match_lanes(available_lanes, parsed_lanes)


def get_lane_from_list(lanes: list, position: int) -> dict:
    lane = lanes[position]
    lane_query_params = {}
    for key in ['from_lvl1', 'to_lvl1', 'from_lvl2', 'to_lvl2']:
        if key in lane:
            lane_query_params[key] = lane[key]
    return lane_query_params
