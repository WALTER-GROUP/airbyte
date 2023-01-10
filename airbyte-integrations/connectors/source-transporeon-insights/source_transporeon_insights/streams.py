#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from datetime import datetime
from functools import cached_property

import requests
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams import IncrementalMixin
from .lane_handler import parse_input_list, get_lanes, calculate_request_slices


class TransporeonInsightsStream(HttpStream, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator, **kwargs):
        super().__init__(authenticator=authenticator)
        self.bearer_token = config['bearer_token']
        self.frequency = config['frequency']
        self.parsed_from_date = config['from_loading_start_date']
        self.lanes_lvl2 = config['lanes_lvl2']
        # If there is a list of lanes parsed and there are no lanes_lvl2, then parse the input list. Otherwise, get the lanes.
        self.lanes = parse_input_list(config['lanes']['lane']) if type(config['lanes']['lane']) is not bool and not config['lanes_lvl2'] \
            else get_lanes(config, self.metric)

    @property
    @abstractmethod
    def metric(self) -> str:
        """
        :return: metric to be queried
        """

    @property
    def url_base(self) -> str:
        return "https://insights.transporeon.com/v1/"

    @cached_property
    def dates(self) -> list:
        return calculate_request_slices(self.parsed_from_date)

    date_position = 0
    lane = None

    def _pop_lane_from_list(self) -> dict:
        lane = self.lanes.pop()
        lane_query_params = {}
        for key in ['from_lvl1', 'to_lvl1', 'from_lvl2', 'to_lvl2']:
            if key in lane:
                lane_query_params[key] = lane[key]
        return lane_query_params

    def _get_next_date(self):
        date_pair = self.dates[self.date_position]
        date_params = {'from_time': date_pair[0], 'to_time': date_pair[1]}

        return date_params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        If there are lanes, and the date position is greater than the length of the dates list,
        then pop the lane from the list and set the date position to 0. Otherwise, get the next
        date and increment the date position by 1

        :param response: The response from the previous request
        :type response: requests.Response
        :return: A dictionary with the next date and lane.
        """
        if bool(self.lanes):
            if self.date_position > len(self.dates) - 1:
                self.lane = self._pop_lane_from_list()
                self.date_position = 0
                date_params = self._get_next_date()
            else:
                date_params = self._get_next_date()
                self.date_position += 1
            return date_params | self.lane
        else:
            return None

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"metrics/{self.metric}/lane"

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        if next_page_token is not None:
            params = next_page_token
        else:
            # initial lane + date combination for request
            self.lane = self._pop_lane_from_list()
            params = self.lane | self._get_next_date()
            self.date_position += 1
        return {'frequency': self.frequency,
                } | params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        values = {k: v for k, v in data.items() if k != "timeseries"}
        ts_data = data.get("timeseries", [])
        for e in ts_data:
            yield values | {self.metric: e[1]} | {"date": e[0]}


class IncrementalTransporeonInsightsStream(TransporeonInsightsStream, IncrementalMixin, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator, **kwargs):
        super().__init__(config, authenticator, **kwargs)
        self._cursor_value = None
        self.initial_state = None

    state_checkpoint_interval = None
    primary_key = None
    date_format = '%Y-%m-%d'

    @property
    def cursor_field(self) -> str:
        return "date"

    @property
    def dates(self) -> list:
        if self.initial_state:
            return calculate_request_slices(self.initial_state)
        else:
            return calculate_request_slices(self.parsed_from_date)

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime(self.date_format)}
        else:
            return {self.cursor_field: str(datetime.today().date())}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        initial_state = datetime.strptime(value[self.cursor_field], self.date_format).date()
        self.initial_state = str(initial_state)
        self._cursor_value = initial_state

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self._cursor_value:
                latest_record_date = datetime.strptime(record[self.cursor_field], self.date_format).date()
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record


class CapacityIndex(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "capacity-index"


class ContractPrice(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "contract-price"


class ContractPriceIndex(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "contract-price-index"


class ContractRejectionRate(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "contract-rejection-rate"


class CostIndex(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "cost-index"


class CostIndexFactors(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "cost-index-factors"


class DieselPrice(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "diesel-price"


class SpotOfferIndex(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "spot-offers-index"


class SpotPrice(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "spot-price"


class SpotPriceIndex(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "spot-price-index"


class TotalPriceIndex(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "total-price-index"


class TransporeonForecast(IncrementalTransporeonInsightsStream, ABC):

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"metrics/{self.metric}/predictions"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if bool(self.lanes):
            return self._pop_lane_from_list()
        else:
            return None

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        if next_page_token is not None:
            params = next_page_token
        else:
            self.lane = self._pop_lane_from_list()
            params = self.lane
        return {'frequency': self.frequency,
                } | params


class SpotPriceForecast(TransporeonForecast):

    @property
    def metric(self) -> str:
        return "spot-price"


class CostIndexForecast(TransporeonForecast):

    @property
    def metric(self) -> str:
        return "cost-index"


class CostIndexFactorsForecast(TransporeonForecast):

    @property
    def metric(self) -> str:
        return "cost-index-factors"
