#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from datetime import datetime
from functools import cached_property

import requests
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional, List

from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams import IncrementalMixin
from .lane_handler import parse_input_list, get_lanes, calculate_request_slices, get_lane_from_list


class TransporeonInsightsStream(HttpStream, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator, **kwargs):
        super().__init__(authenticator=authenticator)
        self.bearer_token = config['bearer_token']
        self.frequency = config['frequency']
        self.parsed_from_date = config['from_loading_start_date']
        self.lanes_lvl2 = config['lanes_lvl2']

        self.lanes = parse_input_list(config['lanes']['lane_list']) if config['lanes']['lane'] == 'SPECIFIC' and not config['lanes_lvl2'] \
            else get_lanes(config, self.metric)
        self.lane = None

    date_format = '%Y-%m-%d'

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

    list_position = 0

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        If the list position is less than the length of the list, get the lane from the list
        and increment the list position by 1. Otherwise, reset the list position to 0 and return None

        :param response: The response from the previous request
        :type response: requests.Response
        :return: A lane
        """
        if self.list_position < len(self.lanes):
            lane = get_lane_from_list(self.lanes, self.list_position)
            self.list_position += 1
            return lane
        else:
            self.list_position = 0
            self.lane = None
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
        if self.lane is None:
            # get first lane from list and move list position to one
            self.lane = get_lane_from_list(self.lanes, self.list_position)
            self.list_position += 1
        if next_page_token is not None:
            # get every other lane
            self.lane = next_page_token
        return {'frequency': self.frequency,
                } | self.lane | stream_slice

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        values = {k: v for k, v in data.items() if k != "timeseries"}
        ts_data = data.get("timeseries", [])
        for e in ts_data:
            yield values | {self.metric: e[1]} | {"date": e[0]}

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) \
            -> Iterable[Optional[Mapping[str, Any]]]:
        from_date = stream_state[self.cursor_field] if stream_state and \
                        self.cursor_field in stream_state else self.parsed_from_date
        slices = calculate_request_slices(from_date)
        return slices


class IncrementalTransporeonInsightsStream(TransporeonInsightsStream, IncrementalMixin, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator, **kwargs):
        super().__init__(config, authenticator, **kwargs)
        self._cursor_value = datetime.strptime(self.parsed_from_date, self.date_format).date()

    state_checkpoint_interval = None
    primary_key = None

    @property
    def cursor_field(self) -> str:
        return "date"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value.strftime(self.date_format)}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], self.date_format).date()

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
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

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) \
            -> Iterable[Optional[Mapping[str, Any]]]:
        # return empty dict since forecast does not neet from_time and to_time parameter
        return [{}]


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
