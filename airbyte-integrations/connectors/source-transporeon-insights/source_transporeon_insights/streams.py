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
from .lane_handler import parse_input_list, get_lanes, calculate_request_slices, pop_lane_from_list


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
        # load first lane from list
        self.lane = pop_lane_from_list(self.lanes)

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

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if bool(self.lanes):
            return pop_lane_from_list(self.lanes)
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
        return calculate_request_slices(from_date)


class IncrementalTransporeonInsightsStream(TransporeonInsightsStream, IncrementalMixin, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator, **kwargs):
        super().__init__(config, authenticator, **kwargs)
        self._cursor_value = None

    state_checkpoint_interval = None
    primary_key = None

    @property
    def cursor_field(self) -> str:
        return "date"

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime(self.date_format)}
        else:
            return {self.cursor_field: self.parsed_from_date}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], self.date_format).date()

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
