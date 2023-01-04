#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from datetime import datetime, timedelta
from typing import List

import requests
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams import IncrementalMixin
from .lane_handler import parse_input_list, get_lanes


# Basic full refresh stream
class TransporeonInsightsStream(HttpStream, ABC):
    url_base = "https://insights.transporeon.com/v1/"

    def __init__(self, config: Mapping[str, Any], authenticator, **kwargs):
        super().__init__(authenticator=authenticator)
        self.bearer_token = config['bearer_token']
        self.frequency = config['frequency']
        self.from_loading_start_date = config['from_loading_start_date']
        self.lanes_lvl2 = config['lanes_lvl2']
        self.lanes = parse_input_list(config['lanes']['lane']) if type(config['lanes']['lane']) is not bool and not config['lanes_lvl2'] \
            else get_lanes(config, self.metric)

    @property
    @abstractmethod
    def metric(self) -> str:
        """
        :return: metric to be queried
        """

    def pop_lane_from_list(self) -> dict:
        lane = self.lanes.pop()
        return {
            'from_lvl1': lane[0].value(),
            'to_lvl1': lane[1].value(),
            'from_lvl2': lane[2].value(),
            'to_lvl2': lane[3].value()
        }

    # toDo check if this is the optimal way to handle lists
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if bool(self.lanes):
            lane = self.pop_lane_from_list()
            return {'frequency': self.frequency,
                    'from_time': self.from_loading_start_date,
                    'to_time': datetime.today().isoformat(),
                    } | lane
        else:
            return None

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"/metrics/{self.metric}/lane"

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        lane = self.pop_lane_from_list()
        return {'frequency': self.frequency,
                'from_time': self.from_loading_start_date,
                'to_time': datetime.today().isoformat(),
                } | lane

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        values = {k: v for k, v in data.items() if k != "timeseries"}
        ts_data = data.get("timeseries", [])
        yield [[values, {self.metric: e[1]}, {"date": e[0]}] for e in ts_data]


class IncrementalTransporeonInsightsStream(TransporeonInsightsStream, IncrementalMixin, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator, **kwargs):
        super().__init__(config, authenticator, **kwargs)
        self._cursor_value = None

    state_checkpoint_interval = None
    primary_key = None
    date_format = '%Y-%m-%d'

    @property
    def cursor_field(self) -> str:
        return "date"

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime(self.date_format)}
        else:
            return {self.cursor_field: self.from_loading_start_date.strftime(self.date_format)}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], self.date_format)

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self._cursor_value:
                latest_record_date = datetime.strptime(record[self.cursor_field], self.date_format)
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record

    #toDo rework
    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({self.cursor_field: start_date.strftime(self.date_format)})
            start_date += timedelta(days=30)
        return dates

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) ->\
            Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field],
                                       self.date_format) if stream_state and self.cursor_field in stream_state else self.from_loading_start_date
        return self._chunk_date_range(start_date)


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
