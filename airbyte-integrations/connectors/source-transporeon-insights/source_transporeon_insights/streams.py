#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from datetime import datetime, timedelta
from typing import List

import requests
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class TransporeonInsightsStream(HttpStream, ABC):
    url_base = "https://insights.transporeon.com/v1/"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.bearer_token = config['bearer_token']
        self.frequency = config['frequency']
        self.from_loading_start_date = config['from_loading_start_date']
        self.lanes = self.parse_string(config["lanes"]) \
            if config["lanes"] != "ALL" and config['lanes_lvl2'] else self.get_lanes(config, self.metric)
        self.lanes_lvl2 = config['lanes_lvl2']

    @property
    @abstractmethod
    def metric(self) -> str:
        """
        :return: metric to be queried
        """

    def pop_lane_from_list(self):
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

    def parse_string(self, lanes: str):
        lane = lanes.split(", ")
        return [{"from_lvl1": a, "to_lvl1": b} for a, b in [entries.split("-") for entries in lane]]

    def filter_lanes(self, lanes, lanes_lvl2):
        if lanes_lvl2:
            return lanes
        else:
            return [e for e in lanes if not any(v != "ALL" and (k == "from_lvl2" or k == "to_lvl2") for k, v in e.items())]

    def match_lanes(self, all_lanes: list, parsed_lanes: list):
        return [{**d, "from_lvl2": e["from_lvl2"], "to_lvl2": e["to_lvl2"]}
                for d in parsed_lanes for e in all_lanes if d["from_lvl1"] == e["from_lvl1"] and d["to_lvl1"] == e["to_lvl1"]]

    def get_lanes(self, config: Mapping[str, Any], metric: str):
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        url = f"https://insights.transporeon.com/v1/metrics/{metric}"
        try:
            request = requests.get(url, headers=headers)
            available_lanes = request.json()['lanes']
        except requests.exceptions.RequestException:
            raise

        available_lanes = self.filter_lanes(available_lanes, self.lanes_lvl2)
        if config["lanes"] == "ALL":
            return available_lanes
        else:
            parsed_lanes = self.parse_string(config["lanes"])
            return self.match_lanes(available_lanes, parsed_lanes)

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
        # toDO change this to enable split entries
        yield response.json()


# Basic incremental stream
class IncrementalTransporeonInsightsStream(TransporeonInsightsStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        return "from_time"

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
        else:
            return {self.cursor_field: self.start_date.strftime('%Y-%m-%d')}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], '%Y-%m-%d')

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self._cursor_value:
                latest_record_date = datetime.strptime(record[self.cursor_field], '%Y-%m-%d')
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({self.cursor_field: start_date.strftime('%Y-%m-%d')})
            start_date += timedelta(days=1)
        return dates

    # toDo slice after every lane
    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[
        Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field],
                                       '%Y-%m-%d') if stream_state and self.cursor_field in stream_state else self.start_date
        return self._chunk_date_range(start_date)

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class CapacityIndex(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "capacity-index"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")


class ContractPrice(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "contract-price"


class ContractPriceIndex(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "contract-price-index"


class ContractRejectionrate(IncrementalTransporeonInsightsStream):

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
        return "const-index-factors"


class DieselPrice(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "diesel-price"


class SpotOfferIndex(IncrementalTransporeonInsightsStream):

    @property
    def metric(self) -> str:
        return "spot-offer-index"


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
