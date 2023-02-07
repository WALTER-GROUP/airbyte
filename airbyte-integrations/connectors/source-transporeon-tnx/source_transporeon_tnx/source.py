#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import json
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.sources.streams import IncrementalMixin


def get_bearer_token(config: Mapping[str, Any]):
    url = "https://offers-transporeon.eu.auth0.com/oauth/token"
    header = {"Content-Type": "application/json"}
    # https://procurement-tribe.notion.site/Authentication-using-Transporeon-Offers-PROD-50664222949a4592aba7a8c153a866d2
    payload = {"client_id": "mQStpabsDqOrGUEbKkcBHAtHgjhsSRcE",
               "client_secret": config["client_secret"],
               "audience": "https://api.tnx.co.nz",
               "grant_type": "password",
               "username": config["username"],
               "password": config["password"]}
    payload = json.dumps(payload)
    request = requests.post(url, data=payload, headers=header)
    return request.json()['access_token']


# Basic full refresh stream
class TransporeonTnxStream(HttpStream, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator, **kwargs):
        super().__init__(authenticator=authenticator)
        self.x_tnx_org = config["x_tnx_org"]

    url_base = "https://api.tnx.co.nz/v2019.4/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        json_response = response.json()
        next_page = json_response["meta"]["continuation_key"] if "next" in json_response["meta"] else None
        if next_page:
            return {"continuation_key": next_page}

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        next_page_token = next_page_token or {}
        return {"size": "100",
                "sort_type": "time_created.desc",
                **next_page_token}

    def request_headers(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"accept": "application/json",
                "x-tnx-auth0-tenant": "offers-transporeon.eu.auth0.com",
                "x-tnx-org": self.x_tnx_org}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()["data"]
        items = data.get("items", [])

        for item in items:
            yield item


class IncrementalTransporeonInsightsStream(TransporeonTnxStream, IncrementalMixin, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator, **kwargs):
        super().__init__(config, authenticator, **kwargs)
        self._cursor_value = None
        self.offset = int(config.get("time_offset")) if config.get("time_offset") else 24

    state_checkpoint_interval = None
    primary_key = None
    date_format = "%Y-%m-%dT%H:%M:%SZ"

    @property
    def cursor_field(self) -> str:
        return "start_time_min"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: datetime.utcnow().strftime(self.date_format)}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], self.date_format)

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        next_page_token = next_page_token or {}
        start_time_min = {self.cursor_field: (self._cursor_value - relativedelta(hours=self.offset)).strftime(self.date_format)}\
            if self._cursor_value else {}

        return {
            "size": "100",
            "sort_type": "time_created.desc",
            **next_page_token,
            **start_time_min
        }


class Tenders(IncrementalTransporeonInsightsStream):

    primary_key = None

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "orders/tenders"


# Source
class SourceTransporeonTnx(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        token = get_bearer_token(config)
        url = "https://api.tnx.co.nz/v2019.4/orders/tenders?size=1&sort_type=time_created.desc"
        headers = {"accept": "application/json",
                   "x-tnx-auth0-tenant": "offers-transporeon.eu.auth0.com",
                   "x-tnx-org": config["x_tnx_org"],
                   "authorization": "Bearer " + token}

        try:
            request = requests.get(url, headers=headers)
            request.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        token = get_bearer_token(config)
        auth = TokenAuthenticator(token=token)
        return [Tenders(config, authenticator=auth)]
