#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import requests
from typing import Any, Tuple, Mapping, List

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .streams import CostIndex, CostIndexForecast, CostIndexFactors, CostIndexFactorsForecast, CapacityIndex, ContractPrice, \
    ContractPriceIndex, ContractRejectionRate, DieselPrice, SpotPrice, SpotPriceForecast, SpotPriceIndex, SpotOfferIndex, SpotOfferSpread, \
    TotalPriceIndex


# Source
class SourceTransporeonInsights(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        bearer_token = config["bearer_token"]
        headers = {"Authorization": f"Bearer {bearer_token}"}
        url = f"https://insights.transporeon.com/v1/metrics"

        try:
            request = requests.get(url, headers=headers)
            request.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["bearer_token"])
        return [CapacityIndex(authenticator=auth, config=config),
                ContractPrice(authenticator=auth, config=config),
                ContractPriceIndex(authenticator=auth, config=config),
                ContractRejectionRate(authenticator=auth, config=config),
                CostIndex(authenticator=auth, config=config),
                CostIndexFactors(authenticator=auth, config=config),
                DieselPrice(authenticator=auth, config=config),
                SpotOfferIndex(authenticator=auth, config=config),
                SpotPrice(authenticator=auth, config=config),
                SpotPriceIndex(authenticator=auth, config=config),
                TotalPriceIndex(authenticator=auth, config=config),
                SpotOfferSpread(authenticator=auth, config=config),
                SpotPriceForecast(authenticator=auth, config=config),
                CostIndexForecast(authenticator=auth, config=config),
                CostIndexFactorsForecast(authenticator=auth, config=config),
                ]
