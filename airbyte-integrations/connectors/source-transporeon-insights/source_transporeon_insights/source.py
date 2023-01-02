#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import Tuple, List

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .streams import *

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


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
                ]
