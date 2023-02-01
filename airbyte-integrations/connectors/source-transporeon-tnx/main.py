#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_transporeon_tnx import SourceTransporeonTnx

if __name__ == "__main__":
    source = SourceTransporeonTnx()
    launch(source, sys.argv[1:])
