# -*- coding: utf-8 -*-
"""
Tests for the Asset Management Utilization client.
"""
# Import python libs
import logging
import time

# Import third party libs

# Import local libs
from systemlink.assetmgmtutilclient import AssetManagementUtilization

# Set up logging
LOGGER = logging.getLogger(__name__)


def start_utilization_with_all_assets():  # pylint: disable=invalid-name
    """
    Starts an utilization with all assets installed in the system.
    The test writes a file which contains the following utilization information:
    - Start Utilization
    - Utilization Heartbeat (2 second delay)
    - End Utilization (2 second delay after Utilization Heartbeat)
    """
    asset_names = None
    utilization_category = 'Test'
    user_name = 'my_user_name'
    task_name = 'my_task_name'
    with AssetManagementUtilization(
                asset_names,
                utilization_category,
                user_name,
                task_name) as amuclient:
        time.sleep(20)
        amuclient.update()
        time.sleep(20)


start_utilization_with_all_assets()
