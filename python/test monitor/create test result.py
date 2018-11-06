#Imports
import datetime
import socket
from io import StringIO

from systemlink.testmonclient import TestMonitorClient, testmon_messages
from systemlink.fileingestionclient import FileIngestionClient, fileingestion_messages

# Get Minion ID
filename = 'C:/ProgramData/National Instruments/salt/conf/minion_id'
minion_id = ''
with open(filename) as f:
    minion_id = f.read()
print (minion_id)

# Create a Test Monitor Client instance
with TestMonitorClient(service_name='TestMonitorClient') as testmonclient:
    start_time = datetime.datetime.now()

    # Create test results
    results = [{'status':'RUNNING',
                'startedAt': start_time.isoformat(),
                'programName': 'Example Test Result',
                'systemId': minion_id,
                'hostName': socket.gethostname(),
                'operator': 'Josh',
                'serialNumber': 'ABC-123-456',
                'totalTimeInSeconds': 0,
                'keywords': ['keyword1', 'keyword2'],
                'properties': {'key1': 'value1'},
                'fileIds': []
            }]

    result_response = testmonclient.create_results(results)
    #print (result_response.results[0].to_dict())

    # Create test steps
    steps = [{
            'name': 'MainSequence Callback',
            'stepType': 'SequenceCall',
            'stepId': '',
            'parentId': None,
            'resultId': result_response.results[0].id,
            'status': {'statusType':'PASSED', 'statusName':'Passed'},
            'totalTimeInSeconds': 5,
            'startedAt': datetime.datetime.now().isoformat(),
            'dataModel': 'TestStand',
            'data': {
                "text": None,
                "parameters": []
            },
            'children': [{
                'name': 'Video Test',
                'stepType': 'NumericLimit',
                'stepId': None,
                'parentId': None,
                'resultId': result_response.results[0].id,
                'status': {'statusType':'FAILED', 'statusName':'Failed'},
                'totalTimeInSeconds': 29.9,
                'startedAt': datetime.datetime.now().isoformat(),
                'dataModel': 'TestStand',
                'data': {
                    "text": None,
                    "parameters": [{
                        "name": "Video Test",
                        "status": "Failed",
                        "measurement": "14",
                        "units": None,
                        "nominalValue": None,
                        "lowLimit": "0",
                        "highLimit": "10",
                        "comparisonType": "GTLT"
                    }]
                },
                'children': None
            },
            {
                'name': 'ROM Test',
                'stepType': 'PassFailTest',
                'stepId': None,
                'parentId': None,
                'resultId': result_response.results[0].id,
                'status': {'statusType':'PASSED', 'statusName':'Passed'},
                'totalTimeInSeconds': 8.7,
                'startedAt': datetime.datetime.now().isoformat(),
                'dataModel': 'TestStand',
                "data": {
                    "text": None,
                    "parameters": [{
                        "name": "ROM Test",
                        "status": "Passed",
                        "measurement": None,
                        "units": None,
                        "nominalValue": None,
                        "lowLimit": None,
                        "highLimit": None,
                        "comparisonType": None
                    }]
                },
                'children': None
            }]
            }
            ]

    steps_response = testmonclient.create_steps(steps)
    #print (steps_response.steps[0].to_dict())

    #Update the result to completed state
    result_update = result_response.results[0].to_dict()
    total_time = (datetime.datetime.now() - start_time).total_seconds()
    print (total_time)
    result_update['totalTimeInSeconds'] = total_time
    result_updates = [result_update]

    update_response = testmonclient.update_results(result_updates, determine_status_from_steps=True)
    #print (update_response.results[0].to_dict())


