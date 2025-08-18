from boto3 import Session
import os
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

endpoint = ''
conn_string = 'wss://' + endpoint + ':8182/gremlin'

default_region = 'us-east-1'
service = 'neptune-db'
credentials = Session().get_credentials()
if credentials is None:
    raise Exception("No AWS credentials found")
creds = credentials.get_frozen_credentials()
# region set inside config profile or via AWS_DEFAULT_REGION environment variable will be loaded
region = Session().region_name if Session().region_name else default_region

request = AWSRequest(method='GET', url=conn_string, data=None)
SigV4Auth(creds, service, region).add_auth(request)

rc = DriverRemoteConnection(conn_string, 'g', headers=request.headers.items())

g = traversal().withRemote(rc)

print(g.inject(1).toList())
rc.close()