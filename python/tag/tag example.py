import time
from systemlink.tagclient import TagClient
from systemlink.tagclient import messages

update = 3.14

with TagClient(service_name='TagGenerator') as tag_client:
    tag_path = "PXI-ATDEMO-2.foo" 
    tag_type = "DOUBLE" 
    tag_properties = {"nitagRetention":"COUNT"}
    tag_client.create_tag(tag_path, tag_type,tag_properties)
    tag_client.update_tag(tag_path, tag_type, update)
