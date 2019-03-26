import time
from systemlink.tagclient import TagClient
from systemlink.tagclient import messages


# minion_id is a unique identifier for every system that should be prepended to any tag path to avoid collisions
file = open("C:\\ProgramData\\National Instruments\\salt\\conf\\minion_id","r")
minion_id = file.read()

update = 5.14

with TagClient(service_name='TagGenerator') as tag_client:
    tag_path = minion_id + ".foo" 
    tag_type = "DOUBLE" 
    tag_properties = {"nitagRetention":"COUNT"}
    tag_client.create_tag(tag_path, tag_type,tag_properties)
    tag_client.update_tag(tag_path, tag_type, update)
