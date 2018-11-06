from systemlink.tagclient import TagClient
from systemlink.tagclient import messages

# minion_id is a unique identifier for every system that should be prepended to any tag path to avoid collisions
file = open("C:\\ProgramData\\National Instruments\\salt\\conf\\minion_id","r")
minion_id = file.read()

with TagClient(service_name='TagGenerator') as tag_client:
    tags = [{
        'path': minion_id + '.operator',
        'type': 'STRING',
        'value': 'Josh'
    },{
        'path': minion_id + '.test',
        'type': 'STRING',
        'value': 'Mobile Device Test'
    },{
        'path': minion_id + '.status',
        'type': 'STRING',
        'value': 'Running'
    }]
    
    tag_properties = {"nitagRetention":"COUNT"}
    tag_keywords = ""
    tag_collect_aggregates = True

    for tag in tags:
        print (tag)
        tag_client.create_tag(tag['path'], tag['type'], tag_properties, tag_keywords, tag_collect_aggregates)

    tag_client.update_tags(tags)
