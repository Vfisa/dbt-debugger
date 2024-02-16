import json
import os

SEARCH_STR = 'o=[i("manifest","manifest.json"+t),i("catalog","catalog.json"+t)]'

with open(os.path.join(os.path.dirname(__file__),'dbt docs generate', 'index.html'), 'r') as f:
    content_index = f.read()
    
with open(os.path.join(os.path.dirname(__file__),'dbt docs generate', 'manifest.json'), 'r') as f:
    json_manifest = json.loads(f.read())

with open(os.path.join(os.path.dirname(__file__),'dbt docs generate', 'catalog.json'), 'r') as f:
    json_catalog = json.loads(f.read())
    
with open(os.path.join(os.path.dirname(__file__),'dbt docs generate', 'index2.html'), 'w') as f:
    new_str = "o=[{label: 'manifest', data: "+json.dumps(json_manifest)+"},{label: 'catalog', data: "+json.dumps(json_catalog)+"}]"
    new_content = content_index.replace(SEARCH_STR, new_str)
    f.write(new_content)

print("Docs file produced: index2.html")
