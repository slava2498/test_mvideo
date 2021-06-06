import json

from config import DATA_DIR, DATA_FILE

result = []
with open('{}/{}'.format(DATA_DIR, DATA_FILE), 'r') as file:
    result = json.loads(file.read())

class DataItem(object):
    __slots__ = 'data'
    def __init__(self, obj):
        self.data = obj

item = DataItem(result)
del result

def get_product(slug, recom=0):
    req = {}
    try:
        req = {'data': item.data[slug]} if not recom else {'data': item.data[slug][str(recom)]}
    except Exception as e:
        print(e)
        req = {'data': []}
    finally:
        return json.loads(json.dumps(req))