from utils.processing_libs import *
from utils.settings import configs

db_broker = DBkafka(
    topic = configs['KAFKA__TOPIC'],
    log_path = configs['LOG__PATH'],
    headers = {'version':'1.1'},
    tz = configs['timezone'],
    host = configs['KAFKA__HOST'], 
    port = configs['KAFKA__PORT'],
    value_serializer = lambda x: json.dumps(x).encode('utf-8'),
)

apistream = streamAPI(
    url = 'https://stream.wikimedia.org/v2/stream/recentchange',
    log_path = configs['LOG__PATH'],
    pattern_msg = re.compile('(?P<name>[^:]*):?( ?(?P<value>.*))?'),
    db_broker = db_broker,
)


if __name__ == "__main__":
    apistream.kind = 'wiki'
    apistream.start_streaming()
  