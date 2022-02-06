
import json
import logging
import sys

from streamz import Stream
from sources.mysql import MySQLSource

from caches import Cache
from sinks import S3Sink

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":

    conf_file_name = sys.argv[1]
    with open(conf_file_name) as f:
        conf = json.load(f)

    cache = Cache()
    source = MySQLSource(
        host=conf["host"],
        user=conf["user"],
        password=conf["password"],
        database=conf["database"],
        table=conf["table"],
        limit=100000,
        sleep=0,
        cache=cache,
    )
    sink = S3Sink(
        bucket=conf["bucket"],
        path=conf["path"],
    )

    stream = Stream()

    stream.sink(sink.run)

    source.run(stream)
