from cassandra.cluster import Cluster
from collections import defaultdict
import logging
import pickle

global_id = '0c539200-605c-11e6-9ddc-df53cc771bed'


def logging_setup():
    local_log = logging.getLogger()
    local_log.setLevel('DEBUG')
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    local_log.addHandler(handler)
    return local_log


def create_session():
    cluster_local = Cluster(contact_points=['10.11.1.131', '10.11.1.133'], protocol_version=3, executor_threads=10)
    return cluster_local, cluster_local.connect()


def search():
    results = session.execute(query='select * from woow_backend.accounts')

    count = 0
    for row in results:
        parent_id = str(row.parent_id)
        id = str(row.id)
        if id != parent_id and parent_id == global_id:
            log.info('%s %s %s', count, row.id, row.parent_id)
            count += 1

    cluster.shutdown()


log = logging_setup()

cluster, session = create_session()
search()
