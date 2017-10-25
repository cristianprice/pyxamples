from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
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


def count_hierarchy(usr_id, level, parent_hierarchy, hierarchy):
    """ Counts the level children """

    if level > 8:
        return

    hierarchy[level] += len(parent_hierarchy[usr_id])
    for child_id in parent_hierarchy[usr_id]:
        count_hierarchy(child_id, level + 1, parent_hierarchy, hierarchy)

    return hierarchy


def search():
    results = session.execute(SimpleStatement(query='select * from woow_backend.accounts', fetch_size=10000))
    count = 0

    for row in results:
        parent_id = str(row.parent_id)
        id = str(row.id)
        if (id != parent_id and (row.account_status == 'CREATED' or row.account_status == 'ENABLED')):
            log.info('%s %s %s', count, row.id, row.parent_id)
            parent_hierarchy[parent_id].add(id)
            count += 1

    cluster.shutdown()


def save(h_obj):
    pickle.dump(h_obj, open('data.bin', 'wb'))
    return


log = logging_setup()

try:
    parent_hierarchy = pickle.load(open('data.bin', 'rb'))
except FileNotFoundError:
    log.error('Could not find local file binary. Getting data from db.')
    parent_hierarchy = defaultdict(set)
    cluster, session = create_session()
    search()
    save(parent_hierarchy)

hierarchy = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0}
count_hierarchy(global_id, 1, parent_hierarchy, hierarchy)
log.info('%s', hierarchy)
