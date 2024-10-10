from datetime import timedelta

# needed for Couchbase cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions


class CouchbaseSDK(object):
    def __init__(self, ip, username, password, bucket_name=None):
        auth = PasswordAuthenticator(username, password)
        self.cluster = Cluster(f'couchbase://{ip}', ClusterOptions(auth))

        self.bucket = self.collection = None
        if bucket_name:
            self.bucket = self.cluster.bucket(bucket_name)

    def close(self):
        self.cluster.close()

    def select_bucket(self, bucket_name):
        self.bucket = self.cluster.bucket(bucket_name)

    def select_collection(self, scope_name, collection_name):
        self.collection = self.bucket.scope(scope_name).collection(collection_name)

    def create(self, doc_key, document):
        return self.collection.insert(doc_key, document)

    def update(self, doc_key, document):
        return self.collection.upsert(doc_key, document)

    def read(self, doc_key):
        return self.collection.get(doc_key)

    def delete(self, doc_key):
        return self.collection.remove(doc_key)

    def query(self, statement):
        return self.cluster.query(statement)
