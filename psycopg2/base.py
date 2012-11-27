from django_pooled.base import PoolKey, PoolObject, PoolReleaser, CreationWrapper, pool
from django.db.backends.postgresql_psycopg2.base import DatabaseWrapper as DjangoDBWrapper
import psycopg2

class DatabaseWrapper(DjangoDBWrapper):
    def __init__(self, settings, *args, **kwargs):
        def on_connect_callback(conn, pool):
            tx_status = conn.connection.get_transaction_status()
            if tx_status != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                raise Exception("This connection isn't usable!")

        self.pool_opts = dict(ON_CONNECT=settings['OPTIONS'].pop('ON_CONNECT', on_connect_callback))
        super(DatabaseWrapper, self).__init__(settings, *args, **kwargs)
        # Pool releaser is needed to break the limitation that objects with
        # reference cycles + __del__ do not get garbage collected.
        self.pool_releaser = PoolReleaser()
        # Test databases can't be dropped if we don't close the connections for real
        # before doing the drop. CreationWrapper handles closing all connections just
        # before dropping the test DB.
        self.creation = CreationWrapper(self.creation)

    def get_new_connection(self, conn_params):
        key = PoolKey(self.alias, self.settings_dict)
        pooled = pool.acquire_connection(key)
        if pooled:
            self.pool_releaser.pool_object = pooled
            return pooled.connection
        conn = super(DatabaseWrapper, self).get_new_connection(conn_params)
        self.pool_releaser.pool_object = PoolObject(key, conn, self.pool_opts)
        pool.add_connection(self.pool_releaser.pool_object)
        return conn

    def close(self):
        self.validate_thread_sharing()
        if self.connection:
            self.connection = None
            self.pool_releaser.release()
