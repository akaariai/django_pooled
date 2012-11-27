import copy
import threading

from django.db.utils import load_backend
from django.utils import six

class PoolKey(object):
    def __init__(self, alias, settings):
        self.key = (alias, settings['NAME'], settings['USER'])

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.key == other.key

    def __hash__(self):
        return hash(self.key)

    def __str__(self):
        return str(self.key)
    __repr__ = __str__

class PoolObject(object):
    def __init__(self, key, connection, pool_opts):
        self.on_connect = pool_opts['ON_CONNECT']
        self.key = key
        self.connection = connection
        self.in_use = True
        self.abandoned = False

class Pool(object):
    pools = {}
    lock = threading.Lock()

    def _status(self):
        for key, conns in self.pools.items():
            if len([c for c in conns if c.in_use]) >= 0:
                print(key, len([c for c in conns if c.in_use]))
            for conn in conns:
                print(conn, conn.in_use, conn.abandoned)

    def acquire_connection(self, key):
        conn = None
        while conn is None:
            with self.lock:
                conn = self.acquire_connection_inner(key)
                if conn is None:
                    # No available connections
                    return None
                try:
                    self.verify_connection(conn)
                except:
                    conn.abandoned = True
                    self.pools[key].remove(conn)
                    conn = None
                    raise
        return conn

    def acquire_connection_inner(self, key):
        try:
            for conn in self.pools[key]:
                if conn.in_use:
                    continue
                conn.in_use = True
                return conn
        except KeyError:
            self.pools[key] = []
            return

    def add_connection(self, conn):
        with self.lock:
            self.pools[conn.key].append(conn)

    def verify_connection(self, conn):
        if conn.on_connect:
            if isinstance(conn.on_connect, six.string_types):
                cursor = conn.connection.cursor()
                cursor.execute(conn.on_connect)
                cursor.fetchone()
            else:
                conn.on_connect(conn, self)

    def release_connection(self, conn):
        if conn.abandoned:
            assert not conn.in_use
            return
        try:
            conn.connection.rollback()
        except:
            # Uqly, but there is no common exception class for different
            # DB adapters. At least logging would be good here...
            pass
        conn.in_use = False

    def close_all(self):
        with self.lock:
            for conns in self.pools.values():
                for conn in conns[:]:
                    conn.connection.close()
                    conns.remove(conn)
# Global pool.
pool = Pool()

class PoolReleaser(object):
    """
    Somewhat ugly situation: we must release the connection on garbage
    collection back to the pool. However, if there is a reference cycle then
    __del__ is not called for the object (actually, the object is not ever
    GCed). So, this class is there only to break the reference cycle so that
    GC will work, yet __del__ is called. This is ugly but seems to work. Better
    ideas are very much welcome :)
    """
    def __init__(self):
        self.pool_object = None
        # Forbid GC of the pool before connections are cleaned.
        self.pool = pool

    def release(self):
        if self.pool_object:
            self.pool.release_connection(self.pool_object)
            self.pool_object = None

    def __del__(self):
        # Trick: when the DBWrapper below is carbage collected, I will get
        # called. Hopefully.
        if self.pool_object:
            self.release()

class CreationWrapper(object):
    """
    Need to wrap the real creation object so that it is possible to destroy
    the test database after tests.
    """
    def __init__(self, wrapped_creation):
        self.wrapped_creation = wrapped_creation

    def __getattr__(self, attr):
        return getattr(self.wrapped_creation, attr)

    def destroy_test_db(self, *args, **kwargs):
        # Destroying a db isn't possible if there are still connections open
        # to the test db. So, close all pool connection before trying to
        # destroy the DB.
        pool.close_all()
        self.wrapped_creation.destroy_test_db(*args, **kwargs)

# wrapped engine -> dynamic wrapper
dyn_wrap_cache = {}
def DatabaseWrapper(settings, *args, **kwargs):
    """
    This is just evil - the caller thinks he is instantiating
    base.DatabaseWrapper. But not true! He is actually calling this
    factory method - we are pretending to be a class here. Hopefully nobody
    is doing isinstance checks against DatabaseWrapper... :)

    Returns a dynamically created wrapper for the settings.wrapped connection.
    """
    settings = copy.deepcopy(settings)
    settings['ENGINE'] = wraps = settings['OPTIONS'].pop('WRAPS', None)
    if wraps is None:
        raise RuntimeError('You must define OPTIONS["WRAPS"] in settings '
                           'for alias %s.' % args[0])
    pool_opts = dict(ON_CONNECT=settings['OPTIONS'].pop('ON_CONNECT', 'select 1'))

    if wraps in dyn_wrap_cache:
        dynwrap = dyn_wrap_cache[wraps]
    else:
        dbwrapper = load_backend(wraps).DatabaseWrapper
        # Methods we are going to add to the dynamically created wrapper.
        def get_new_connection(self, conn_params):
            key = PoolKey(self.alias, self.settings_dict)
            pooled = pool.acquire_connection(key)
            if pooled:
                self.pool_releaser.pool_object = pooled
                return pooled.connection
            conn = super(self.own_class, self).get_new_connection(conn_params)
            self.pool_releaser.pool_object = PoolObject(key, conn,
                                                        self.pool_opts)
            pool.add_connection(self.pool_releaser.pool_object)
            return conn

        def close(self):
            self.validate_thread_sharing()
            if self.connection:
                self.connection = None
                self.pool_releaser.release()

        dynwrap = type('Pooled' + dbwrapper.__name__,
                      (dbwrapper,), {'get_new_connection': get_new_connection,
                                     'close': close,
                                     'pool_opts': pool_opts})
        dynwrap.own_class = dynwrap
        dyn_wrap_cache[wraps] = dynwrap
    conn = dynwrap(settings, *args, **kwargs)
    conn.pool_releaser = PoolReleaser()
    conn.creation = CreationWrapper(conn.creation)
    return conn
