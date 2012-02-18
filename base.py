import threading

from django.db.utils import load_backend, DatabaseError

class PoolKey(object):
    def __init__(self, alias, settings):
        self.key = (alias, settings['NAME'])

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
        self.on_close = pool_opts['ON_CLOSE']
        self.key = key
        self.connection = connection
        self.in_use = True
        self.abandoned = False

class Pool(object):
    pools = {}
    lock = threading.Lock()

    def acquire_connection(self, key):
        conn = None
        while conn is None:
            with self.lock:
                conn = self._acquire_connection(key)
                if conn is None:
                    # No available connections
                    return None
            if not self.verify_connection(conn):
                with self.lock:
                    self.pools[key].remove(conn)
                conn = None
        return conn
    
    def _acquire_connection(self, key):
        # self.lock is assumed to be locked by this thread
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
        try:
            cursor = conn.connection.cursor()
            cursor.execute(conn.on_connect)
            cursor.fetchone()
            return True
        except:
            # log - return False
            raise

    def release_connection(self, conn):
        if conn.abandoned:
            return
        try:
            cursor = conn.connection.cursor()
            for command in conn.on_close:
                cursor.execute(command)
            conn.connection.rollback()
            conn.in_use = False
        except DatabaseError, e:
            with self.lock:
                self.pools[conn.key].remove(conn)
            # logger.log...
                
    def close_all(self):
        with self.lock:
            for conns in self.pools.values():
                for conn in conns[:]:
                    conn.connection.close()
                    conn.abandoned = True
                    conns.remove(conn)

pool = Pool() 

class PoolReleaser(object):
    def __init__(self):
        self.pool_object = None
        # Forbid GC of the pool before connections are cleaned.
        self.pool = pool

    def release(self):
        if self.pool_object:
             self.pool.release_connection(self.pool_object)

    def __del__(self):
        # Trick: when the DBWrapper below is carbage collected, I will get
        # called. Hopefully.
        self.release()

class WrappedCreation(object):
    def __init__(self, real_creation):
        self.real_creation = real_creation

    def __getattr__(self, attr):
        return getattr(self.real_creation, attr)
    
    def destroy_test_db(self, *args, **kwargs):
        pool.close_all()
        self.real_creation.destroy_test_db(*args, **kwargs)

def DatabaseWrapper(settings, *args, **kwargs):
    """
    This is just evil - the caller thinks he is instantiating base.DatabaseWrapper.
    But not true! He is actually calling this factory method.
    """
    settings = settings.copy()
    settings['ENGINE'] = wraps =  settings['OPTIONS']['WRAPS']
    dbwrapper = load_backend(wraps).DatabaseWrapper

    pool_opts = dict(ON_CLOSE=settings['OPTIONS']['ON_CLOSE'],
                     ON_CONNECT=settings['OPTIONS']['ON_CONNECT'])
    del settings['OPTIONS']['WRAPS']
    del settings['OPTIONS']['ON_CLOSE']
    del settings['OPTIONS']['ON_CONNECT']

    def _cursor(self):
        key = PoolKey(self.alias, self.settings_dict)
        if not self.connection:
            pooled = pool.acquire_connection(key)
            if pooled:
                self.connection = pooled.connection
                return super(self.__class__, self)._cursor()
            cursor = super(self.__class__, self)._cursor()
            self.pool_releaser.pool_object = PoolObject(key, self.connection, self.pool_opts) 
            pool.add_connection(self.pool_releaser.pool_object)
            return cursor
        else:
            assert key == self.pool_releaser.pool_object.key
            return super(self.__class__, self)._cursor()
    
    def close(self):
        self.connection = None
        self.pool_releaser.release()
    
    dynwrap = type('Pooled' + dbwrapper.__name__,
                  (dbwrapper,), {'_cursor': _cursor, 'close': close, 'pool_opts': pool_opts})
    conn = dynwrap(settings, *args, **kwargs)
    conn.pool_releaser = PoolReleaser()
    conn.creation = WrappedCreation(conn.creation)
    return conn
