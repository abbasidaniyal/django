"""Redis cache backend."""

import pickle
import random
import re
import urllib

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.utils.functional import cached_property
from django.utils.module_loading import import_string


class PickleSerializer:
    def __init__(self, protocol):
        self._protocol = pickle.HIGHEST_PROTOCOL if protocol is None else protocol

    def dumps(self, value):
        return pickle.dumps(value, self._protocol)

    def loads(self, value):
        return pickle.loads(value)


class RedisCacheClient:
    def __init__(
        self,
        servers,
        username=None,
        password=None,
        pool_class=None,
        parser_class=None,
        pickle_protocol=None,
    ):
        import redis
        self._lib = redis
        self._servers = servers
        self._pools = [None] * len(servers)

        self._pool_class = pool_class or self._lib.ConnectionPool
        self._client = self._lib.Redis
        self._serializer = PickleSerializer(pickle_protocol)

        self._client_kwargs = {}

        if parser_class is None:
            parser_class = self._lib.connection.PythonParser
        elif isinstance(parser_class, str):
            parser_class = import_string(parser_class)
        self._client_kwargs = {'parser_class': parser_class}

        if username is not None:
            self._client_kwargs['username'] = username
        if password is not None:
            self._client_kwargs['password'] = password

    def _get_connection_pool_index(self, write):
        if write or len(self._servers) == 1:
            return 0
        return random.randint(1, len(self._servers) - 1)

    def _parse_url(self, url):
        parsed_url = urllib.parse.urlparse(url)

        kwargs = {'host': parsed_url.hostname}
        if parsed_url.port:
            kwargs['port'] = parsed_url.port

        if parsed_url.username:
            kwargs['username'] = parsed_url.username

        if parsed_url.password:
            kwargs['password'] = parsed_url.password

        kwargs.update(urllib.parse.parse_qsl(parsed_url.query))

        return kwargs

    def _get_connection_pool(self, write):
        index = self._get_connection_pool_index(write)

        params = self._parse_url(self._servers[index])
        params.update(self._client_kwargs)

        if self._pools[index] is None:
            if self._servers[index].startswith('unix'):
                self._pools[index] = self._pool_class(
                    connection_class=self._lib.UnixDomainSocketConnection,
                    path=self._servers[index][5:],
                    **self._client_kwargs
                )
            else:
                self._pools[index] = self._pool_class(**params)

        return self._pools[index]

    def get_client(self, key=None, *, write=False):
        pool = self._get_connection_pool(write)
        return self._client(connection_pool=pool)

    def add(self, key, value, timeout):
        client = self.get_client(key, write=True)
        value = self._serializer.dumps(value)

        if timeout == 0:
            if ret := bool(client.set(key, value, nx=True)):
                client.delete(key)
            return ret
        else:
            return bool(client.set(key, value, ex=timeout, nx=True))

    def get(self, key, default):
        client = self.get_client(key)
        value = client.get(key)
        return default if value is None else self._serializer.loads(value)

    def set(self, key, value, timeout):
        client = self.get_client(key, write=True)
        value = self._serializer.dumps(value)
        if timeout == 0:
            client.delete(key)
        else:
            client.set(key, value, ex=timeout)

    def touch(self, key, timeout):
        client = self.get_client(key, write=True)
        if timeout is None:
            return bool(client.persist(key))
        else:
            return bool(client.expire(key, timeout))

    def delete(self, key):
        client = self.get_client(key, write=True)
        return bool(client.delete(key))

    def get_many(self, keys):
        client = self.get_client(None)
        ret = client.mget(keys)
        return {
            k: self._serializer.loads(v) for k, v in zip(keys, ret) if v is not None
        }

    def set_many(self, data, timeout):
        client = self.get_client(None, write=True)
        pipeline = client.pipeline()
        pipeline.mset({k: self._serializer.dumps(v) for k, v in data.items()})

        if timeout is not None:
            # Setting timeout for each key as redis-py does not support timeout
            # with mset
            for key in data:
                pipeline.expire(key, timeout)
        pipeline.execute()

    def clear(self):
        client = self.get_client(None, write=True)
        return bool(client.flushdb())


class RedisCache(BaseCache):
    def __init__(self, server, params):
        super().__init__(params)
        if isinstance(server, str):
            self._servers = re.split('[;,]', server)
        else:
            self._servers = server

        self._class = RedisCacheClient
        self._options = params.get('OPTIONS') or {}

    def get_backend_timeout(self, timeout=DEFAULT_TIMEOUT):
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout

        # The key will be made persistent if None used as a timeout.
        # Non-positive values will cause the key to be deleted.
        return None if timeout is None else max(0, int(timeout))

    @cached_property
    def _cache(self):
        return self._class(self._servers, **self._options)

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return bool(self._cache.add(key, value, self.get_backend_timeout(timeout)))

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return self._cache.get(key, default)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        self._cache.set(key, value, self.get_backend_timeout(timeout))

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return bool(self._cache.touch(key, self.get_backend_timeout(timeout)))

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return bool(self._cache.delete(key))

    def get_many(self, keys, version=None):
        key_map = {self.make_key(key, version=version): key for key in keys}
        for key in key_map:
            self.validate_key(key)
        ret = self._cache.get_many(key_map.keys())
        return {key_map[k]: v for k, v in ret.items()}

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        safe_data = {}
        original_keys = {}
        for key, value in data.items():
            safe_key = self.make_key(key, version=version)
            self.validate_key(safe_key)
            safe_data[safe_key] = value
            original_keys[safe_key] = key
        self._cache.set_many(safe_data, self.get_backend_timeout(timeout))
        return []

    def clear(self):
        return bool(self._cache.clear())
