"""Redis cache backend."""

import pickle
import random
import re
import urllib

import redis

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.core.exceptions import ImproperlyConfigured
from django.utils.functional import cached_property
from django.utils.module_loading import import_string


class BaseCacheSerializer:
    def dumps(self):
        raise NotImplementedError

    def loads(self):
        raise NotImplementedError


class PickleCacheSerializer(BaseCacheSerializer):
    def __init__(self, options):
        self._pickle_version = -1

        if 'PICKLE_VERSION' in options:
            try:
                self._pickle_version = int(options['PICKLE_VERSION'])
            except (ValueError, TypeError):
                raise ImproperlyConfigured('must be an integer')

    def dumps(self, value):
        return pickle.dumps(value, self._pickle_version)

    def loads(self, value):
        return pickle.loads(value)


class RedisCacheClient:
    def __init__(self, servers, options):
        self._servers = servers
        self._pools = [None] * len(servers)
        self._options = options

        self._pool_class = redis.ConnectionPool
        self._client = redis.Redis
        self._serializer = PickleCacheSerializer(self._options)

        self._client_kwargs = {}

        username = self._options.get('USERNAME', None)
        if username:
            self._client_kwargs['username'] = username

        password = self._options.get('PASSWORD', None)
        if password:
            self._client_kwargs['password'] = password

        parser = self._options.get('PARSER', redis.connection.PythonParser)
        if parser:
            if isinstance(parser, str):
                parser = import_string(parser)

            self._client_kwargs['parser_class'] = parser

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

        return kwargs

    def _get_connection_pool(self, write):
        index = self._get_connection_pool_index(write)

        params = self._parse_url(self._servers[index])
        params.update(self._client_kwargs)

        if self._pools[index] is None:
            if self._servers[index].startswith('unix'):
                self._pools[index] = self._pool_class(
                    connection_class=redis.UnixDomainSocketConnection,
                    path=self._servers[index][5:],
                    **self._client_kwargs
                )
            else:
                self._pools[index] = self._pool_class(**params)

        return self._pools[index]

    def get_client(self, key='', write=False):
        pool = self._get_connection_pool(write)
        return self._client(connection_pool=pool)

    def get(self, key, default):
        client = self.get_client(key)
        value = client.get(key)
        # If key does not exists, return None
        if value is None:
            return default
        else:
            return self._serializer.loads(value)

    def add(self, key, value, timeout):
        client = self.get_client(key)
        return bool(client.set(key, self._serializer.dumps(value), ex=timeout, nx=True))

    def set(self, key, value, timeout):
        client = self.get_client(key, write=True)
        client.set(key, self._serializer.dumps(value), ex=timeout)

    def touch(self, key, timeout):
        client = self.get_client(key, write=True)
        if timeout is None:
            return client.persist(key)
        else:
            return client.expire(key, timeout)

    def delete(self, key):
        client = self.get_client(key, write=True)
        return bool(client.delete(key))

    def clear(self):
        client = self.get_client(None, write=True)
        return client.flushall()

    def get_many(self, keys):
        client = self.get_client(None)
        ret = client.mget(keys)
        return {
            k: self._serializer.loads(v) for k, v in zip(keys, ret) if v is not None
        }

    def set_many(self, data, timeout):
        client = self.get_client(None)
        serialized_data = {k: self._serializer.dumps(v) for k, v in data.items()}

        client.mset(serialized_data)

        for k, _ in data.items():
            # Setting timeout for each as redis-py does not support timeout with mset
            if timeout is None:
                client.persist(k)
            else:
                client.expire(k, timeout)


class RedisCache(BaseCache):
    def __init__(self, server, params):
        super().__init__(params)
        if isinstance(server, str):
            self._servers = re.split('[;,]', server)
        else:
            self._servers = server

        self._options = params.get('OPTIONS') or {}
        self._class = self._options.get('CLIENT', RedisCacheClient)

        if isinstance(self._class, str):
            self._class = import_string(self._class)

    def get_backend_timeout(self, timeout=DEFAULT_TIMEOUT):
        # Needs rethinking

        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout
        elif timeout is None:
            # Redis-py accepts None as timeout for as persistant key
            return timeout
        elif timeout <= 0:
            # Need to fix this
            timeout = 1

        return int(timeout)

    @cached_property
    def _cache(self):
        return self._class(self._servers, self._options)

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return self._cache.get(key, default)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        self._cache.set(key, value, self.get_backend_timeout(timeout))

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return bool(self._cache.add(key, value, self.get_backend_timeout(timeout)))

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return bool(self._cache.touch(key, self.get_backend_timeout(timeout)))

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return bool(self._cache.delete(key))

    def clear(self):
        return bool(self._cache.clear())

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
