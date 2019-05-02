"""
globey.common.redis_tools
"""

import ast
import json
import simplejson

import redis
from redis.exceptions import ResponseError
from past.builtins import basestring
from cocore.Logger import Logger

LOG = Logger()


class RedisInteraction(object):
    def __init__(self, host, port, db, decode_responses=True):
        """Instantiate a Redis interaction manager.

        decode_responses mode is explicitly set to a boolean value in case a
        string is provided.

        :param host: Redis host
        :param port: Redis port
        :param db: Redis database number
        :param decode_responses: Whether or not to decode responses from Redis
        """
        if not host or not port or not db or decode_responses is None:
            raise RuntimeError("%s request all __init__ arguments" % __name__)

        self.host = host
        self.port = port
        self.db = db

        # Handle string decode response which should be boolean
        if isinstance(decode_responses, basestring) and decode_responses.lower() in ("true", "t"):
            self.decode_responses = True
        else:
            self.decode_responses = False

        self.conn = None

    def connect(self):
        """Create a connection to Redis database.

        Redis connection is implemented with Redis Connection Pools which are
        stored for each database in a dictionary.
        """
        db_pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=self.decode_responses
            )

        self.conn = redis.Redis(connection_pool=db_pool)

        return self

    def fetch_by_key_name(self, key_name, field=None):
        """Retrieve a redis hash_key record by key_name and field if provided.

        Fields are optional. If a field is not provided, all fields are
        returned.

        :param key_name: Redis cache hash_key
        :param field: Optional field
        :return: Result from Redis cache or Log error
        """
        try:
            if field:
                # LOG.l('Fetching category: {} for {}'.format(field, key_name))
                response = self.conn.hmget(key_name, field)
                return response[0].decode('utf-8')
            else:
                # LOG.l('Fetching all categories for {}'.format(key_name))
                result = self.conn.hgetall(key_name)

            return result
        except Exception as e:
            if field:
                LOG.l('Error fetching category: {} for {}: {}'.format(
                    field, key_name, e))
            else:
                LOG.l('Error fetching {}: {}'.format(key_name, e))

    def set_key(self, key_name, field):
        """Set a redis hash_key record.

        :param key_name: Redis cache hash_key
        :param field: Required field
        :return: Result from Redis cache or Log error
        """
        try:
            self.conn.hmset(key_name, field)
        except Exception as e:
            LOG.l('Error: {}'.format(e))