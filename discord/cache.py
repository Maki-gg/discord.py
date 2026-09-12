"""
Maki fork addition: cache layer for discord.py.

This module is not part of upstream discord.py. It provides the in-memory
TTL containers, the optional Redis tier and the manager that glues them to
:class:`discord.state.ConnectionState`. The upstream files only carry a
handful of hook lines marked ``# Maki fork: cache layer``.

The MIT License (MIT)

Copyright (c) 2015-present Rapptz

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import time
from collections import OrderedDict
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from .channel import PartialMessageable, _guild_channel_factory
from .member import Member
from .message import Message
from .role import Role
from .threads import Thread

if TYPE_CHECKING:
    from .client import Client
    from .guild import Guild
    from .state import ConnectionState
    from .user import User

__all__ = (
    'CacheSettings',
    'RedisSettings',
)

_log = logging.getLogger(__name__)

K = TypeVar('K')
V = TypeVar('V')

# Module attribute so tests can monkeypatch the clock.
_now = time.monotonic
_MISSING: Any = object()
_BARRIER: Any = object()
_REDIS_RETRY_AFTER = 30.0
_SWEEP_YIELD_EVERY = 500


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


class RedisSettings:
    """Fork addition, not part of upstream discord.py.

    Settings for the optional Redis tier of :class:`CacheSettings`.
    Requires ``pip install "discord.py[redis]"``.

    Parameters
    -----------
    uri: Optional[Union[:class:`str`, Sequence[:class:`str`]]]
        One Redis URI or several. With ``cluster=True`` every URI is a startup
        node and the first one that answers is used to discover the cluster.
        Mutually exclusive with ``client``.
    client: Optional[Any]
        An existing ``redis.asyncio`` client (standalone, Sentinel ``master_for`` or
        cluster) or a zero-argument callable returning one, resolved when the client logs in.
        The library shares it and never closes it, so the bot can keep using the same
        connection pool in cogs. Mutually exclusive with ``uri``.
    cluster: :class:`bool`
        Connect with the Redis Cluster client when building from ``uri``. Defaults to ``True``.
    read_from_replicas: :class:`bool`
        Cluster only. Allow reads from replica nodes. Defaults to ``False``.
    max_connections: Optional[:class:`int`]
        Connection pool size per node. ``None`` keeps the redis-py default.
    guild_ttl: :class:`int`
        Seconds the guild base data stays in Redis after its last write. Defaults to one day.
    role_ttl: :class:`int`
        Same for the role collection. Defaults to one day.
    channel_ttl: :class:`int`
        Same for the channel collection. Defaults to one day.
    thread_ttl: :class:`int`
        Same for the thread collection. Defaults to one hour.
    emoji_ttl: :class:`int`
        Same for the emoji collection. Defaults to one day.
    sticker_ttl: :class:`int`
        Same for the sticker collection. Defaults to one day.
    member_ttl: :class:`int`
        Seconds a member entry stays in Redis. Only written when ``serve_fetches`` is on.
        Defaults to six hours.
    user_ttl: :class:`int`
        Seconds a user entry stays in Redis. Only written when ``serve_fetches`` is on.
        Defaults to six hours.
    message_ttl: Optional[:class:`int`]
        Seconds a message stays in Redis. ``None`` disables message mirroring. Defaults to ``None``.
    serve_fetches: :class:`bool`
        Mirror members and users so :func:`discord.cache.get_cached_member` and
        :func:`discord.cache.get_cached_user` can answer from Redis. Defaults to ``False``.
    queue_size: :class:`int`
        Bound on pending writes. Writes beyond it are dropped and logged. Defaults to ``10000``.
    batch_size: :class:`int`
        Maximum number of writes per pipeline. Defaults to ``256``.
    """

    __slots__ = (
        'uri',
        'client',
        'cluster',
        'read_from_replicas',
        'max_connections',
        'guild_ttl',
        'role_ttl',
        'channel_ttl',
        'thread_ttl',
        'emoji_ttl',
        'sticker_ttl',
        'member_ttl',
        'user_ttl',
        'message_ttl',
        'serve_fetches',
        'queue_size',
        'batch_size',
    )

    def __init__(
        self,
        uri: Optional[Union[str, Sequence[str]]] = None,
        *,
        client: Any = None,
        cluster: bool = True,
        read_from_replicas: bool = False,
        max_connections: Optional[int] = None,
        guild_ttl: int = 86400,
        role_ttl: int = 86400,
        channel_ttl: int = 86400,
        thread_ttl: int = 3600,
        emoji_ttl: int = 86400,
        sticker_ttl: int = 86400,
        member_ttl: int = 21600,
        user_ttl: int = 21600,
        message_ttl: Optional[int] = None,
        serve_fetches: bool = False,
        queue_size: int = 10_000,
        batch_size: int = 256,
    ) -> None:
        if (uri is None) == (client is None):
            raise TypeError('RedisSettings requires exactly one of uri or client')
        uris: List[str] = [] if uri is None else [uri] if isinstance(uri, str) else list(uri)
        if uri is not None and not uris:
            raise ValueError('RedisSettings requires at least one uri')
        self.uri: List[str] = uris
        self.client: Any = client
        self.cluster: bool = cluster
        self.read_from_replicas: bool = read_from_replicas
        self.max_connections: Optional[int] = max_connections
        self.guild_ttl: int = guild_ttl
        self.role_ttl: int = role_ttl
        self.channel_ttl: int = channel_ttl
        self.thread_ttl: int = thread_ttl
        self.emoji_ttl: int = emoji_ttl
        self.sticker_ttl: int = sticker_ttl
        self.member_ttl: int = member_ttl
        self.user_ttl: int = user_ttl
        self.message_ttl: Optional[int] = message_ttl
        self.serve_fetches: bool = serve_fetches
        self.queue_size: int = queue_size
        self.batch_size: int = batch_size

    def __repr__(self) -> str:
        return (
            f'<RedisSettings uri={self.uri!r} client={self.client!r} cluster={self.cluster} message_ttl={self.message_ttl} '
            f'serve_fetches={self.serve_fetches}>'
        )


class CacheSettings:
    """Fork addition, not part of upstream discord.py.

    Controls cache eviction. Pass an instance as the ``cache`` keyword argument of
    :class:`Client`, :class:`AutoShardedClient`, :class:`~discord.ext.commands.Bot`
    or :class:`~discord.ext.commands.AutoShardedBot`. Every value defaults to
    "never evict", which is upstream behaviour.

    Parameters
    -----------
    member_ttl: Optional[:class:`float`]
        Seconds a cached member may go untouched before it is evicted from its guild.
        Members are touched by every event and lookup that resolves them. The bot's own
        member is never evicted.
    member_max: Optional[:class:`int`]
        Maximum cached members per guild. The least recently touched member is evicted first.
    thread_ttl: Optional[:class:`float`]
        Same as ``member_ttl`` for threads.
    thread_max: Optional[:class:`int`]
        Same as ``member_max`` for threads.
    message_ttl: Optional[:class:`float`]
        Seconds a cached message may go untouched before it is evicted. The size bound stays
        ``max_messages`` on the client. Without a TTL the message cache keeps upstream
        first-in first-out behaviour.
    sweep_interval: :class:`float`
        Seconds between eviction passes. Defaults to ``300``.
    max_loaded_guilds: Optional[:class:`int`]
        Maximum guilds kept fully loaded in memory. Beyond it the least recently used guild is
        unloaded and restored from Redis before its next event is processed. Requires ``redis``.
    redis: Optional[:class:`RedisSettings`]
        Enables the Redis tier. Its ``member_ttl``, ``thread_ttl`` and ``message_ttl`` must each be
        at least as long as the matching setting here, since Redis is the fallback once memory
        forgets something. Raises :exc:`ValueError` otherwise.
    """

    __slots__ = (
        'member_ttl',
        'member_max',
        'thread_ttl',
        'thread_max',
        'message_ttl',
        'sweep_interval',
        'max_loaded_guilds',
        'redis',
    )

    def __init__(
        self,
        *,
        member_ttl: Optional[float] = None,
        member_max: Optional[int] = None,
        thread_ttl: Optional[float] = None,
        thread_max: Optional[int] = None,
        message_ttl: Optional[float] = None,
        sweep_interval: float = 300.0,
        max_loaded_guilds: Optional[int] = None,
        redis: Optional[RedisSettings] = None,
    ) -> None:
        for name, value in (
            ('member_ttl', member_ttl),
            ('member_max', member_max),
            ('thread_ttl', thread_ttl),
            ('thread_max', thread_max),
            ('message_ttl', message_ttl),
            ('max_loaded_guilds', max_loaded_guilds),
        ):
            if value is not None and value <= 0:
                raise ValueError(f'{name} must be positive or None')
        if sweep_interval <= 0:
            raise ValueError('sweep_interval must be positive')
        if max_loaded_guilds is not None and redis is None:
            raise TypeError('max_loaded_guilds requires redis=RedisSettings(...)')

        if redis is not None:
            # The Redis copy is the fallback once memory forgets something, so it must
            # outlive (or at least match) the in-memory TTL for the same entity. If Redis
            # expired it first, a guild reload silently comes back missing entries.
            for name, mem_ttl, redis_ttl in (
                ('member', member_ttl, redis.member_ttl),
                ('thread', thread_ttl, redis.thread_ttl),
                ('message', message_ttl, redis.message_ttl),
            ):
                if mem_ttl is not None and redis_ttl is not None and redis_ttl < mem_ttl:
                    raise ValueError(
                        f'redis.{name}_ttl ({redis_ttl}s) is lower than {name}_ttl ({mem_ttl}s); '
                        f'the Redis copy would expire before the in-memory one, so unloading a guild '
                        f'or fetching a cached {name} could silently return stale or missing data. '
                        f'Set redis.{name}_ttl to at least {mem_ttl}.'
                    )

        self.member_ttl: Optional[float] = member_ttl
        self.member_max: Optional[int] = member_max
        self.thread_ttl: Optional[float] = thread_ttl
        self.thread_max: Optional[int] = thread_max
        self.message_ttl: Optional[float] = message_ttl
        self.sweep_interval: float = sweep_interval
        self.max_loaded_guilds: Optional[int] = max_loaded_guilds
        self.redis: Optional[RedisSettings] = redis

    def __repr__(self) -> str:
        return (
            f'<CacheSettings member_ttl={self.member_ttl} member_max={self.member_max} '
            f'thread_ttl={self.thread_ttl} thread_max={self.thread_max} message_ttl={self.message_ttl} '
            f'max_loaded_guilds={self.max_loaded_guilds} redis={self.redis!r}>'
        )


# ---------------------------------------------------------------------------
# In-memory containers
# ---------------------------------------------------------------------------


class TTLDict(Dict[K, V]):
    """A dict whose entries expire after going untouched for ``ttl`` seconds and
    which keeps at most ``max_size`` entries.

    Touching means ``get``, ``[]`` and assignment while ``sliding`` is on, or only
    assignment when it is off (first-in first-out). Expired entries are removed by
    :meth:`sweep`; reads never filter, so hot paths stay at dict speed. The ``pin``
    key is never expired or trimmed.
    """

    __slots__ = ('_ts', '_ttl', '_max', '_pin', '_sliding')

    def __init__(
        self,
        *,
        ttl: Optional[float] = None,
        max_size: Optional[int] = None,
        pin: Optional[K] = None,
        sliding: bool = True,
    ) -> None:
        super().__init__()
        # Insertion ordered: oldest touch first. Doubles as the LRU list.
        self._ts: Dict[K, float] = {}
        self._ttl: Optional[float] = ttl
        self._max: Optional[int] = max_size
        self._pin: Optional[K] = pin
        self._sliding: bool = sliding

    @property
    def ttl(self) -> Optional[float]:
        return self._ttl

    @property
    def max_size(self) -> Optional[int]:
        return self._max

    def _touch(self, key: K) -> None:
        ts = self._ts
        if ts.pop(key, None) is not None:
            ts[key] = _now()

    def __setitem__(self, key: K, value: V) -> None:
        dict.__setitem__(self, key, value)
        if key == self._pin:
            return
        ts = self._ts
        ts.pop(key, None)
        ts[key] = _now()
        max_size = self._max
        if max_size is not None and len(ts) > max_size:
            oldest = next(iter(ts))
            del ts[oldest]
            dict.pop(self, oldest, None)

    def __getitem__(self, key: K) -> V:
        value = dict.__getitem__(self, key)
        if self._sliding:
            self._touch(key)
        return value

    def get(self, key: K, default: Any = None) -> Any:
        value = dict.get(self, key, _MISSING)
        if value is _MISSING:
            return default
        if self._sliding:
            self._touch(key)
        return value

    def pop(self, key: K, *default: Any) -> Any:
        self._ts.pop(key, None)
        return dict.pop(self, key, *default)

    def __delitem__(self, key: K) -> None:
        dict.__delitem__(self, key)
        self._ts.pop(key, None)

    def clear(self) -> None:
        dict.clear(self)
        self._ts.clear()

    def popitem(self) -> Tuple[K, V]:
        key, value = dict.popitem(self)
        self._ts.pop(key, None)
        return key, value

    def setdefault(self, key: K, default: Any = None) -> Any:
        value = dict.get(self, key, _MISSING)
        if value is _MISSING:
            self[key] = default
            return default
        if self._sliding:
            self._touch(key)
        return value

    def update(self, *args: Any, **kwargs: Any) -> None:
        for key, value in dict(*args, **kwargs).items():
            self[key] = value  # type: ignore

    def copy(self) -> TTLDict[K, V]:
        new: TTLDict[K, V] = TTLDict(ttl=self._ttl, max_size=self._max, pin=self._pin, sliding=self._sliding)
        new.update(self)
        return new

    def sweep(self, now: Optional[float] = None) -> int:
        """Remove every entry untouched for longer than ``ttl``. Returns the number removed."""
        ttl = self._ttl
        ts = self._ts
        if ttl is None or not ts:
            return 0
        if now is None:
            now = _now()
        cutoff = now - ttl
        expired: List[K] = []
        for key, touched in ts.items():
            if touched > cutoff:
                break
            expired.append(key)
        for key in expired:
            del ts[key]
            dict.pop(self, key, None)
        return len(expired)


class MessageCache:
    """Drop-in replacement for the message deque: bounded by ``max_size`` and
    optionally by ``ttl``. Lookups by id are O(1)."""

    __slots__ = ('_d',)

    def __init__(self, max_size: Optional[int], ttl: Optional[float] = None) -> None:
        self._d: TTLDict[int, Message] = TTLDict(ttl=ttl, max_size=max_size, sliding=ttl is not None)

    def append(self, message: Message) -> None:
        self._d[message.id] = message

    def remove(self, message: Message) -> None:
        try:
            del self._d[message.id]
        except KeyError:
            raise ValueError('message is not cached') from None

    def get(self, message_id: Optional[int], default: Any = None) -> Any:
        return self._d.get(message_id, default)  # type: ignore

    def remove_if(self, predicate: Callable[[Message], bool]) -> int:
        removed = [key for key, message in self._d.items() if predicate(message)]
        for key in removed:
            del self._d[key]
        return len(removed)

    def sweep(self, now: Optional[float] = None) -> int:
        return self._d.sweep(now)

    def clear(self) -> None:
        self._d.clear()

    def __iter__(self) -> Iterator[Message]:
        return iter(self._d.values())

    def __reversed__(self) -> Iterator[Message]:
        return reversed(self._d.values())

    def __len__(self) -> int:
        return len(self._d)

    def __bool__(self) -> bool:
        return len(self._d) > 0

    def __contains__(self, item: Any) -> bool:
        return getattr(item, 'id', _MISSING) in self._d

    def __repr__(self) -> str:
        return f'<MessageCache size={len(self._d)} max_size={self._d.max_size} ttl={self._d.ttl}>'


# ---------------------------------------------------------------------------
# Redis tier
# ---------------------------------------------------------------------------

# Keys removed from a GUILD_CREATE payload before storing the guild base. Sub
# entities live in their own hashes; ephemeral data is not persisted.
GUILD_BASE_STRIP_KEYS = frozenset(
    {
        'roles',
        'emojis',
        'stickers',
        'members',
        'channels',
        'threads',
        'presences',
        'voice_states',
        'stage_instances',
        'guild_scheduled_events',
        'soundboard_sounds',
    }
)

_GUILD_HASHES = (':roles', ':channels', ':threads', ':emojis', ':stickers')


def _guild_key(guild_id: int, suffix: str = '') -> str:
    # {guild_id} is a Redis Cluster hash tag: every key of a guild shares a slot.
    return f'guild:{{{guild_id}}}{suffix}'


def _member_key(guild_id: int, user_id: int) -> str:
    return f'member:{{{guild_id}}}:{user_id}'


def _user_key(user_id: int) -> str:
    return f'user:{user_id}'


def _message_key(channel_id: int, message_id: int) -> str:
    return f'message:{{{channel_id}}}:{message_id}'


def _load_hash(raw: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    return {key: json.loads(value) for key, value in raw.items()}


class RedisCache:
    """Thin client wrapper. Writes are synchronous pipeline builders so the manager
    can batch them; reads are coroutines."""

    __slots__ = ('settings', '_client', '_owns_client')

    def __init__(self, settings: RedisSettings) -> None:
        if importlib.util.find_spec('redis') is None:
            raise RuntimeError(
                'The redis package is required for Redis caching. Install it with: pip install "discord.py[redis]"'
            )
        self.settings: RedisSettings = settings
        self._client: Optional[Any] = None
        self._owns_client: bool = False

    @property
    def connected(self) -> bool:
        return self._client is not None

    @property
    def client(self) -> Any:
        if self._client is None:
            raise RuntimeError('RedisCache is not connected')
        return self._client

    async def connect(self) -> None:
        if self._client is not None:
            return

        settings = self.settings
        if settings.client is not None:
            client: Any = settings.client() if callable(settings.client) else settings.client
            if client is None:
                raise RuntimeError('RedisSettings.client factory returned None')
            await client.ping()
            self._client = client
            self._owns_client = False
            _log.info('Redis cache using the provided %s client', type(client).__name__)
            return

        import redis.asyncio as aioredis

        kwargs: Dict[str, Any] = {'decode_responses': True}
        if settings.max_connections is not None:
            kwargs['max_connections'] = settings.max_connections

        last_error: Optional[BaseException] = None
        for uri in settings.uri:
            try:
                if settings.cluster:
                    client = aioredis.RedisCluster.from_url(uri, read_from_replicas=settings.read_from_replicas, **kwargs)
                else:
                    client = aioredis.Redis.from_url(uri, **kwargs)
                await client.ping()
            except Exception as exc:
                last_error = exc
                _log.warning('Could not connect to Redis at %s: %s', uri, exc)
                continue
            self._client = client
            self._owns_client = True
            _log.info('Redis cache connected (%s) via %s', 'cluster' if settings.cluster else 'standalone', uri)
            return

        raise RuntimeError(f'Could not connect to any Redis node in {settings.uri!r}') from last_error

    async def close(self) -> None:
        client = self._client
        if client is not None:
            self._client = None
            if self._owns_client:
                await client.aclose()

    def pipeline(self) -> Any:
        return self.client.pipeline(transaction=False)

    # Pipeline builders

    def _hset_bulk(self, pipe: Any, key: str, mapping: Dict[int, Dict[str, Any]], ttl: int) -> None:
        if not mapping:
            return
        pipe.hset(key, mapping={str(k): json.dumps(v) for k, v in mapping.items()})
        pipe.expire(key, ttl)

    def _replace_hash(self, pipe: Any, key: str, mapping: Dict[int, Dict[str, Any]], ttl: int) -> None:
        pipe.delete(key)
        self._hset_bulk(pipe, key, mapping, ttl)

    def _hset_one(self, pipe: Any, key: str, field: int, data: Dict[str, Any], ttl: int) -> None:
        pipe.hset(key, str(field), json.dumps(data))
        pipe.expire(key, ttl)

    def set_guild_base(self, pipe: Any, guild_id: int, data: Dict[str, Any]) -> None:
        pipe.set(_guild_key(guild_id), json.dumps(data), ex=self.settings.guild_ttl)

    def delete_guild(self, pipe: Any, guild_id: int) -> None:
        pipe.delete(_guild_key(guild_id))
        for suffix in _GUILD_HASHES:
            pipe.delete(_guild_key(guild_id, suffix))

    def set_role(self, pipe: Any, guild_id: int, role_id: int, data: Dict[str, Any]) -> None:
        self._hset_one(pipe, _guild_key(guild_id, ':roles'), role_id, data, self.settings.role_ttl)

    def replace_roles(self, pipe: Any, guild_id: int, roles: Dict[int, Dict[str, Any]]) -> None:
        self._replace_hash(pipe, _guild_key(guild_id, ':roles'), roles, self.settings.role_ttl)

    def delete_role(self, pipe: Any, guild_id: int, role_id: int) -> None:
        pipe.hdel(_guild_key(guild_id, ':roles'), str(role_id))

    def set_channel(self, pipe: Any, guild_id: int, channel_id: int, data: Dict[str, Any]) -> None:
        self._hset_one(pipe, _guild_key(guild_id, ':channels'), channel_id, data, self.settings.channel_ttl)

    def replace_channels(self, pipe: Any, guild_id: int, channels: Dict[int, Dict[str, Any]]) -> None:
        self._replace_hash(pipe, _guild_key(guild_id, ':channels'), channels, self.settings.channel_ttl)

    def delete_channel(self, pipe: Any, guild_id: int, channel_id: int) -> None:
        pipe.hdel(_guild_key(guild_id, ':channels'), str(channel_id))

    def set_thread(self, pipe: Any, guild_id: int, thread_id: int, data: Dict[str, Any]) -> None:
        self._hset_one(pipe, _guild_key(guild_id, ':threads'), thread_id, data, self.settings.thread_ttl)

    def set_threads_bulk(self, pipe: Any, guild_id: int, threads: Dict[int, Dict[str, Any]]) -> None:
        self._hset_bulk(pipe, _guild_key(guild_id, ':threads'), threads, self.settings.thread_ttl)

    def replace_threads(self, pipe: Any, guild_id: int, threads: Dict[int, Dict[str, Any]]) -> None:
        self._replace_hash(pipe, _guild_key(guild_id, ':threads'), threads, self.settings.thread_ttl)

    def delete_thread(self, pipe: Any, guild_id: int, thread_id: int) -> None:
        pipe.hdel(_guild_key(guild_id, ':threads'), str(thread_id))

    def replace_emojis(self, pipe: Any, guild_id: int, emojis: Dict[int, Dict[str, Any]]) -> None:
        self._replace_hash(pipe, _guild_key(guild_id, ':emojis'), emojis, self.settings.emoji_ttl)

    def replace_stickers(self, pipe: Any, guild_id: int, stickers: Dict[int, Dict[str, Any]]) -> None:
        self._replace_hash(pipe, _guild_key(guild_id, ':stickers'), stickers, self.settings.sticker_ttl)

    def set_member(self, pipe: Any, guild_id: int, user_id: int, data: Dict[str, Any]) -> None:
        pipe.set(_member_key(guild_id, user_id), json.dumps(data), ex=self.settings.member_ttl)
        user = data.get('user')
        if user:
            self.set_user(pipe, user_id, user)

    def delete_member(self, pipe: Any, guild_id: int, user_id: int) -> None:
        pipe.delete(_member_key(guild_id, user_id))

    def set_user(self, pipe: Any, user_id: int, data: Dict[str, Any]) -> None:
        pipe.set(_user_key(user_id), json.dumps(data), ex=self.settings.user_ttl)

    def set_message(self, pipe: Any, channel_id: int, message_id: int, data: Dict[str, Any]) -> None:
        ttl = self.settings.message_ttl
        if ttl is None or not data:
            return
        key = _message_key(channel_id, message_id)
        pipe.hset(key, mapping={field: json.dumps(value) for field, value in data.items()})
        pipe.expire(key, ttl)

    # Reads

    async def get_guild_base(self, guild_id: int) -> Optional[Dict[str, Any]]:
        raw = await self.client.get(_guild_key(guild_id))
        return json.loads(raw) if raw is not None else None

    async def get_guild_bundle(self, guild_id: int) -> Optional[Tuple[Dict[str, Any], ...]]:
        """Returns ``(base, roles, channels, threads, emojis, stickers)`` in one round trip,
        or ``None`` when the base key is gone."""
        pipe = self.pipeline()
        pipe.get(_guild_key(guild_id))
        for suffix in _GUILD_HASHES:
            pipe.hgetall(_guild_key(guild_id, suffix))
        results = await pipe.execute()
        base = results[0]
        if base is None:
            return None
        return (json.loads(base), *[_load_hash(raw) for raw in results[1:]])

    async def get_member(self, guild_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        raw = await self.client.get(_member_key(guild_id, user_id))
        return json.loads(raw) if raw is not None else None

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        raw = await self.client.get(_user_key(user_id))
        return json.loads(raw) if raw is not None else None

    async def get_message(self, channel_id: int, message_id: int) -> Optional[Dict[str, Any]]:
        raw = await self.client.hgetall(_message_key(channel_id, message_id))
        return _load_hash(raw) if raw else None


# Event name -> pipeline builder. Every builder receives the raw gateway payload.
MirrorFunc = Callable[[RedisCache, Any, Dict[str, Any]], None]


def _mirror_guild_create(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    if data.get('unavailable'):
        return
    guild_id = int(data['id'])
    base = {k: v for k, v in data.items() if k not in GUILD_BASE_STRIP_KEYS}
    cache.set_guild_base(pipe, guild_id, base)
    cache.replace_roles(pipe, guild_id, {int(r['id']): r for r in data.get('roles', [])})
    cache.replace_channels(pipe, guild_id, {int(c['id']): c for c in data.get('channels', [])})
    cache.replace_threads(pipe, guild_id, {int(t['id']): t for t in data.get('threads', [])})
    cache.replace_emojis(pipe, guild_id, {int(e['id']): e for e in data.get('emojis', [])})
    cache.replace_stickers(pipe, guild_id, {int(s['id']): s for s in data.get('stickers', [])})
    if cache.settings.serve_fetches:
        for member in data.get('members', []):
            cache.set_member(pipe, guild_id, int(member['user']['id']), member)


def _mirror_guild_update(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    guild_id = int(data['id'])
    base = {k: v for k, v in data.items() if k not in GUILD_BASE_STRIP_KEYS}
    cache.set_guild_base(pipe, guild_id, base)
    if 'roles' in data:
        cache.replace_roles(pipe, guild_id, {int(r['id']): r for r in data['roles']})


def _mirror_guild_delete(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    if not data.get('unavailable', False):
        cache.delete_guild(pipe, int(data['id']))


def _mirror_channel_set(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    guild_id = data.get('guild_id')
    if guild_id is not None:
        cache.set_channel(pipe, int(guild_id), int(data['id']), data)


def _mirror_channel_delete(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    guild_id = data.get('guild_id')
    if guild_id is not None:
        cache.delete_channel(pipe, int(guild_id), int(data['id']))


def _mirror_thread_set(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    guild_id = int(data['guild_id'])
    if data.get('thread_metadata', {}).get('archived'):
        cache.delete_thread(pipe, guild_id, int(data['id']))
    else:
        cache.set_thread(pipe, guild_id, int(data['id']), data)


def _mirror_thread_delete(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    cache.delete_thread(pipe, int(data['guild_id']), int(data['id']))


def _mirror_thread_list_sync(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    guild_id = int(data['guild_id'])
    threads = {int(t['id']): t for t in data.get('threads', [])}
    if 'channel_ids' in data:
        cache.set_threads_bulk(pipe, guild_id, threads)
    else:
        cache.replace_threads(pipe, guild_id, threads)


def _mirror_role_set(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    role = data['role']
    cache.set_role(pipe, int(data['guild_id']), int(role['id']), role)


def _mirror_role_delete(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    cache.delete_role(pipe, int(data['guild_id']), int(data['role_id']))


def _mirror_member_set(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    cache.set_member(pipe, int(data['guild_id']), int(data['user']['id']), data)


def _mirror_member_delete(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    cache.delete_member(pipe, int(data['guild_id']), int(data['user']['id']))


def _mirror_members_chunk(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    guild_id = int(data['guild_id'])
    for member in data.get('members', []):
        cache.set_member(pipe, guild_id, int(member['user']['id']), member)


def _mirror_emojis_update(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    cache.replace_emojis(pipe, int(data['guild_id']), {int(e['id']): e for e in data.get('emojis', [])})


def _mirror_stickers_update(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    cache.replace_stickers(pipe, int(data['guild_id']), {int(s['id']): s for s in data.get('stickers', [])})


def _mirror_message_set(cache: RedisCache, pipe: Any, data: Dict[str, Any]) -> None:
    cache.set_message(pipe, int(data['channel_id']), int(data['id']), data)


MIRROR: Dict[str, MirrorFunc] = {
    'GUILD_CREATE': _mirror_guild_create,
    'GUILD_UPDATE': _mirror_guild_update,
    'GUILD_DELETE': _mirror_guild_delete,
    'CHANNEL_CREATE': _mirror_channel_set,
    'CHANNEL_UPDATE': _mirror_channel_set,
    'CHANNEL_DELETE': _mirror_channel_delete,
    'THREAD_CREATE': _mirror_thread_set,
    'THREAD_UPDATE': _mirror_thread_set,
    'THREAD_DELETE': _mirror_thread_delete,
    'THREAD_LIST_SYNC': _mirror_thread_list_sync,
    'GUILD_ROLE_CREATE': _mirror_role_set,
    'GUILD_ROLE_UPDATE': _mirror_role_set,
    'GUILD_ROLE_DELETE': _mirror_role_delete,
    'GUILD_MEMBER_ADD': _mirror_member_set,
    'GUILD_MEMBER_UPDATE': _mirror_member_set,
    'GUILD_MEMBER_REMOVE': _mirror_member_delete,
    'GUILD_MEMBERS_CHUNK': _mirror_members_chunk,
    'GUILD_EMOJIS_UPDATE': _mirror_emojis_update,
    'GUILD_STICKERS_UPDATE': _mirror_stickers_update,
    'MESSAGE_CREATE': _mirror_message_set,
    'MESSAGE_UPDATE': _mirror_message_set,
}

_MEMBER_EVENTS = frozenset({'GUILD_MEMBER_ADD', 'GUILD_MEMBER_UPDATE', 'GUILD_MEMBER_REMOVE', 'GUILD_MEMBERS_CHUNK'})
_MESSAGE_EVENTS = frozenset({'MESSAGE_CREATE', 'MESSAGE_UPDATE'})

# Events whose payload carries the guild id under ``id`` rather than ``guild_id``.
_GUILD_ID_IN_ID = frozenset({'GUILD_CREATE', 'GUILD_UPDATE', 'GUILD_DELETE'})
# Events that must never trigger a guild load nor count as guild activity.
_NO_LOAD = frozenset(
    {
        'READY',
        'RESUMED',
        'GUILD_CREATE',
        'GUILD_DELETE',
        'PRESENCE_UPDATE',
        'TYPING_START',
        'GUILD_MEMBERS_CHUNK',
        'VOICE_SERVER_UPDATE',
    }
)


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------


class CacheManager:
    """Owns the cache containers, the sweeper, the Redis writer and guild unloading.
    One instance lives on ``ConnectionState._cache``."""

    def __init__(self, state: ConnectionState, settings: Optional[CacheSettings]) -> None:
        self._state: ConnectionState = state
        self.settings: CacheSettings = settings if settings is not None else CacheSettings()
        s = self.settings

        self._member_ttl_on: bool = s.member_ttl is not None or s.member_max is not None
        self._thread_ttl_on: bool = s.thread_ttl is not None or s.thread_max is not None
        self._sweeps_needed: bool = self._member_ttl_on or self._thread_ttl_on or s.message_ttl is not None

        self.redis: Optional[RedisCache] = RedisCache(s.redis) if s.redis is not None else None
        self._mirror: Dict[str, MirrorFunc] = self._build_mirror()

        # Loaded guilds in LRU order; only used with max_loaded_guilds.
        self._lru: Optional[OrderedDict[int, None]] = OrderedDict() if s.max_loaded_guilds is not None else None
        self._unloaded: Set[int] = set()
        self._loading: Dict[int, asyncio.Future[Guild]] = {}

        self._sweeper: Optional[asyncio.Task[None]] = None
        self._writer: Optional[asyncio.Task[None]] = None
        self._queue: Optional[asyncio.Queue[Tuple[Any, Any]]] = None
        self._inflight: bool = False
        self._redis_down_until: float = 0.0
        self._dropped: int = 0
        self._last_drop_warning: float = 0.0

        if self._lru is None and self.redis is None:
            # The gateway checks this attribute before awaiting. None means "nothing to do".
            self.pre_event = None  # type: ignore

    def _build_mirror(self) -> Dict[str, MirrorFunc]:
        redis = self.settings.redis
        if redis is None:
            return {}
        mirror = dict(MIRROR)
        if not redis.serve_fetches:
            for event in _MEMBER_EVENTS:
                mirror.pop(event, None)
        if redis.message_ttl is None:
            for event in _MESSAGE_EVENTS:
                mirror.pop(event, None)
        return mirror

    # Container factories

    def members(self) -> Dict[int, Member]:
        if not self._member_ttl_on:
            return {}
        s = self.settings
        return TTLDict(ttl=s.member_ttl, max_size=s.member_max, pin=self._state.self_id)

    def threads(self) -> Dict[int, Thread]:
        if not self._thread_ttl_on:
            return {}
        s = self.settings
        return TTLDict(ttl=s.thread_ttl, max_size=s.thread_max)

    def messages(self) -> Optional[MessageCache]:
        max_messages = self._state.max_messages
        if max_messages is None:
            return None
        return MessageCache(max_messages, self.settings.message_ttl)

    # Hooks called from ConnectionState

    def guild_added(self, guild: Guild) -> None:
        if self._member_ttl_on and type(guild._members) is not TTLDict:
            members = self.members()
            members.update(guild._members)
            guild._members = members
        if self._thread_ttl_on and type(guild._threads) is not TTLDict:
            threads = self.threads()
            threads.update(guild._threads)
            guild._threads = threads
        if self._lru is not None and not guild.unavailable:
            self.mark_loaded(guild.id)

    def touch(self, guild_id: Optional[int]) -> None:
        lru = self._lru
        if lru is not None and guild_id in lru:
            lru.move_to_end(guild_id)

    def forget(self, guild_id: int) -> None:
        if self._lru is not None:
            self._lru.pop(guild_id, None)
        self._unloaded.discard(guild_id)
        self._loading.pop(guild_id, None)

    def reset(self) -> None:
        if self._lru is not None:
            self._lru.clear()
        self._unloaded.clear()
        self._loading.clear()

    # Guild unloading

    def is_loaded(self, guild: Guild) -> bool:
        return guild.id not in self._unloaded

    def mark_loaded(self, guild_id: int) -> None:
        lru = self._lru
        if lru is None:
            return
        self._unloaded.discard(guild_id)
        lru[guild_id] = None
        lru.move_to_end(guild_id)
        max_loaded: int = self.settings.max_loaded_guilds  # type: ignore
        while len(lru) > max_loaded:
            victim, _ = lru.popitem(last=False)
            if victim == guild_id:
                lru[guild_id] = None
                break
            guild = self._state._guilds.get(victim)
            if guild is not None:
                self._unload(guild)

    def _unload(self, guild: Guild) -> None:
        state = self._state
        self_id = state.self_id
        me = dict.get(guild._members, self_id) if self_id is not None else None
        guild._channels = {}
        guild._members = self.members()
        guild._roles = {}
        guild._threads = self.threads()
        guild._stage_instances = {}
        guild._scheduled_events = {}
        guild._soundboard_sounds = {}
        guild._voice_states = {}
        for emoji in guild.emojis:
            state._emojis.pop(emoji.id, None)
        for sticker in guild.stickers:
            state._stickers.pop(sticker.id, None)
        guild.emojis = ()
        guild.stickers = ()
        if me is not None:
            guild._members[self_id] = me  # type: ignore
        self._unloaded.add(guild.id)
        _log.debug('Unloaded guild %s from memory', guild.id)

    async def load_guild(self, guild: Guild) -> Guild:
        """Restores an unloaded guild from Redis, or from the API when Redis has no copy."""
        guild_id = guild.id
        if guild_id not in self._unloaded:
            return guild
        future = self._loading.get(guild_id)
        if future is None:
            future = asyncio.ensure_future(self._load(guild))
            self._loading[guild_id] = future
            future.add_done_callback(lambda _: self._loading.pop(guild_id, None))
        return await asyncio.shield(future)

    async def _load(self, guild: Guild) -> Guild:
        state = self._state
        redis = self.redis
        bundle: Optional[Tuple[Dict[str, Any], ...]] = None
        if redis is not None and _now() >= self._redis_down_until:
            try:
                await self.flush()
                bundle = await redis.get_guild_bundle(guild.id)
            except Exception:
                self._mark_redis_down('read')

        if bundle is not None:
            base, roles, channels, threads, emojis, stickers = bundle
            guild._from_data(base)  # type: ignore
            for role_data in roles.values():
                role = Role(guild=guild, data=role_data, state=state)
                guild._roles[role.id] = role
            for channel_data in channels.values():
                factory, _ = _guild_channel_factory(channel_data['type'])
                if factory:
                    channel = factory(guild=guild, data=channel_data, state=state)
                    guild._channels[channel.id] = channel
            for thread_data in threads.values():
                thread = Thread(guild=guild, state=state, data=thread_data)
                guild._threads[thread.id] = thread
            if state.cache_guild_expressions:
                if emojis:
                    guild.emojis = tuple(state.store_emoji(guild, d) for d in emojis.values())
                if stickers:
                    guild.stickers = tuple(state.store_sticker(guild, d) for d in stickers.values())
            _log.debug('Loaded guild %s from Redis', guild.id)
        else:
            guild_data, channels_data = await asyncio.gather(
                state.http.get_guild(guild.id, with_counts=False),
                state.http.get_all_guild_channels(guild.id),
            )
            guild_data['channels'] = channels_data
            guild._from_data(guild_data)
            if redis is not None:
                self._enqueue(_mirror_guild_create, guild_data)  # type: ignore
            _log.debug('Loaded guild %s from the API', guild.id)

        self._unloaded.discard(guild.id)
        self.mark_loaded(guild.id)
        return guild

    # Gateway hook

    async def pre_event(self, event: str, data: Any) -> None:
        """Runs before the parser of every gateway event. Restores an unloaded guild
        so the parser sees it hydrated, and queues the Redis mirror write."""
        if type(data) is not dict:
            return
        try:
            if self._lru is not None:
                guild_id = data.get('guild_id')
                if guild_id is None and event in _GUILD_ID_IN_ID:
                    guild_id = data.get('id')
                if guild_id is not None:
                    guild_id = int(guild_id)
                    if event == 'GUILD_CREATE':
                        if not data.get('unavailable'):
                            self.mark_loaded(guild_id)
                    elif event in _NO_LOAD:
                        pass
                    elif guild_id in self._unloaded:
                        guild = self._state._guilds.get(guild_id)
                        if guild is not None:
                            await self.load_guild(guild)
                    else:
                        self.touch(guild_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            _log.warning('Failed to prepare guild for %s', event, exc_info=True)

        mirror = self._mirror.get(event)
        if mirror is not None:
            self._enqueue(mirror, data)

    # Lifecycle

    async def start(self) -> None:
        redis = self.redis
        if redis is not None:
            if not redis.connected:
                await redis.connect()
            if self._writer is None or self._writer.done():
                queue_size = self.settings.redis.queue_size  # type: ignore
                self._queue = asyncio.Queue(maxsize=queue_size)
                self._writer = asyncio.create_task(self._writer_loop(), name='discord-cache-redis-writer')
        if self._sweeps_needed and (self._sweeper is None or self._sweeper.done()):
            self._sweeper = asyncio.create_task(self._sweep_loop(), name='discord-cache-sweeper')

    async def close(self) -> None:
        sweeper = self._sweeper
        if sweeper is not None:
            self._sweeper = None
            sweeper.cancel()
            try:
                await sweeper
            except (asyncio.CancelledError, Exception):
                pass
        writer = self._writer
        if writer is not None:
            try:
                await asyncio.wait_for(self.flush(), timeout=5.0)
            except Exception:
                pass
            self._writer = None
            writer.cancel()
            try:
                await writer
            except (asyncio.CancelledError, Exception):
                pass
            self._queue = None
        if self.redis is not None:
            await self.redis.close()

    # Sweeper

    async def sweep_once(self) -> int:
        now = _now()
        removed = 0
        count = 0
        for guild in list(self._state._guilds.values()):
            members = guild._members
            if type(members) is TTLDict:
                removed += members.sweep(now)
            threads = guild._threads
            if type(threads) is TTLDict:
                removed += threads.sweep(now)
            count += 1
            if count % _SWEEP_YIELD_EVERY == 0:
                await asyncio.sleep(0)
        messages = self._state._messages
        if isinstance(messages, MessageCache):
            removed += messages.sweep(now)
        return removed

    async def _sweep_loop(self) -> None:
        interval = self.settings.sweep_interval
        while True:
            await asyncio.sleep(interval)
            try:
                removed = await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                _log.exception('Cache sweep failed')
            else:
                if removed:
                    _log.debug('Cache sweep evicted %s entries', removed)

    # Redis writer

    def _mark_redis_down(self, what: str) -> None:
        self._redis_down_until = _now() + _REDIS_RETRY_AFTER
        _log.warning('Redis %s failed; cache mirroring paused for %.0fs', what, _REDIS_RETRY_AFTER, exc_info=True)

    def _enqueue(self, fn: MirrorFunc, data: Dict[str, Any]) -> None:
        queue = self._queue
        if queue is None or _now() < self._redis_down_until:
            return
        try:
            queue.put_nowait((fn, data))
        except asyncio.QueueFull:
            self._dropped += 1
            now = _now()
            if now - self._last_drop_warning > 60.0:
                self._last_drop_warning = now
                _log.warning('Redis write queue is full; %s cache writes dropped so far', self._dropped)

    async def flush(self) -> None:
        """Waits until every queued Redis write has been sent."""
        queue = self._queue
        if queue is None or (queue.empty() and not self._inflight):
            return
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        await queue.put((_BARRIER, future))
        await future

    async def _writer_loop(self) -> None:
        queue = self._queue
        assert queue is not None
        batch_size: int = self.settings.redis.batch_size  # type: ignore
        while True:
            batch: List[Tuple[Any, Any]] = [await queue.get()]
            while len(batch) < batch_size:
                try:
                    batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            self._inflight = True
            try:
                await self._write_batch(batch)
            finally:
                self._inflight = False

    async def _write_batch(self, batch: List[Tuple[Any, Any]]) -> None:
        redis = self.redis
        assert redis is not None
        barriers: List[asyncio.Future[None]] = []
        writes = 0
        if _now() >= self._redis_down_until:
            pipe = redis.pipeline()
            for fn, data in batch:
                if fn is _BARRIER:
                    barriers.append(data)
                    continue
                try:
                    fn(redis, pipe, data)
                    writes += 1
                except Exception:
                    _log.exception('Failed to build Redis write for cache mirror')
            if writes:
                try:
                    await pipe.execute()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._mark_redis_down('write')
        else:
            barriers.extend(data for fn, data in batch if fn is _BARRIER)
        for future in barriers:
            if not future.done():
                future.set_result(None)

    # Lookups that may fall back to Redis

    async def _redis_read(self, coro_factory: Callable[[], Any]) -> Optional[Dict[str, Any]]:
        redis = self.redis
        if redis is None or _now() < self._redis_down_until:
            return None
        try:
            return await coro_factory()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._mark_redis_down('read')
            return None

    async def get_user(self, user_id: int) -> Optional[User]:
        state = self._state
        user = state.get_user(user_id)
        if user is not None or self.redis is None:
            return user
        data = await self._redis_read(lambda: self.redis.get_user(user_id))  # type: ignore
        return state.store_user(data) if data else None  # type: ignore

    async def get_member(self, guild: Guild, user_id: int) -> Optional[Member]:
        member = guild.get_member(user_id)
        if member is not None or self.redis is None:
            return member
        data = await self._redis_read(lambda: self.redis.get_member(guild.id, user_id))  # type: ignore
        return Member(data=data, guild=guild, state=self._state) if data else None  # type: ignore

    async def get_message(self, channel_id: int, message_id: int) -> Optional[Message]:
        state = self._state
        message = state._get_message(message_id)
        if message is not None or self.redis is None:
            return message
        data = await self._redis_read(lambda: self.redis.get_message(channel_id, message_id))  # type: ignore
        if not data:
            return None
        channel = state.get_channel(channel_id)
        if channel is None:
            raw_guild_id = data.get('guild_id')
            guild_id = int(raw_guild_id) if raw_guild_id is not None else None
            channel = PartialMessageable(state=state, id=channel_id, guild_id=guild_id)
        return Message(state=state, channel=channel, data=data)  # type: ignore


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def is_guild_loaded(guild: Guild) -> bool:
    """Whether the guild's channels, roles and members are currently in memory.
    Always ``True`` unless ``max_loaded_guilds`` is configured."""
    return guild._state._cache.is_loaded(guild)


async def load_guild(guild: Guild) -> Guild:
    """|coro|

    Restores an unloaded guild into memory. No-op when it is already loaded."""
    return await guild._state._cache.load_guild(guild)


async def get_cached_user(client: Client, user_id: int) -> Optional[User]:
    """|coro|

    Returns the user from memory, else from Redis when ``serve_fetches`` is on, else ``None``."""
    return await client._connection._cache.get_user(user_id)


async def get_cached_member(guild: Guild, user_id: int) -> Optional[Member]:
    """|coro|

    Returns the member from memory, else from Redis when ``serve_fetches`` is on, else ``None``."""
    return await guild._state._cache.get_member(guild, user_id)


async def get_cached_message(client: Client, channel_id: int, message_id: int) -> Optional[Message]:
    """|coro|

    Returns the message from memory, else from Redis when ``message_ttl`` is set, else ``None``.
    Deleted messages stay readable from Redis until their TTL runs out."""
    return await client._connection._cache.get_message(channel_id, message_id)
