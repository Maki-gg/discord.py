"""Tests for the Maki fork cache layer (discord/cache.py)."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

import discord
from discord import cache as cache_mod
from discord.cache import (
    MIRROR,
    CacheManager,
    CacheSettings,
    MessageCache,
    RedisCache,
    RedisSettings,
    TTLDict,
    _mirror_guild_create,
)
from discord.utils import SequenceProxy


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock()
    monkeypatch.setattr(cache_mod, '_now', clock)
    return clock


class FakePipeline:
    def __init__(self, store: 'FakeRedis') -> None:
        self.store = store
        self.ops: List[tuple] = []

    def __getattr__(self, name: str):
        def record(*args: Any, **kwargs: Any) -> None:
            self.ops.append((name, args, kwargs))

        return record

    async def execute(self) -> List[Any]:
        ops, self.ops = self.ops, []
        return [self.store.apply(op) for op in ops]


class FakeRedis:
    """Just enough of redis.asyncio to exercise the cache: strings, hashes, TTLs."""

    def __init__(self) -> None:
        self.data: Dict[str, Any] = {}
        self.ttls: Dict[str, int] = {}
        self.fail = False
        self.closed = False
        self.executed = 0

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        return FakePipeline(self)

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        self.closed = True

    async def get(self, key: str) -> Any:
        return self.apply(('get', (key,), {}))

    async def hgetall(self, key: str) -> Dict[str, str]:
        return self.apply(('hgetall', (key,), {}))

    def apply(self, op: tuple) -> Any:
        if self.fail:
            raise ConnectionError('redis down')
        name, args, kwargs = op
        self.executed += 1
        if name == 'set':
            key, value = args
            self.data[key] = value
            if kwargs.get('ex') is not None:
                self.ttls[key] = kwargs['ex']
            return True
        if name == 'get':
            value = self.data.get(args[0])
            return value if isinstance(value, str) else None
        if name == 'hset':
            key = args[0]
            bucket = self.data.setdefault(key, {})
            if kwargs.get('mapping'):
                bucket.update(kwargs['mapping'])
                return len(kwargs['mapping'])
            bucket[args[1]] = args[2]
            return 1
        if name == 'hgetall':
            value = self.data.get(args[0])
            return dict(value) if isinstance(value, dict) else {}
        if name == 'hdel':
            bucket = self.data.get(args[0], {})
            return sum(1 for field in args[1:] if bucket.pop(field, None) is not None)
        if name == 'delete':
            return sum(1 for key in args if self.data.pop(key, None) is not None)
        if name == 'expire':
            self.ttls[args[0]] = args[1]
            return True
        raise AssertionError(f'unexpected op {name}')

    def string(self, key: str) -> Any:
        return json.loads(self.data[key])

    def hash(self, key: str) -> Dict[str, Any]:
        return {k: json.loads(v) for k, v in self.data[key].items()}


def redis_cache(**kwargs: Any) -> RedisCache:
    rc = RedisCache(RedisSettings('redis://test', cluster=False, **kwargs))
    rc._client = FakeRedis()
    return rc


def fake_message(message_id: int, guild: Any = None) -> SimpleNamespace:
    return SimpleNamespace(id=message_id, guild=guild)


def fake_guild(guild_id: int, *, unavailable: bool = False, members: Dict[int, Any] = None) -> SimpleNamespace:
    return SimpleNamespace(
        id=guild_id,
        unavailable=unavailable,
        _members=dict(members or {}),
        _threads={},
        _channels={1: 'channel'},
        _roles={2: 'role'},
        _voice_states={},
        _stage_instances={},
        _scheduled_events={},
        _soundboard_sounds={},
        emojis=(),
        stickers=(),
    )


class FakeState:
    def __init__(self, *, max_messages: Any = 100, self_id: Any = 999) -> None:
        self._guilds: Dict[int, Any] = {}
        self._emojis: Dict[int, Any] = {}
        self._stickers: Dict[int, Any] = {}
        self._messages: Any = None
        self.max_messages = max_messages
        self.self_id = self_id
        self.cache_guild_expressions = True

    def get_user(self, user_id: int) -> Any:
        return None

    def _get_message(self, message_id: int) -> Any:
        return self._messages.get(message_id) if self._messages else None


def manager(settings: CacheSettings = None, **state_kwargs: Any) -> CacheManager:
    state = FakeState(**state_kwargs)
    m = CacheManager(state, settings)  # type: ignore
    state._messages = m.messages()
    return m


async def connect_fake(manager: CacheManager) -> FakeRedis:
    fake = FakeRedis()
    assert manager.redis is not None
    manager.redis._client = fake
    manager.redis._owns_client = True
    await manager.start()
    return fake


GUILD_CREATE = {
    'id': '1',
    'name': 'guild',
    'owner_id': '2',
    'member_count': 2,
    'features': [],
    'roles': [
        {
            'id': '1',
            'name': '@everyone',
            'permissions': '0',
            'position': 0,
            'color': 0,
            'hoist': False,
            'managed': False,
            'mentionable': False,
        }
    ],
    'emojis': [],
    'stickers': [],
    'channels': [
        {
            'id': '20',
            'type': 0,
            'name': 'general',
            'position': 0,
            'permission_overwrites': [],
            'nsfw': False,
            'parent_id': None,
        }
    ],
    'threads': [],
    'members': [
        {
            'user': {'id': '2', 'username': 'user', 'discriminator': '0', 'avatar': None, 'global_name': None},
            'roles': [],
            'joined_at': '2024-01-01T00:00:00+00:00',
            'deaf': False,
            'mute': False,
            'flags': 0,
        }
    ],
}

MESSAGE_CREATE = {
    'id': '10',
    'channel_id': '20',
    'guild_id': '1',
    'author': {'id': '2', 'username': 'user', 'discriminator': '0', 'avatar': None, 'global_name': None},
    'content': 'hello',
    'timestamp': '2024-01-01T00:00:00+00:00',
    'edited_timestamp': None,
    'tts': False,
    'mention_everyone': False,
    'mentions': [],
    'mention_roles': [],
    'attachments': [],
    'embeds': [],
    'pinned': False,
    'type': 0,
    'flags': 0,
}


def make_client(**cache_kwargs: Any) -> discord.Client:
    intents = discord.Intents.default()
    intents.members = True
    client = discord.Client(
        intents=intents,
        chunk_guilds_at_startup=False,
        max_messages=100,
        cache=CacheSettings(**cache_kwargs),
    )
    state = client._connection
    state.user = discord.ClientUser(
        state=state,
        data={
            'id': '999',
            'username': 'bot',
            'discriminator': '0',
            'avatar': None,
            'global_name': None,
            'bot': True,
            'verified': True,
            'mfa_enabled': False,
            'flags': 0,
        },  # type: ignore
    )
    return client


# ---------------------------------------------------------------------------
# TTLDict
# ---------------------------------------------------------------------------


def test_ttldict_basic_ops(clock: Clock) -> None:
    d: TTLDict[int, str] = TTLDict(ttl=10)
    d[1] = 'a'
    d[2] = 'b'
    assert d[1] == 'a' and d.get(2) == 'b' and d.get(3) is None and d.get(3, 'x') == 'x'
    assert 1 in d and len(d) == 2 and list(d) == [1, 2] and list(d.values()) == ['a', 'b']
    assert d.pop(1) == 'a'
    with pytest.raises(KeyError):
        d.pop(1)
    assert d.pop(1, None) is None
    del d[2]
    with pytest.raises(KeyError):
        del d[2]
    assert len(d) == 0 and len(d._ts) == 0
    d[3] = 'c'
    d.clear()
    assert len(d) == 0 and len(d._ts) == 0


def test_ttldict_sliding_ttl_and_sweep(clock: Clock) -> None:
    d: TTLDict[int, str] = TTLDict(ttl=10)
    d[1] = 'a'
    d[2] = 'b'
    clock.advance(6)
    assert d.get(1) == 'a'  # touched at t+6
    clock.advance(6)  # 2 is 12 old, 1 is 6 old
    assert d.sweep() == 1
    assert 1 in d and 2 not in d
    clock.advance(6)
    assert d[1] == 'a'  # __getitem__ touches too
    clock.advance(6)
    assert d.sweep() == 0
    clock.advance(10)
    assert d.sweep() == 1 and len(d) == 0


def test_ttldict_no_ttl_never_sweeps(clock: Clock) -> None:
    d: TTLDict[int, str] = TTLDict()
    d[1] = 'a'
    clock.advance(10_000)
    assert d.sweep() == 0 and d[1] == 'a'


def test_ttldict_max_size_trims_oldest_touched(clock: Clock) -> None:
    d: TTLDict[int, str] = TTLDict(max_size=2)
    d[1] = 'a'
    d[2] = 'b'
    assert d.get(1) == 'a'  # 1 is now most recently used
    d[3] = 'c'
    assert 2 not in d and 1 in d and 3 in d and len(d._ts) == 2


def test_ttldict_pin_survives_sweep_and_trim(clock: Clock) -> None:
    d: TTLDict[int, str] = TTLDict(ttl=10, max_size=1, pin=999)
    d[999] = 'me'
    d[1] = 'a'
    d[2] = 'b'
    assert 999 in d and 1 not in d and 2 in d
    clock.advance(100)
    assert d.sweep() == 1
    assert 999 in d and len(d) == 1 and 999 not in d._ts


def test_ttldict_non_sliding_is_fifo(clock: Clock) -> None:
    d: TTLDict[int, str] = TTLDict(ttl=10, max_size=2, sliding=False)
    d[1] = 'a'
    d[2] = 'b'
    assert d.get(1) == 'a' and d[1] == 'a'
    d[3] = 'c'
    assert 1 not in d  # get did not refresh 1
    clock.advance(11)
    assert d.sweep() == 2


def test_ttldict_update_setdefault_copy_invariant(clock: Clock) -> None:
    d: TTLDict[int, str] = TTLDict(ttl=10, pin=0)
    d.update({1: 'a', 2: 'b'}, three='c')  # type: ignore
    assert d.setdefault(1, 'z') == 'a' and d.setdefault(4, 'd') == 'd'
    d[0] = 'pinned'
    assert set(d._ts) == {1, 2, 'three', 4}
    copied = d.copy()
    assert type(copied) is TTLDict and dict(copied) == dict(d) and copied.ttl == 10 and set(copied._ts) == set(d._ts)
    key, _ = d.popitem()
    assert key not in d._ts


# ---------------------------------------------------------------------------
# MessageCache
# ---------------------------------------------------------------------------


def test_message_cache_deque_surface() -> None:
    cache = MessageCache(max_size=3)
    assert not cache and len(cache) == 0
    m1, m2, m3 = fake_message(1), fake_message(2), fake_message(3)
    for m in (m1, m2, m3):
        cache.append(m)  # type: ignore
    assert cache and len(cache) == 3
    assert list(cache) == [m1, m2, m3] and list(reversed(cache)) == [m3, m2, m1]
    assert m2 in cache and fake_message(9) not in cache and cache.get(2) is m2 and cache.get(9) is None
    cache.remove(m2)  # type: ignore
    assert m2 not in cache
    with pytest.raises(ValueError):
        cache.remove(m2)  # type: ignore
    proxy = SequenceProxy(cache)
    assert list(proxy) == [m1, m3] and proxy[0] is m1 and m3 in proxy


def test_message_cache_fifo_without_ttl(clock: Clock) -> None:
    cache = MessageCache(max_size=2)
    cache.append(fake_message(1))  # type: ignore
    cache.append(fake_message(2))  # type: ignore
    cache.get(1)
    cache.append(fake_message(3))  # type: ignore
    assert cache.get(1) is None and cache.get(2) is not None
    clock.advance(10_000)
    assert cache.sweep() == 0


def test_message_cache_ttl_and_remove_if(clock: Clock) -> None:
    cache = MessageCache(max_size=10, ttl=5)
    g1, g2 = object(), object()
    cache.append(fake_message(1, g1))  # type: ignore
    cache.append(fake_message(2, g2))  # type: ignore
    cache.append(fake_message(3, g1))  # type: ignore
    assert cache.remove_if(lambda m: m.guild is g1) == 2
    assert [m.id for m in cache] == [2]
    clock.advance(6)
    assert cache.sweep() == 1 and len(cache) == 0


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def test_settings_validation() -> None:
    with pytest.raises(TypeError):
        CacheSettings(max_loaded_guilds=10)
    with pytest.raises(ValueError):
        CacheSettings(member_ttl=0)
    with pytest.raises(ValueError):
        CacheSettings(sweep_interval=0)
    with pytest.raises(ValueError):
        RedisSettings([])
    s = RedisSettings(['redis://a', 'redis://b'])
    assert s.uri == ['redis://a', 'redis://b'] and s.cluster is True
    assert RedisSettings('redis://a').uri == ['redis://a']
    assert 'member_ttl' in repr(CacheSettings(member_ttl=5))


def test_settings_rejects_redis_ttl_shorter_than_memory_ttl() -> None:
    # member_ttl: Redis defaults to 6h, which is too short for a 1 day in-memory TTL.
    with pytest.raises(ValueError, match='member_ttl'):
        CacheSettings(member_ttl=86400, redis=RedisSettings('redis://x', cluster=False))
    # thread_ttl: Redis defaults to 1h.
    with pytest.raises(ValueError, match='thread_ttl'):
        CacheSettings(thread_ttl=7200, redis=RedisSettings('redis://x', cluster=False))
    # message_ttl: only checked when Redis actually mirrors messages.
    CacheSettings(message_ttl=7200, redis=RedisSettings('redis://x', cluster=False))  # redis message_ttl is None: fine
    with pytest.raises(ValueError, match='message_ttl'):
        CacheSettings(message_ttl=7200, redis=RedisSettings('redis://x', cluster=False, message_ttl=3600))
    # equal TTLs and an explicitly longer Redis TTL are both fine.
    CacheSettings(member_ttl=21600, redis=RedisSettings('redis://x', cluster=False))
    CacheSettings(member_ttl=3600, redis=RedisSettings('redis://x', cluster=False, member_ttl=21600))


# ---------------------------------------------------------------------------
# Redis mirror table
# ---------------------------------------------------------------------------


async def run_mirror(rc: RedisCache, event: str, data: Dict[str, Any]) -> FakeRedis:
    pipe = rc.pipeline()
    MIRROR[event](rc, pipe, data)
    await pipe.execute()
    return rc.client


@pytest.mark.asyncio
async def test_mirror_guild_create_and_hash_tags() -> None:
    rc = redis_cache(thread_ttl=77)
    payload = dict(
        GUILD_CREATE,
        threads=[
            {'id': '30', 'guild_id': '1', 'type': 11, 'name': 't', 'parent_id': '20', 'thread_metadata': {'archived': False}}
        ],
    )
    fake = await run_mirror(rc, 'GUILD_CREATE', payload)
    assert fake.string('guild:{1}')['name'] == 'guild' and 'members' not in fake.string('guild:{1}')
    assert set(fake.hash('guild:{1}:roles')) == {'1'} and set(fake.hash('guild:{1}:channels')) == {'20'}
    assert set(fake.hash('guild:{1}:threads')) == {'30'} and fake.ttls['guild:{1}:threads'] == 77
    assert fake.string('member:{1}:2')['user']['id'] == '2' and fake.string('user:2')['username'] == 'user'
    for key in fake.data:
        if key.startswith(('guild:', 'member:')):
            assert '{1}' in key, key
    assert fake.ttls['guild:{1}'] == rc.settings.guild_ttl

    bundle = await rc.get_guild_bundle(1)
    assert bundle is not None
    base, roles, channels, threads, emojis, stickers = bundle
    assert base['id'] == '1' and '1' in roles and '20' in channels and '30' in threads and emojis == {} and stickers == {}
    assert await rc.get_member(1, 2) is not None and await rc.get_user(2) is not None

    await run_mirror(rc, 'GUILD_DELETE', {'id': '1'})
    assert await rc.get_guild_bundle(1) is None and not any(k.startswith('guild:') for k in fake.data)


@pytest.mark.asyncio
async def test_mirror_guild_create_skips_unavailable_and_members_without_member_ttl() -> None:
    rc = redis_cache(member_ttl=None)
    fake = await run_mirror(rc, 'GUILD_CREATE', {'id': '5', 'unavailable': True})
    assert fake.data == {}
    fake = await run_mirror(rc, 'GUILD_CREATE', GUILD_CREATE)
    assert not any(k.startswith(('member:', 'user:')) for k in fake.data)


@pytest.mark.asyncio
async def test_mirror_sub_entities() -> None:
    rc = redis_cache()
    fake = await run_mirror(rc, 'GUILD_CREATE', GUILD_CREATE)
    await run_mirror(rc, 'CHANNEL_CREATE', {'id': '21', 'guild_id': '1', 'type': 0, 'name': 'new'})
    await run_mirror(rc, 'CHANNEL_DELETE', {'id': '20', 'guild_id': '1', 'type': 0})
    assert set(fake.hash('guild:{1}:channels')) == {'21'}
    await run_mirror(rc, 'CHANNEL_CREATE', {'id': '99', 'type': 1})  # DM: ignored
    assert 'guild:{None}:channels' not in fake.data

    await run_mirror(rc, 'GUILD_ROLE_CREATE', {'guild_id': '1', 'role': {'id': '7', 'name': 'r'}})
    await run_mirror(rc, 'GUILD_ROLE_DELETE', {'guild_id': '1', 'role_id': '1'})
    assert set(fake.hash('guild:{1}:roles')) == {'7'}
    await run_mirror(rc, 'GUILD_UPDATE', {'id': '1', 'name': 'renamed', 'roles': [{'id': '8', 'name': 'x'}]})
    assert fake.string('guild:{1}')['name'] == 'renamed' and set(fake.hash('guild:{1}:roles')) == {'8'}

    thread = {'id': '30', 'guild_id': '1', 'thread_metadata': {'archived': False}}
    await run_mirror(rc, 'THREAD_CREATE', thread)
    assert '30' in fake.hash('guild:{1}:threads')
    await run_mirror(rc, 'THREAD_UPDATE', dict(thread, thread_metadata={'archived': True}))
    assert '30' not in fake.hash('guild:{1}:threads')
    await run_mirror(rc, 'THREAD_LIST_SYNC', {'guild_id': '1', 'threads': [dict(thread, id='31')]})
    assert set(fake.hash('guild:{1}:threads')) == {'31'}
    await run_mirror(rc, 'THREAD_LIST_SYNC', {'guild_id': '1', 'channel_ids': ['20'], 'threads': [dict(thread, id='32')]})
    assert set(fake.hash('guild:{1}:threads')) == {'31', '32'}
    await run_mirror(rc, 'THREAD_DELETE', {'id': '31', 'guild_id': '1'})
    assert set(fake.hash('guild:{1}:threads')) == {'32'}

    member = {'guild_id': '1', 'user': {'id': '3', 'username': 'm'}, 'roles': []}
    await run_mirror(rc, 'GUILD_MEMBER_ADD', member)
    assert fake.string('member:{1}:3')['user']['id'] == '3'
    await run_mirror(rc, 'GUILD_MEMBERS_CHUNK', {'guild_id': '1', 'members': [dict(member, user={'id': '4'})]})
    assert 'member:{1}:4' in fake.data
    await run_mirror(rc, 'GUILD_MEMBER_REMOVE', member)
    assert 'member:{1}:3' not in fake.data and 'user:3' in fake.data

    await run_mirror(rc, 'GUILD_EMOJIS_UPDATE', {'guild_id': '1', 'emojis': [{'id': '50', 'name': 'e'}]})
    await run_mirror(rc, 'GUILD_STICKERS_UPDATE', {'guild_id': '1', 'stickers': [{'id': '60', 'name': 's'}]})
    assert set(fake.hash('guild:{1}:emojis')) == {'50'} and set(fake.hash('guild:{1}:stickers')) == {'60'}


@pytest.mark.asyncio
async def test_mirror_messages_merge_partial_update() -> None:
    rc = redis_cache(message_ttl=120)
    fake = await run_mirror(rc, 'MESSAGE_CREATE', MESSAGE_CREATE)
    assert fake.ttls['message:{20}:10'] == 120
    await run_mirror(rc, 'MESSAGE_UPDATE', {'id': '10', 'channel_id': '20', 'content': 'edited'})
    data = await rc.get_message(20, 10)
    assert data is not None and data['content'] == 'edited' and data['author']['id'] == '2'
    assert await rc.get_message(20, 11) is None
    await run_mirror(rc, 'MESSAGE_DELETE', {'id': '10', 'channel_id': '20'})
    assert await rc.get_message(20, 10) is None
    await run_mirror(rc, 'MESSAGE_CREATE', MESSAGE_CREATE)
    await run_mirror(rc, 'MESSAGE_DELETE_BULK', {'ids': ['10', '11'], 'channel_id': '20'})
    assert await rc.get_message(20, 10) is None

    without = redis_cache()
    fake = await run_mirror(without, 'MESSAGE_CREATE', MESSAGE_CREATE)
    assert fake.data == {}


@pytest.mark.asyncio
async def test_redis_connect_selects_client(monkeypatch: pytest.MonkeyPatch) -> None:
    import redis.asyncio as aioredis

    calls: List[tuple] = []

    class Bad:
        async def ping(self) -> None:
            raise ConnectionError('nope')

    def cluster_from_url(uri: str, **kwargs: Any) -> Any:
        calls.append(('cluster', uri, kwargs))
        return Bad() if uri.endswith('dead') else FakeRedis()

    def single_from_url(uri: str, **kwargs: Any) -> Any:
        calls.append(('single', uri, kwargs))
        return FakeRedis()

    monkeypatch.setattr(aioredis.RedisCluster, 'from_url', staticmethod(cluster_from_url))
    monkeypatch.setattr(aioredis.Redis, 'from_url', staticmethod(single_from_url))

    rc = RedisCache(RedisSettings(['redis://dead', 'redis://alive'], read_from_replicas=True, max_connections=8))
    await rc.connect()
    assert rc.connected and [c[0:2] for c in calls] == [('cluster', 'redis://dead'), ('cluster', 'redis://alive')]
    assert calls[1][2]['read_from_replicas'] is True and calls[1][2]['max_connections'] == 8
    await rc.close()
    assert not rc.connected

    calls.clear()
    rc = RedisCache(RedisSettings('redis://one', cluster=False))
    await rc.connect()
    assert calls == [('single', 'redis://one', {'decode_responses': True})]

    with pytest.raises(RuntimeError):
        await RedisCache(RedisSettings('redis://dead')).connect()


@pytest.mark.asyncio
async def test_redis_provided_client_is_shared_not_closed() -> None:
    fake = FakeRedis()
    rc = RedisCache(RedisSettings(client=fake))
    await rc.connect()
    assert rc.client is fake
    await rc.close()
    assert not fake.closed and not rc.connected

    holder: Dict[str, Any] = {}
    rc = RedisCache(RedisSettings(client=lambda: holder.get('client')))
    with pytest.raises(RuntimeError):
        await rc.connect()
    holder['client'] = fake
    await rc.connect()
    assert rc.client is fake

    with pytest.raises(TypeError):
        RedisSettings()
    with pytest.raises(TypeError):
        RedisSettings('redis://x', client=fake)

    # end to end through the manager: writes land on the shared client
    m = manager(CacheSettings(redis=RedisSettings(client=fake)))
    await m.start()
    await m.pre_event('GUILD_ROLE_CREATE', {'guild_id': '1', 'role': {'id': '1'}})
    await m.flush()
    assert '1' in fake.hash('guild:{1}:roles')
    await m.close()
    assert not fake.closed


# ---------------------------------------------------------------------------
# CacheManager
# ---------------------------------------------------------------------------


def test_manager_defaults_are_upstream() -> None:
    m = manager(None)
    assert m.pre_event is None and m.redis is None and not m._sweeps_needed
    assert type(m.members()) is dict and type(m.threads()) is dict
    assert isinstance(m.messages(), MessageCache) and manager(None, max_messages=None).messages() is None
    guild = fake_guild(1, members={5: 'm'})
    m.guild_added(guild)  # type: ignore
    assert type(guild._members) is dict


def test_manager_swaps_containers(clock: Clock) -> None:
    m = manager(CacheSettings(member_ttl=10, thread_max=5))
    assert m.pre_event is None
    guild = fake_guild(1, members={5: 'm', 999: 'me'})
    m.guild_added(guild)  # type: ignore
    assert type(guild._members) is TTLDict and guild._members._pin == 999 and guild._members.ttl == 10
    assert type(guild._threads) is TTLDict and guild._threads.max_size == 5
    assert dict(guild._members) == {5: 'm', 999: 'me'} and 999 not in guild._members._ts


@pytest.mark.asyncio
async def test_manager_sweep_once(clock: Clock) -> None:
    m = manager(CacheSettings(member_ttl=10, message_ttl=10, sweep_interval=1))
    state = m._state
    for guild_id in range(1, 1202):
        guild = fake_guild(guild_id, members={5: 'm', 999: 'me'})
        m.guild_added(guild)  # type: ignore
        state._guilds[guild_id] = guild
    state._messages.append(fake_message(1))
    clock.advance(11)
    state._guilds[1]._members.get(5)  # touched, survives
    assert await m.sweep_once() == 1200 + 1
    assert 5 in state._guilds[1]._members and 5 not in state._guilds[2]._members and 999 in state._guilds[2]._members
    assert len(state._messages) == 0


@pytest.mark.asyncio
async def test_manager_lru_unload_and_touch(clock: Clock) -> None:
    m = manager(CacheSettings(member_ttl=10, max_loaded_guilds=2, redis=RedisSettings('redis://x', cluster=False)))
    state = m._state
    guilds = {}
    for guild_id in (1, 2, 3):
        guild = fake_guild(guild_id, members={5: 'm', 999: 'me'})
        state._guilds[guild_id] = guilds[guild_id] = guild
        m.guild_added(guild)  # type: ignore
    assert list(m._lru) == [2, 3] and not m.is_loaded(guilds[1]) and m.is_loaded(guilds[2])  # type: ignore
    assert guilds[1]._channels == {} and guilds[1]._roles == {} and dict(guilds[1]._members) == {999: 'me'}
    assert type(guilds[1]._members) is TTLDict

    m.touch(2)
    assert list(m._lru) == [3, 2]  # type: ignore
    m.mark_loaded(4)
    assert list(m._lru) == [2, 4] and 3 in m._unloaded  # type: ignore

    stub = fake_guild(9, unavailable=True)
    m.guild_added(stub)  # type: ignore
    assert 9 not in m._lru  # type: ignore

    m.forget(2)
    assert 2 not in m._lru  # type: ignore
    m.reset()
    assert not m._lru and not m._unloaded  # type: ignore


@pytest.mark.asyncio
async def test_manager_writer_batches_flushes_and_breaks_circuit(clock: Clock, monkeypatch: pytest.MonkeyPatch) -> None:
    m = manager(CacheSettings(redis=RedisSettings('redis://x', cluster=False, batch_size=2)))
    assert m.pre_event is not None
    fake = await connect_fake(m)
    assert m._writer is not None and m._sweeper is None

    for role_id in range(5):
        await m.pre_event('GUILD_ROLE_CREATE', {'guild_id': '1', 'role': {'id': str(role_id)}})
    await m.pre_event('MESSAGE_CREATE', MESSAGE_CREATE)  # not mirrored: message_ttl is None
    await m.flush()
    assert set(fake.hash('guild:{1}:roles')) == {'0', '1', '2', '3', '4'} and 'message:{20}:10' not in fake.data

    fake.fail = True
    await m.pre_event('GUILD_ROLE_CREATE', {'guild_id': '1', 'role': {'id': '9'}})
    await m.flush()
    assert m._redis_down_until > clock.now
    fake.fail = False
    await m.pre_event('GUILD_ROLE_CREATE', {'guild_id': '1', 'role': {'id': '10'}})
    await m.flush()
    assert '10' not in fake.hash('guild:{1}:roles')  # dropped while the circuit is open
    clock.advance(31)
    await m.pre_event('GUILD_ROLE_CREATE', {'guild_id': '1', 'role': {'id': '11'}})
    await m.flush()
    assert '11' in fake.hash('guild:{1}:roles')

    await m.pre_event('GUILD_ROLE_CREATE', {'guild_id': '1', 'role': 'malformed'})  # never raises
    await m.flush()

    await m.close()
    assert fake.closed and m._writer is None and m._queue is None


@pytest.mark.asyncio
async def test_manager_start_is_idempotent_and_close_stops_sweeper() -> None:
    m = manager(CacheSettings(member_ttl=1, sweep_interval=1000))
    await m.start()
    sweeper = m._sweeper
    assert sweeper is not None and not sweeper.done()
    await m.start()
    assert m._sweeper is sweeper
    await m.close()
    assert sweeper.cancelled() or sweeper.done()


# ---------------------------------------------------------------------------
# Integration through a real Client / ConnectionState
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_member_and_message_ttl(clock: Clock) -> None:
    client = make_client(member_ttl=10, message_ttl=10, sweep_interval=10_000)
    state = client._connection
    assert state._cache.pre_event is None

    state.parse_guild_create(dict(GUILD_CREATE))  # type: ignore
    guild = client.get_guild(1)
    assert guild is not None and type(guild._members) is TTLDict and guild.get_member(2) is not None

    state.parse_message_create(dict(MESSAGE_CREATE))  # type: ignore
    assert isinstance(state._messages, MessageCache) and state._get_message(10) is not None
    assert len(client.cached_messages) == 1 and state._get_message(10) in client.cached_messages

    clock.advance(11)
    assert await state._cache.sweep_once() == 2
    assert guild.get_member(2) is None and state._get_message(10) is None and len(client.cached_messages) == 0

    state.parse_message_create(dict(MESSAGE_CREATE))  # type: ignore
    state.parse_guild_delete({'id': '1'})  # type: ignore
    assert client.get_guild(1) is None and state._get_message(10) is None

    state.clear()
    assert isinstance(state._messages, MessageCache)
    await state._cache.close()


@pytest.mark.asyncio
async def test_client_guild_unload_and_reload_from_redis(clock: Clock) -> None:
    client = make_client(member_ttl=10, max_loaded_guilds=1, redis=RedisSettings('redis://x', cluster=False))
    state = client._connection
    cache = state._cache
    fake = await connect_fake(cache)

    async def gateway(event: str, data: Dict[str, Any]) -> None:
        await cache.pre_event(event, data)
        state.parsers[event](data)

    await gateway('GUILD_CREATE', dict(GUILD_CREATE))
    second = dict(GUILD_CREATE, id='2', channels=[dict(GUILD_CREATE['channels'][0], id='40')])
    await gateway('GUILD_CREATE', second)
    await cache.flush()

    one, two = client.get_guild(1), client.get_guild(2)
    assert one is not None and two is not None
    assert not cache_mod.is_guild_loaded(one) and cache_mod.is_guild_loaded(two)
    assert one.get_channel(20) is None and one.get_role(1) is None
    # the bot's own member survives unloading
    one._add_member(discord.Member._from_client_user(user=state.user, guild=one, state=state))  # type: ignore
    cache._unload(one)
    assert one.me is not None

    await gateway('MESSAGE_CREATE', dict(MESSAGE_CREATE))
    assert cache_mod.is_guild_loaded(one) and not cache_mod.is_guild_loaded(two)
    assert isinstance(one.get_channel(20), discord.TextChannel) and one.get_role(1) is not None
    message = state._get_message(10)
    assert message is not None and isinstance(message.channel, discord.TextChannel) and one.me is not None

    # Redis lost the guild: fall back to the API
    fake.data = {k: v for k, v in fake.data.items() if not k.startswith('guild:{2}')}
    calls: List[Any] = []

    async def get_guild(guild_id: int, *, with_counts: bool = True) -> Dict[str, Any]:
        calls.append(guild_id)
        return {k: v for k, v in second.items() if k not in ('channels', 'members')}

    async def get_all_guild_channels(guild_id: int) -> List[Dict[str, Any]]:
        return list(second['channels'])

    state.http.get_guild = get_guild  # type: ignore
    state.http.get_all_guild_channels = get_all_guild_channels  # type: ignore
    await gateway('MESSAGE_CREATE', dict(MESSAGE_CREATE, id='11', channel_id='40', guild_id='2'))
    assert calls == [2] and two.get_channel(40) is not None and not cache_mod.is_guild_loaded(one)
    await cache.flush()
    assert 'guild:{2}' in fake.data  # re-warmed

    assert await cache_mod.load_guild(two) is two
    await cache.close()


@pytest.mark.asyncio
async def test_client_cached_lookups_fall_back_to_redis(clock: Clock) -> None:
    client = make_client(redis=RedisSettings('redis://x', cluster=False, message_ttl=60))
    state = client._connection
    cache = state._cache
    fake = await connect_fake(cache)
    state.parse_guild_create(dict(GUILD_CREATE))  # type: ignore
    guild = client.get_guild(1)
    assert guild is not None

    await cache.pre_event(
        'GUILD_MEMBER_ADD',
        {
            'guild_id': '1',
            'user': {'id': '3', 'username': 'three', 'discriminator': '0', 'avatar': None, 'global_name': None},
            'roles': [],
            'joined_at': None,
            'deaf': False,
            'mute': False,
            'flags': 0,
        },
    )
    await cache.pre_event('MESSAGE_CREATE', dict(MESSAGE_CREATE))
    await cache.flush()

    member = await cache_mod.get_cached_member(guild, 3)
    assert member is not None and member.name == 'three' and guild.get_member(3) is None
    user = await cache_mod.get_cached_user(client, 3)
    assert user is not None and user.name == 'three'
    assert await cache_mod.get_cached_user(client, 4) is None
    message = await cache_mod.get_cached_message(client, 20, 10)
    assert message is not None and message.content == 'hello' and message.guild is guild
    assert await cache_mod.get_cached_message(client, 20, 11) is None

    fake.fail = True
    assert await cache_mod.get_cached_member(guild, 4) is None and cache._redis_down_until > clock.now
    await cache.close()


def capture_dispatch(state: Any) -> List[tuple]:
    events: List[tuple] = []

    def dispatch(event: str, *args: Any) -> None:
        events.append((event, *args))

    state.dispatch = dispatch
    return events


@pytest.mark.asyncio
async def test_redis_hydrates_evicted_messages_and_members_before_events(clock: Clock) -> None:
    client = make_client(
        member_ttl=10, message_ttl=10, sweep_interval=10_000, redis=RedisSettings('redis://x', cluster=False, message_ttl=60)
    )
    state = client._connection
    cache = state._cache
    fake = await connect_fake(cache)
    events = capture_dispatch(state)

    async def gateway(event: str, data: Dict[str, Any]) -> None:
        await cache.pre_event(event, data)
        state.parsers[event](data)

    await gateway('GUILD_CREATE', dict(GUILD_CREATE))
    await gateway('MESSAGE_CREATE', dict(MESSAGE_CREATE))
    await gateway('MESSAGE_CREATE', dict(MESSAGE_CREATE, id='11', content='second'))
    guild = client.get_guild(1)
    assert guild is not None and guild.get_member(2) is not None and state._get_message(10) is not None

    # everything ages out of memory, Redis still has it
    clock.advance(11)
    await cache.sweep_once()
    assert guild.get_member(2) is None and state._get_message(10) is None and state._get_message(11) is None
    events.clear()

    # message delete: on_message_delete fires with the full message, Redis copy is removed too
    await gateway('MESSAGE_DELETE', {'id': '10', 'channel_id': '20', 'guild_id': '1'})
    names = [e[0] for e in events]
    assert names == ['raw_message_delete', 'message_delete']
    assert events[0][1].cached_message is not None and events[1][1].content == 'hello'
    assert isinstance(events[1][1].author, discord.Member) and events[1][1].guild is guild
    assert state._get_message(10) is None
    await cache.flush()
    assert 'message:{20}:10' not in fake.data
    events.clear()

    # message edit: before/after both available
    await gateway('MESSAGE_UPDATE', dict(MESSAGE_CREATE, id='11', content='edited'))
    names = [e[0] for e in events]
    assert names == ['raw_message_edit', 'message_edit']
    before, after = events[1][1], events[1][2]
    assert before.content == 'second' and after.content == 'edited' and state._get_message(11) is not None
    events.clear()

    # reaction without a member in the payload: message and member are both restored
    clock.advance(11)
    await cache.sweep_once()
    assert guild.get_member(2) is None and state._get_message(11) is None
    await gateway(
        'MESSAGE_REACTION_REMOVE',
        {
            'message_id': '11',
            'channel_id': '20',
            'guild_id': '1',
            'user_id': '2',
            'emoji': {'id': None, 'name': 'x'},
            'type': 0,
            'burst': False,
        },
    )
    assert [e[0] for e in events] == ['raw_reaction_remove']  # no reaction to remove yet, but no crash
    assert guild.get_member(2) is not None and state._get_message(11) is not None
    events.clear()

    # member update after eviction: on_member_update fires with the old state
    clock.advance(11)
    await cache.sweep_once()
    assert guild.get_member(2) is None
    await gateway(
        'GUILD_MEMBER_UPDATE',
        {
            'guild_id': '1',
            'user': MESSAGE_CREATE['author'],
            'roles': [],
            'nick': 'renamed',
            'joined_at': '2024-01-01T00:00:00+00:00',
            'flags': 0,
        },
    )
    assert [e[0] for e in events] == ['member_update']
    old, new = events[0][1], events[0][2]
    assert old.nick is None and new.nick == 'renamed' and guild.get_member(2) is new
    events.clear()

    # bulk delete after eviction
    clock.advance(11)
    await cache.sweep_once()
    await gateway('MESSAGE_DELETE_BULK', {'ids': ['11', '12'], 'channel_id': '20', 'guild_id': '1'})
    names = [e[0] for e in events]
    assert names == ['raw_bulk_message_delete', 'bulk_message_delete']
    assert [m.id for m in events[1][1]] == [11] and state._get_message(11) is None
    events.clear()

    # member remove after eviction: on_member_remove fires with the Member and Redis forgets it
    clock.advance(11)
    await cache.sweep_once()
    assert guild.get_member(2) is None
    await gateway('GUILD_MEMBER_REMOVE', {'guild_id': '1', 'user': MESSAGE_CREATE['author']})
    names = [e[0] for e in events]
    assert names == ['member_remove', 'raw_member_remove'] and isinstance(events[0][1], discord.Member)
    assert guild.get_member(2) is None
    await cache.flush()
    assert 'member:{1}:2' not in fake.data
    events.clear()

    # payload that already carries the member does not touch Redis
    executed = fake.executed
    await gateway(
        'MESSAGE_REACTION_ADD',
        {
            'message_id': '99',
            'channel_id': '20',
            'guild_id': '1',
            'user_id': '3',
            'emoji': {'id': None, 'name': 'x'},
            'type': 0,
            'burst': False,
            'member': {
                'user': {'id': '3', 'username': 'u3', 'discriminator': '0', 'avatar': None, 'global_name': None},
                'roles': [],
                'joined_at': None,
                'deaf': False,
                'mute': False,
                'flags': 0,
            },
        },
    )
    await cache.flush()
    assert fake.executed == executed + 1  # only the message lookup, no member GET
    events.clear()

    # Redis down: events still fire in their raw form, nothing raises
    fake.fail = True
    await gateway('MESSAGE_DELETE', {'id': '11', 'channel_id': '20', 'guild_id': '1'})
    assert [e[0] for e in events] == ['raw_message_delete'] and cache._redis_down_until > clock.now
    await cache.close()


@pytest.mark.asyncio
async def test_redis_member_hydration_respects_member_cache_flags() -> None:
    client = make_client(member_ttl=10, redis=RedisSettings('redis://x', cluster=False))
    state = client._connection
    cache = state._cache
    fake = await connect_fake(cache)
    state.parse_guild_create(dict(GUILD_CREATE))  # type: ignore
    await cache.pre_event('GUILD_CREATE', dict(GUILD_CREATE))
    await cache.flush()
    assert 'member:{1}:2' in fake.data
    guild = client.get_guild(1)
    assert guild is not None
    guild._remove_member(guild.get_member(2))  # type: ignore

    state.member_cache_flags = discord.MemberCacheFlags.none()
    await cache.pre_event(
        'GUILD_MEMBER_UPDATE', {'guild_id': '1', 'user': MESSAGE_CREATE['author'], 'roles': [], 'flags': 0}
    )
    assert guild.get_member(2) is None  # upstream would not have cached it either

    state.member_cache_flags = discord.MemberCacheFlags.all()
    await cache.pre_event(
        'GUILD_MEMBER_UPDATE', {'guild_id': '1', 'user': MESSAGE_CREATE['author'], 'roles': [], 'flags': 0}
    )
    assert guild.get_member(2) is not None
    await cache.close()
