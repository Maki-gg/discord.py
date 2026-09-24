"""Tests for the Maki fork cache layer (discord/cache.py)."""

from __future__ import annotations

import ast
import pathlib
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

import discord
from discord import cache as cache_mod
from discord.cache import CacheManager, CacheSettings, MessageCache, TTLDict
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
    with pytest.raises(ValueError):
        CacheSettings(member_ttl=0)
    with pytest.raises(ValueError):
        CacheSettings(member_max=-1)
    with pytest.raises(ValueError):
        CacheSettings(sweep_interval=0)
    with pytest.raises(TypeError):
        CacheSettings(redis=None)  # type: ignore  # removed option must not be silently accepted
    with pytest.raises(TypeError):
        CacheSettings(thread_ttl=10)  # type: ignore
    assert 'member_ttl' in repr(CacheSettings(member_ttl=5))


# ---------------------------------------------------------------------------
# CacheManager
# ---------------------------------------------------------------------------


def test_manager_defaults_are_upstream() -> None:
    m = manager(None)
    assert not m._sweeps_needed
    assert type(m.members()) is dict
    assert isinstance(m.messages(), MessageCache) and manager(None, max_messages=None).messages() is None
    guild = fake_guild(1, members={5: 'm'})
    m.guild_added(guild)  # type: ignore
    assert type(guild._members) is dict


def test_manager_swaps_member_container(clock: Clock) -> None:
    m = manager(CacheSettings(member_ttl=10, member_max=50))
    guild = fake_guild(1, members={5: 'm', 999: 'me'})
    m.guild_added(guild)  # type: ignore
    assert type(guild._members) is TTLDict and guild._members._pin == 999
    assert guild._members.ttl == 10 and guild._members.max_size == 50
    assert dict(guild._members) == {5: 'm', 999: 'me'} and 999 not in guild._members._ts
    assert type(guild._threads) is dict  # threads are never bounded


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
async def test_manager_start_is_idempotent_and_close_stops_sweeper() -> None:
    m = manager(CacheSettings(member_ttl=1, sweep_interval=1000))
    await m.start()
    sweeper = m._sweeper
    assert sweeper is not None and not sweeper.done()
    await m.start()
    assert m._sweeper is sweeper
    await m.close()
    assert sweeper.cancelled() or sweeper.done()


@pytest.mark.asyncio
async def test_manager_without_settings_starts_no_task() -> None:
    m = manager(None)
    await m.start()
    assert m._sweeper is None
    await m.close()


# ---------------------------------------------------------------------------
# Integration through a real Client / ConnectionState
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_member_and_message_ttl(clock: Clock) -> None:
    client = make_client(member_ttl=10, message_ttl=10, sweep_interval=10_000)
    state = client._connection

    state.parse_guild_create(dict(GUILD_CREATE))  # type: ignore
    guild = client.get_guild(1)
    assert guild is not None and type(guild._members) is TTLDict and guild.get_member(2) is not None

    state.parse_message_create(dict(MESSAGE_CREATE))  # type: ignore
    assert isinstance(state._messages, MessageCache) and state._get_message(10) is not None
    assert len(client.cached_messages) == 1 and state._get_message(10) in client.cached_messages

    clock.advance(11)
    assert await state._cache.sweep_once() == 2
    assert guild.get_member(2) is None and state._get_message(10) is None and len(client.cached_messages) == 0

    # evicted objects behave as never cached: raw events only, no crash
    state.parse_message_delete({'id': '10', 'channel_id': '20', 'guild_id': '1'})  # type: ignore
    state.parse_guild_member_update(  # type: ignore
        {'guild_id': '1', 'user': MESSAGE_CREATE['author'], 'roles': [], 'flags': 0, 'joined_at': None}
    )

    state.parse_message_create(dict(MESSAGE_CREATE))  # type: ignore
    state.parse_guild_delete({'id': '1'})  # type: ignore
    assert client.get_guild(1) is None and state._get_message(10) is None

    state.clear()
    assert isinstance(state._messages, MessageCache)
    await state._cache.close()


@pytest.mark.asyncio
async def test_client_without_cache_option_is_upstream() -> None:
    client = discord.Client(intents=discord.Intents.default(), max_messages=5)
    state = client._connection
    assert type(state._cache.members()) is dict and not state._cache._sweeps_needed
    assert isinstance(state._messages, MessageCache) and state._messages._d.ttl is None
    await state._cache.close()


# ---------------------------------------------------------------------------
# Guards for upstream merges (see "Updating from upstream" in README.rst)
# ---------------------------------------------------------------------------

REPO = pathlib.Path(__file__).resolve().parent.parent
FORK_MARKER = '# Maki fork: cache layer'
# Upstream files that carry hook lines, with the number of marked lines each must have.
FORK_MARKER_COUNTS = {
    'discord/state.py': 7,
    'discord/client.py': 3,
    'discord/__init__.py': 1,
}
# Upstream files that must stay byte-identical to upstream.
UNTOUCHED_UPSTREAM = ('discord/guild.py', 'discord/gateway.py', 'discord/ext/commands/bot.py')


def test_fork_marker_count() -> None:
    for relative, expected in FORK_MARKER_COUNTS.items():
        found = (REPO / relative).read_text().count(FORK_MARKER)
        assert found == expected, f'{relative}: expected {expected} fork markers, found {found}'
    for relative in UNTOUCHED_UPSTREAM:
        assert FORK_MARKER not in (REPO / relative).read_text(), f'{relative} must stay identical to upstream'


def test_message_cache_supports_every_state_usage() -> None:
    """state.py uses MessageCache where upstream uses a deque. Any new attribute upstream
    starts calling on self._messages must exist on MessageCache, or a merge breaks at runtime."""
    tree = ast.parse((REPO / 'discord/state.py').read_text())
    used = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Attribute)
            and node.value.attr == '_messages'
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == 'self'
        ):
            used.add(node.attr)
    assert used, 'no self._messages.<attr> usage found; the scan is broken'
    missing = {name for name in used if not hasattr(MessageCache, name)}
    assert not missing, f'state.py calls these on self._messages but MessageCache lacks them: {missing}'
