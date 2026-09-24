"""
Maki fork addition: cache layer for discord.py.

This module is not part of upstream discord.py. It provides the TTL containers
that bound the member and message caches, plus the manager that owns them and
the background sweeper. The upstream files only carry a handful of hook lines
marked ``# Maki fork: cache layer``.

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
import logging
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

if TYPE_CHECKING:
    from .guild import Guild
    from .member import Member
    from .message import Message
    from .state import ConnectionState

__all__ = ('CacheSettings',)

_log = logging.getLogger(__name__)

K = TypeVar('K')
V = TypeVar('V')

# Module attribute so tests can monkeypatch the clock.
_now = time.monotonic
_MISSING: Any = object()
_SWEEP_YIELD_EVERY = 500


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


class CacheSettings:
    """Fork addition, not part of upstream discord.py.

    Controls cache eviction. Pass an instance as the ``cache`` keyword argument of
    :class:`Client`, :class:`AutoShardedClient`, :class:`~discord.ext.commands.Bot`
    or :class:`~discord.ext.commands.AutoShardedBot`. Every value defaults to
    "never evict", which is upstream behaviour.

    Evicting a member or message means events about it behave as they would upstream
    for anything that was never cached: the raw event still fires, and the richer one
    that needs the cached object does not.

    Parameters
    -----------
    member_ttl: Optional[:class:`float`]
        Seconds a cached member may go untouched before it is evicted from its guild.
        Members are touched by every event and lookup that resolves them. The bot's own
        member is never evicted.
    member_max: Optional[:class:`int`]
        Maximum cached members per guild. The least recently touched member is evicted first.
    message_ttl: Optional[:class:`float`]
        Seconds a cached message may go untouched before it is evicted. The size bound stays
        ``max_messages`` on the client, and on a busy bot that count is usually what evicts
        first. Without a TTL the message cache keeps upstream first-in first-out behaviour.
    sweep_interval: :class:`float`
        Seconds between eviction passes. Defaults to ``300``.
    """

    __slots__ = (
        'member_ttl',
        'member_max',
        'message_ttl',
        'sweep_interval',
    )

    def __init__(
        self,
        *,
        member_ttl: Optional[float] = None,
        member_max: Optional[int] = None,
        message_ttl: Optional[float] = None,
        sweep_interval: float = 300.0,
    ) -> None:
        for name, value in (
            ('member_ttl', member_ttl),
            ('member_max', member_max),
            ('message_ttl', message_ttl),
        ):
            if value is not None and value <= 0:
                raise ValueError(f'{name} must be positive or None')
        if sweep_interval <= 0:
            raise ValueError('sweep_interval must be positive')

        self.member_ttl: Optional[float] = member_ttl
        self.member_max: Optional[int] = member_max
        self.message_ttl: Optional[float] = message_ttl
        self.sweep_interval: float = sweep_interval

    def __repr__(self) -> str:
        return (
            f'<CacheSettings member_ttl={self.member_ttl} member_max={self.member_max} '
            f'message_ttl={self.message_ttl} sweep_interval={self.sweep_interval}>'
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
# Manager
# ---------------------------------------------------------------------------


class CacheManager:
    """Owns the cache containers and the background sweeper. One instance lives on
    ``ConnectionState._cache``."""

    def __init__(self, state: ConnectionState, settings: Optional[CacheSettings]) -> None:
        self._state: ConnectionState = state
        self.settings: CacheSettings = settings if settings is not None else CacheSettings()
        s = self.settings
        self._members_bounded: bool = s.member_ttl is not None or s.member_max is not None
        self._sweeps_needed: bool = self._members_bounded or s.message_ttl is not None
        self._sweeper: Optional[asyncio.Task[None]] = None

    # Container factories

    def members(self) -> Dict[int, Member]:
        if not self._members_bounded:
            return {}
        s = self.settings
        return TTLDict(ttl=s.member_ttl, max_size=s.member_max, pin=self._state.self_id)

    def messages(self) -> Optional[MessageCache]:
        max_messages = self._state.max_messages
        if max_messages is None:
            return None
        return MessageCache(max_messages, self.settings.message_ttl)

    # Hooks called from ConnectionState

    def guild_added(self, guild: Guild) -> None:
        """Swaps in a bounded member container for a guild entering the cache."""
        if self._members_bounded and type(guild._members) is not TTLDict:
            members = self.members()
            members.update(guild._members)
            guild._members = members

    # Lifecycle

    async def start(self) -> None:
        """Starts the sweeper. Idempotent, so reconnects and ``Client.clear`` are safe."""
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

    # Sweeper

    async def sweep_once(self) -> int:
        """Evicts everything past its TTL. Yields periodically so a large guild count
        does not block the event loop."""
        now = _now()
        removed = 0
        count = 0
        for guild in list(self._state._guilds.values()):
            members = guild._members
            if type(members) is TTLDict:
                removed += members.sweep(now)
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
