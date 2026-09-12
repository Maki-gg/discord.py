discord.py
==========

.. image:: https://discord.com/api/guilds/336642139381301249/embed.png
   :target: https://discord.gg/r3sSKJJ
   :alt: Discord server invite
.. image:: https://img.shields.io/pypi/v/discord.py.svg
   :target: https://pypi.python.org/pypi/discord.py
   :alt: PyPI version info
.. image:: https://img.shields.io/pypi/pyversions/discord.py.svg
   :target: https://pypi.python.org/pypi/discord.py
   :alt: PyPI supported Python versions

A modern, easy to use, feature-rich, and async ready API wrapper for Discord written in Python.

Key Features
-------------

- Modern Pythonic API using ``async`` and ``await``.
- Proper rate limit handling.
- Optimised in both speed and memory.

Installing
----------

**Python 3.8 or higher is required**

To install the library without full voice support, you can just run the following command:

.. note::

    A `Virtual Environment <https://docs.python.org/3/library/venv.html>`__ is recommended to install
    the library, especially on Linux where the system Python is externally managed and restricts which
    packages you can install on it.


.. code:: sh

    # Linux/macOS
    python3 -m pip install -U discord.py

    # Windows
    py -3 -m pip install -U discord.py

Otherwise to get voice support you should run the following command:

.. code:: sh

    # Linux/macOS
    python3 -m pip install -U "discord.py[voice]"

    # Windows
    py -3 -m pip install -U discord.py[voice]


To install the development version, do the following:

.. code:: sh

    $ git clone https://github.com/Rapptz/discord.py
    $ cd discord.py
    $ python3 -m pip install -U .[voice]


Optional Packages
~~~~~~~~~~~~~~~~~~

* `PyNaCl <https://pypi.org/project/PyNaCl/>`__ (for voice support)
* `redis <https://pypi.org/project/redis/>`__ (for the fork's Redis cache tier)

Please note that when installing voice support on Linux, you must install the following packages via your favourite package manager (e.g. ``apt``, ``dnf``, etc) before running the above commands:

* libffi-dev (or ``libffi-devel`` on some systems)
* python-dev (e.g. ``python3.8-dev`` for Python 3.8)

Quick Example
--------------

.. code:: py

    import discord

    class MyClient(discord.Client):
        async def on_ready(self):
            print('Logged on as', self.user)

        async def on_message(self, message):
            # don't respond to ourselves
            if message.author == self.user:
                return

            if message.content == 'ping':
                await message.channel.send('pong')

    intents = discord.Intents.default()
    intents.message_content = True
    client = MyClient(intents=intents)
    client.run('token')

Bot Example
~~~~~~~~~~~~~

.. code:: py

    import discord
    from discord.ext import commands

    intents = discord.Intents.default()
    intents.message_content = True
    bot = commands.Bot(command_prefix='>', intents=intents)

    @bot.command()
    async def ping(ctx):
        await ctx.send('pong')

    bot.run('token')

You can find more examples in the examples directory.

Fork changes: caching
---------------------

This fork adds a cache eviction layer that upstream discord.py does not have. All of it lives in
``discord/cache.py``; the upstream files only carry a few hook lines marked ``# Maki fork: cache layer``.
Without the ``cache`` option the library behaves exactly like upstream.

What changed
~~~~~~~~~~~~~

* ``cache=discord.CacheSettings(...)`` is accepted by ``Client``, ``AutoShardedClient``, ``commands.Bot``
  and ``commands.AutoShardedBot``.
* Members and threads inside a guild are evicted after ``member_ttl`` / ``thread_ttl`` seconds without
  activity, or beyond ``member_max`` / ``thread_max`` entries per guild. The bot's own member is never evicted.
* The message cache is keyed by id (O(1) lookups) and evicts messages after ``message_ttl`` seconds.
  ``max_messages`` still bounds its size.
* Optional Redis tier via ``RedisSettings`` (Redis Cluster by default). Guild data is mirrored to Redis
  from a single gateway hook. With ``max_loaded_guilds`` the least recently used guilds are unloaded from
  memory and restored from Redis (or the API) before their next event is processed.
* Coroutine helpers in ``discord.cache``: ``get_cached_user``, ``get_cached_member``,
  ``get_cached_message``, ``is_guild_loaded`` and ``load_guild``.

Setup
~~~~~~

In-memory eviction only:

.. code:: py

    import discord
    from discord.ext import commands

    intents = discord.Intents.default()
    intents.members = True

    bot = commands.Bot(
        command_prefix='!',
        intents=intents,
        chunk_guilds_at_startup=False,
        max_messages=10_000,
        cache=discord.CacheSettings(
            member_ttl=6 * 3600,   # members untouched for 6 hours are dropped
            member_max=5_000,      # and at most 5000 members per guild
            thread_ttl=3600,
            message_ttl=3600,
        ),
    )

With the Redis tier and guild unloading (``pip install -U "discord.py[redis]"``):

.. code:: py

    bot = commands.Bot(
        command_prefix='!',
        intents=intents,
        chunk_guilds_at_startup=False,
        cache=discord.CacheSettings(
            member_ttl=6 * 3600,
            message_ttl=3600,
            max_loaded_guilds=4_000,
            redis=discord.RedisSettings(
                ['redis://node1:6379', 'redis://node2:6379', 'redis://node3:6379'],
                message_ttl=3600,     # keep deleted/edited message content readable for an hour
                serve_fetches=True,   # also mirror members and users for get_cached_member/user
            ),
        ),
    )

    @bot.event
    async def on_raw_message_delete(payload):
        message = await discord.cache.get_cached_message(bot, payload.channel_id, payload.message_id)

Pass ``cluster=False`` to ``RedisSettings`` for a standalone Redis server.

To share one connection pool between the cache and your own code (cogs, services), build the client
yourself and hand it over with ``client=``. The library never closes a client it did not create.
This also covers Sentinel setups:

.. code:: py

    class MyBot(commands.AutoShardedBot):
        def __init__(self, **kwargs):
            # redis.asyncio clients can be built before the event loop runs
            sentinel = Sentinel(SENTINEL_NODES, sentinel_kwargs={'socket_timeout': 0.5})
            self.redis = sentinel.master_for(
                'mymaster', connection_pool_class=BlockingSentinelConnectionPool,
                max_connections=BUDGET, timeout=10, decode_responses=True,
            )
            super().__init__(
                **kwargs,
                cache=discord.CacheSettings(
                    member_ttl=6 * 3600,
                    max_loaded_guilds=4_000,
                    redis=discord.RedisSettings(client=self.redis, message_ttl=3600),
                ),
            )

The cache uses the shared pool sparingly: one connection for the write batcher, one per guild being
restored, and one per ``get_cached_*`` call. Budget a few extra connections per process for it.

Caveats
~~~~~~~~

* ``Guild.chunked`` compares ``member_count`` with the cached member count and is not meaningful once
  members are evicted. Use ``chunk_guilds_at_startup=False`` with ``member_ttl``.
* ``Client.guilds`` still lists unloaded guilds; their channels, roles and members are empty until
  ``load_guild`` runs. Guild events trigger that automatically.
* ``fetch_*`` methods still always hit the API. Use the ``discord.cache`` helpers for Redis-backed lookups.
* If Redis is unreachable at startup the client raises. If it fails later, mirroring pauses for 30 seconds
  and unloaded guilds are restored from the API instead.
* Message mirroring is high volume; only set ``RedisSettings.message_ttl`` if you need deleted or edited
  message content after it left memory.
* ``CacheSettings`` validates that ``redis.member_ttl``, ``redis.thread_ttl`` and ``redis.message_ttl``
  are each at least as long as the matching in-memory TTL, since Redis is the fallback tier. Widen the
  Redis TTL rather than shrinking the in-memory one if this raises.

Links
------

- `Documentation <https://discordpy.readthedocs.io/en/latest/index.html>`_
- `Official Discord Server <https://discord.gg/r3sSKJJ>`_
- `Discord API <https://discord.gg/discord-api>`_
