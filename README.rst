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

This fork adds cache eviction that upstream discord.py does not have. All of it lives in
``discord/cache.py``; the upstream files only carry a few hook lines marked ``# Maki fork: cache layer``.
Without the ``cache`` option the library behaves exactly like upstream.

What changed
~~~~~~~~~~~~~

* ``cache=discord.CacheSettings(...)`` is accepted by ``Client``, ``AutoShardedClient``, ``commands.Bot``
  and ``commands.AutoShardedBot``.
* Members inside a guild are evicted after ``member_ttl`` seconds without activity, or beyond
  ``member_max`` entries per guild. The bot's own member is never evicted.
* The message cache is keyed by id (O(1) lookups) and evicts messages after ``message_ttl`` seconds.
  ``max_messages`` still bounds its size and on a busy bot is usually what evicts first.
* A background sweeper runs every ``sweep_interval`` seconds (default 300).

Nothing else is bounded. Guild, channel, role, emoji, sticker, thread and voice state caches behave as
upstream. Users are held weakly by upstream already and die with their last member or message.

Setup
~~~~~~

.. code:: py

    import discord
    from discord.ext import commands

    intents = discord.Intents.default()
    intents.members = True

    bot = commands.Bot(
        command_prefix='!',
        intents=intents,
        chunk_guilds_at_startup=False,
        max_messages=50_000,        # size this to the delete log window you want
        cache=discord.CacheSettings(
            member_ttl=24 * 3600,   # members untouched for a day are dropped
            member_max=10_000,      # and at most 10000 members per guild
            message_ttl=6 * 3600,
        ),
    )

Caveats
~~~~~~~~

* An evicted member or message behaves like one that was never cached: the raw event still fires, the
  richer one does not. ``on_member_update``, ``on_member_remove``, ``on_message_edit`` and
  ``on_message_delete`` only fire for objects still in memory.
* ``max_messages`` is a count shared by every guild on the process, not a duration. Set it from your
  ``MESSAGE_CREATE`` rate times the window you want; ``message_ttl`` only trims quiet processes.
* ``Guild.chunked`` compares ``member_count`` with the cached member count and is not meaningful once
  members are evicted. Use ``chunk_guilds_at_startup=False`` with ``member_ttl``.
* ``Message`` objects keep their ``Member``/``User`` alive until the message itself is evicted.
* Nothing persists across restarts; the cache starts cold like upstream.

Updating from upstream
~~~~~~~~~~~~~~~~~~~~~~~

**Where the fork lives**

* All logic: ``discord/cache.py`` and ``tests/test_cache.py``. Upstream never touches these.
* Hook lines inside upstream files, every one marked ``# Maki fork: cache layer``:
  ``discord/state.py`` (7), ``discord/client.py`` (3), ``discord/__init__.py`` (1). 11 in total;
  ``tests/test_cache.py`` asserts those numbers.
* ``discord/guild.py``, ``discord/gateway.py`` and ``discord/ext/commands/bot.py`` are identical to
  upstream and must stay so.
* Also fork-only: this README section and the ``CacheSettings`` entry in ``docs/api.rst``.

**Procedure**

Merge, never rebase.

.. code:: sh

    git remote add upstream https://github.com/Rapptz/discord.py.git   # once
    git fetch upstream
    git checkout master
    git merge upstream/master

Resolving a conflict: keep upstream's version of the surrounding code, then put the marked line(s) back.
Never drop a marked line to make a conflict go away. Each hook is a single call into ``self._cache``.

**Checklist after every merge**

.. code:: sh

    git diff upstream/master -- discord/guild.py discord/gateway.py discord/ext/commands/bot.py   # must print nothing
    python -m pytest -q
    python -m pyright discord/cache.py discord/state.py discord/client.py
    ruff format --check

``test_fork_marker_count`` fails if a hook line was dropped. ``test_message_cache_supports_every_state_usage``
fails if upstream started calling something on ``self._messages`` that ``MessageCache`` does not provide;
add the method to ``MessageCache`` when that happens. When a hook is intentionally added or removed,
update the counts above and in the test in the same commit.

Links
------

- `Documentation <https://discordpy.readthedocs.io/en/latest/index.html>`_
- `Official Discord Server <https://discord.gg/r3sSKJJ>`_
- `Discord API <https://discord.gg/discord-api>`_
