Juju Operator Framework Charm Interface for PostgreSQL Relations
================================================================

[![PyPI version](https://badge.fury.io/py/ops-lib-pgsql.svg)](https://badge.fury.io/py/ops-lib-pgsql)
[![PyPI Supported Python Versions](https://img.shields.io/pypi/pyversions/ops-lib-pgsql.svg)](https://pypi.python.org/pypi/ops-lib-pgsql/)
[![GitHub license](https://img.shields.io/github/license/canonical/ops-lib-pgsql)](https://github.com/canonical/ops-lib-pgsql/blob/master/LICENSE)
[![GitHub Actions (Tests)](https://github.com/canonical/ops-lib-pgsql/workflows/Tests/badge.svg)](https://github.com/canonical/ops-lib-pgsql/actions?query=workflow%3ATests)


To use this interface in your
[Juju Operator Framework](https://github.com/canonical/operator) charm,
instruct [charmcraft](https://github.com/canonical/charmcraft) to embed
it into your built Operator Framework charm by adding ops-lib-pgsql to
your `requirements.txt` file::

```
ops
ops-lib-pgsql
```

Your charm needs to declare its use of the interface in its `metadata.yaml` file:

```yaml
requires:
  db:
    interface: pgsql
    limit: 1  # Most charms only handle a single PostgreSQL Application.
```


Your charm needs to bootstrap it and handle events:

```python
import ops.lib
pgsql = ops.lib.use("pgsql", 1, "postgresql-charmers@lists.launchpad.net")


class MyCharm(ops.charm.CharmBase):
    _state = ops.framework.StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self._state.set_default(db_conn_str=None, db_uri=None, db_ro_uris=[])
        self.db = pgsql.PostgreSQLClient(self, 'db')  # 'db' relation in metadata.yaml
        self.framework.observe(self.db.on.database_relation_joined, self._on_database_relation_joined)
        self.framework.observe(self.db.on.master_changed, self._on_master_changed)
        self.framework.observe(self.db.on.standby_changed, self._on_standby_changed)

    def _on_database_relation_joined(self, event: pgsql.DatabaseRelationJoinedEvent):
        if self.model.unit.is_leader():
            # Provide requirements to the PostgreSQL server.
            event.database = 'mydbname'  # Request database named mydbname
            event.extensions = ['citext']  # Request the citext extension installed
        elif event.database != 'mydbname':
            # Leader has not yet set requirements. Defer, incase this unit
            # becomes leader and needs to perform that operation.
            event.defer()
            return

    def _on_master_changed(self, event: pgsql.MasterChangedEvent):
        if event.database != 'mydbname':
            # Leader has not yet set requirements. Wait until next event,
            # or risk connecting to an incorrect database.
            return
        
        # The connection to the primary database has been created,
        # changed or removed. More specific events are available, but
        # most charms will find it easier to just handle the Changed
        # events. event.master is None if the master database is not
        # available, or a pgsql.ConnectionString instance.
        self._state.db_conn_str = None if event.master is None else event.master.conn_str
        self._state.db_uri = None if event.master is None else event.master.uri

        # You probably want to emit an event here or call a setup routine to
        # do something useful with the libpq connection string or URI now they
        # are available.

    def _on_standby_changed(self, event: pgsql.StandbyChangedEvent):
        if event.database != 'mydbname':
            # Leader has not yet set requirements. Wait until next event,
            # or risk connecting to an incorrect database.
            return

        # Charms needing access to the hot standby databases can get
        # their connection details here. Applications can scale out
        # horizontally if they can make use of the read only hot
        # standby replica databases, rather than only use the single
        # master. event.stanbys will be an empty list if no hot standby
        # databases are available.
        self._state.db_ro_uris = [c.uri for c in event.standbys]
```
