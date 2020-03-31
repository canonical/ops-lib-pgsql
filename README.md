Juju Operator Framework Charm Interface for PostgreSQL Relations
================================================================

/!\ Alpha. Everything here subject to change based on feedback and
emerging consensus.

To use this interface in your Juju Operator Framework charm, first
install it into your git branch:

```
git submodule add git+ssh://git.launchpad.net/~stub/interface-pgsql/+git/operator mod/interface-pgsql
mkdir lib/interface
ln -s ../../mod/interface-pgsql/pgsql.py lib/interface/
```

Your charm needs to bootstrap it and handle events:

```python
from interfaces import pgsql


class MyCharm(ops.charm.CharmBase):
    state = ops.framework.StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self.state.set_default(db_conn_str=None, db_uri=None, db_ro_uris=[])
        self.db = pgsql.PostgreSQLClient(self, 'db')  # 'db' relation required in metadata.yaml
        self.framework.observe(self.db.on.database_joined, self.on_database_joined)
        self.framework.observe(self.db.on.master_changed, self.on_master_changed)
        self.framework.observe(self.db.on.standby_changed, self.on_standby_changed)

    def on_database_joined(self, event: pgsql.DatabaseJoinedEvent):
        # Provide requirements to the PostgreSQL server.
        event.database = 'mydbname'  # Request database named mydbname
        event.extensions = ['citext']  # Request the citext extension installed

    def on_master_changed(self, event: pgsql.MasterChangedEvent):
        # Enforce a single 'db' relation, or else we risk directing writes to
        # an unknown backend. This can happen via user error, or redeploying
        # the PostgreSQL backend.
        if len(self.model.relations['db']) > 0:
            self.unit.status = ops.model.BlockedStatus("Too many db relations!")
            event.defer()
            return
        if event.relation.id not in (r.id for r in self.model.relations['db']):
            return  # Deferred event for relation that no longer exists.
        
        # The connection to the primary database have been created, changed or removed.
        # More specific events are available, but most charms will find it easier
        # to just handle the Changed events.
        # event.master is None if the master database is not available, or
        # a pgsql.ConnectionString instance.
        self.state.db_conn_str = None if event.master is None else event.master.conn_str
        self.state.db_uri = None if event.master is None else event.master.uri

        # You probably want to emit an event here or call a setup routine to
        # do something useful with the libpq connection string or URI now they
        # are available.

    def on_standby_changed(self, event: pgsql.StandbyChangedEvent):
        if len(self.model.relations['db']) > 0:
            self.unit.status = ops.model.BlockedStatus("Too many db relations!")
            event.defer()
            return
        if event.relation.id not in (r.id for r in self.model.relations['db']):
            return  # Deferred event for relation that no longer exists.

        # Charms needing access to the hot standby databases can get
        # their connection details here. Applications can scale out
        # horizontally if they can make use of the read only hot
        # standby replica databases, rather than only use the single
        # master. event.stanbys will be an empty list if no hot standby
        # databases are available.
        self.state.db_ro_uris = [c.uri for c in event.standbys]
```
