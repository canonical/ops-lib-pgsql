import ipaddress
import logging
import re
from typing import Iterable, List, Tuple
import urllib

import ops.charm
import ops.framework
import ops.model


class ConnectionString:
    """A libpq connection string.

    >>> c = ConnectionString(host='1.2.3.4', dbname='mydb',
    ...                      port=5432, user='anon', password='secret')
    ...
    >>> str(c)
    'host=1.2.3.4 dbname=mydb port=5432 user=anon password=secret
    >>> str(ConnectionString(str(c), dbname='otherdb'))
    'host=1.2.3.4 dbname=otherdb port=5432 user=anon password=secret

    Components may be accessed as attributes.

    >>> c.dbname
    'mydb'
    >>> c.host
    '1.2.3.4'
    >>> c.port
    '5432'

    The standard URI format is also accessible:

    >>> c.uri
    'postgresql://anon:secret@1.2.3.4:5432/mydb'

    """

    # Common libpq connection string elements. Not all of them. Use
    # getattr to avoid exceptions when attempting to read the more
    # obscure libpq connection string elements.
    conn_str: str = None
    host: str = None
    dbname: str = None
    port: str = None
    user: str = None
    password: str = None
    uri: str = None


    def __init__(self, conn_str=None, **kw):  # noqa
        # Parse libpq key=value style connection string. Components
        # passed by keyword argument override. If the connection string
        # is invalid, some components may be skipped (but in practice,
        # where database and usernames don't contain whitespace,
        # quotes or backslashes, this doesn't happen).
        if conn_str is not None:
            r = re.compile(
                r"""(?x)
                    (\w+) \s* = \s*
                    (?:
                      '((?:.|\.)*?)' |
                      (\S*)
                    )
                    (?=(?:\s|\Z))
                """
            )
            for key, v1, v2 in r.findall(conn_str):
                if key not in kw:
                    kw[key] = v1 or v2

        def quote(x):
            q = str(x).replace("\\", "\\\\").replace("'", "\\'")
            q = q.replace('\n', ' ')  # \n is invalid in connection strings
            if ' ' in q:
                q = "'" + q + "'"
            return q

        c = " ".join("{}={}".format(k, quote(v)) for k, v in sorted(kw.items()) if v)
        self.conn_str = c

        for k, v in kw.items():
            setattr(self, k, v)

        self._keys = set(kw.keys())

        # Construct the documented PostgreSQL URI for applications
        # that use this format. PostgreSQL docs refer to this as a
        # URI so we do do, even though it meets the requirements the
        # more specific term URL.
        fmt = ['postgresql://']
        d = {k: urllib.parse.quote(v, safe='') for k, v in kw.items() if v}
        if 'user' in d:
            if 'password' in d:
                fmt.append('{user}:{password}@')
            else:
                fmt.append('{user}@')
        if 'host' in kw:
            try:
                hostaddr = ipaddress.ip_address(kw.get('hostaddr') or kw.get('host'))
                if isinstance(hostaddr, ipaddress.IPv6Address):
                    d['hostaddr'] = '[{}]'.format(hostaddr)
                else:
                    d['hostaddr'] = str(hostaddr)
            except ValueError:
                # Not an IP address, but hopefully a resolvable name.
                d['hostaddr'] = d['host']
            del d['host']
            fmt.append('{hostaddr}')
        if 'port' in d:
            fmt.append(':{port}')
        if 'dbname' in d:
            fmt.append('/{dbname}')
        main_keys = frozenset(['user', 'password', 'dbname', 'hostaddr', 'port'])
        extra_fmt = ['{}={{{}}}'.format(extra, extra) for extra in sorted(d.keys()) if extra not in main_keys]
        if extra_fmt:
            fmt.extend(['?', '&'.join(extra_fmt)])
        self.uri = ''.join(fmt).format(**d)

    def keys(self) -> Iterable[str]:
        return iter(self._keys)

    def items(self) -> Iterable[Tuple[str, str]]:
        return {k: self[k] for k in self.keys()}.items()

    def values(self) -> Iterable[str]:
        return iter(self[k] for k in self.keys())

    def __getitem__(self, key) -> str:
        if isinstance(key, int):
            return super(ConnectionString, self).__getitem__(key)
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __str__(self) -> str:
        return self.conn_str

    def __repr__(self) -> str:
        return 'ConnectionString({!r})'.format(self.conn_str)


class PostgreSQLRelationEvent(ops.charm.RelationEvent):
    def __init__(
        self,
        handle,
        relation: ops.model.Relation,
        app: ops.model.Application,
        unit: ops.model.Unit,
        local_unit: ops.model.Unit,
    ):
        super().__init__(handle, relation, app, unit)
        self._local_unit = local_unit

    @property
    def master(self) -> ConnectionString:
        '''The master database. None if there is currently no master.'''
        return ConnectionString(_master(self.relation, self._local_unit))

    @property
    def standbys(self) -> List[ConnectionString]:
        '''All hot standby databases (read only replicas).'''
        return [ConnectionString(s) for s in _standbys(self.relation, self._local_unit)]

    @property
    def database(self) -> str:
        '''Connect relation to the named database.

        May only be set by the leader.

        The PostgreSQL provider will create the database if necessary.
        It will never remove it.

        Applications needing to share data need to agree on the
        database name to use. This is normally set at deployment time
        in charm config, or propagated by relation to cooperating charms,
        and then passed to this method.
        '''
        return self.relation.data[self.unit.app].get('database') or None

    @database.setter
    def database(self, dbname: str) -> None:
        self.relation.data[self._local_unit.app]['database'] = dbname
        # Deprecated, per PostgreSQLClient._mirror_appdata()
        self.relation.data[self._local_unit]['database'] = dbname

    @property
    def roles(self) -> List[str]:
        '''Assign PostgreSQL roles to the application's database user.

        May only be set by the leader.

        The PostgreSQL provider will create the roles if necessary.
        Applications may open a PostgreSQL connection to the PostgreSQL
        service and grant permissions on tables and resources to roles,
        thereby controlling access to other applications.

        It is common to not use custom roles, and instead grant access
        to tables and other PostgreSQL resources to the PUBLIC role.
        This grants access to all clients able to connect to the
        database, and a normal setup when it is not a security concern
        to grant all clients access to all data in the database.
        '''
        sroles = self.relation.data[self.unit.app].get('roles') or ''
        return list(sroles.split(','))

    @roles.setter
    def roles(self, roles: Iterable[str]) -> None:
        sroles = ','.join(sorted(roles))
        self.relation.data[self._local_unit.app]['roles'] = sroles
        # Deprecated, per PostgreSQLClient._mirror_appdata()
        self.relation.data[self._local_unit]['roles'] = sroles

    @property
    def extensions(self) -> List[str]:
        '''Ensure PostgreSQL extensions are installed into the database.

        May only be set by the leader.

        The PostgreSQL provider will install the requested extensions
        into the database. Extensions not bundled with PostgreSQL are
        normally installed onto the PostgreSQL application using the
        `extra_packages` config setting.
        '''
        sext = self.relation.data[self.unit.app].get('extensions') or ''
        return list(sext.split(','))

    @extensions.setter
    def extensions(self, extensions: Iterable[str]) -> None:
        sext = ','.join(sorted(extensions))
        self.relation.data[self._local_unit.app]['extensions'] = sext
        # Deprecated, per PostgreSQLClient._mirror_appdata()
        self.relation.data[self._local_unit]['extensions'] = sext  # Deprecated, should be app reldata

    def snapshot(self):
        s = [
            super().snapshot(),
            dict(local_unit_name=self._local_unit.name),
        ]
        return s

    def restore(self, snapshot) -> None:
        sup, mine = snapshot
        super().restore(sup)
        self._local_unit = self.framework.model.get_unit(mine['local_unit_name'])


class DatabaseJoinedEvent(PostgreSQLRelationEvent):
    '''The pgsql relation has been joined.

    This is the best time to configure the relation, setting the database name,
    and required roles and extensions.
    '''

    pass


class DatabaseAvailableEvent(PostgreSQLRelationEvent):
    '''A new database is available for use on this relation.'''

    pass


class MasterAvailableEvent(PostgreSQLRelationEvent):
    '''A master database is available for use on this relation.'''

    pass


class StandbyAvailableEvent(PostgreSQLRelationEvent):
    '''A new hot standby database is available for use (read only replica).'''

    pass


class DatabaseChangedEvent(PostgreSQLRelationEvent):
    '''Connection details to one of the databases on this relation have changed.'''

    pass


class MasterChangedEvent(PostgreSQLRelationEvent):
    '''Connection details to the master database on this relation have changed.'''

    pass


class StandbyChangedEvent(PostgreSQLRelationEvent):
    pass


class DatabaseGoneEvent(PostgreSQLRelationEvent):
    pass


class MasterGoneEvent(PostgreSQLRelationEvent):
    pass


class StandbyGoneEvent(PostgreSQLRelationEvent):
    pass


class PostgreSQLClientEvents(ops.charm.CharmEvents):
    database_joined = ops.framework.EventSource(DatabaseJoinedEvent)
    database_available = ops.framework.EventSource(DatabaseAvailableEvent)
    master_available = ops.framework.EventSource(MasterAvailableEvent)
    standby_available = ops.framework.EventSource(StandbyAvailableEvent)
    database_changed = ops.framework.EventSource(DatabaseAvailableEvent)
    master_changed = ops.framework.EventSource(MasterAvailableEvent)
    standby_changed = ops.framework.EventSource(StandbyAvailableEvent)
    database_gone = ops.framework.EventSource(DatabaseGoneEvent)
    master_gone = ops.framework.EventSource(MasterGoneEvent)
    standby_gone = ops.framework.EventSource(StandbyGoneEvent)


class PostgreSQLClient(ops.framework.Object):
    '''Requires side of a PostgreSQL (pgsql) Endpoint'''

    on = PostgreSQLClientEvents()
    _state = ops.framework.StoredState()

    def __init__(self, charm: ops.charm.CharmBase, relation_name: str):
        super().__init__(charm, relation_name)

        self.relation_name = relation_name
        self.log = logging.getLogger('pgsql.client.{}'.format(relation_name))

        self._state.set_default(rels={})

        self.framework.observe(charm.on[relation_name].relation_joined, self._on_joined)
        self.framework.observe(charm.on[relation_name].relation_changed, self._on_changed)
        self.framework.observe(charm.on[relation_name].relation_departed, self._on_changed)
        self.framework.observe(charm.on[relation_name].relation_broken, self._on_broken)
        self.framework.observe(charm.on.upgrade_charm, self._on_upgrade_charm)

    def _db_event_args(self, relation_event):
        return dict(
            relation=relation_event.relation,
            app=relation_event.app,
            unit=relation_event.unit,
            local_unit=self.model.unit,
        )

    def _on_joined(self, event: ops.charm.RelationEvent) -> None:
        self.on.database_joined.emit(**self._db_event_args(event))
        self._state.rels[event.relation.id] = dict(
            master=_master(event.relation, self.model.unit), standbys=_standbys(event.relation, self.model.unit),
        )

    def _on_changed(self, event: ops.charm.RelationEvent) -> None:
        # Backwards compatibility until PostgreSQL charm updated to use application relation data.
        self._mirror_appdata(event)
        kwargs = self._db_event_args(event)

        rel = event.relation
        relid = rel.id

        prev_master = self._state.rels[relid]['master']
        prev_standbys = self._state.rels[relid]['standbys']
        new_master = _master(rel, self.model.unit)
        new_standbys = _standbys(rel, self.model.unit)

        database_available = False
        database_changed = False
        database_gone = False

        if prev_master is None and new_master is not None:
            self.on.master_available.emit(**kwargs)
            database_available = True

        if prev_master is not None and new_master is not None:
            self.on.master_changed.emit(**kwargs)
            database_changed = True

        if prev_master is not None and new_master is None:
            self.on.master_gone.emit(**kwargs)
            database_gone = True

        if prev_standbys == [] and new_standbys != []:
            self.on.standby_available.emit(**kwargs)
            database_available = True

        if prev_standbys != [] and new_standbys != prev_standbys:
            self.on.standby_changed.emit(**kwargs)
            database_changed = True

        if prev_standbys != [] and new_standbys == []:
            self.on.standby_gone.emit(**kwargs)
            database_gone = True

        if database_available:
            self.on.database_available.emit(**kwargs)

        if database_changed:
            self.on.database_changed.emit(**kwargs)

        if database_gone:
            self.on.database_gone.emit(**kwargs)

        self._state.rels[relid]['master'] = new_master
        self._state.rels[relid]['standbys'] = new_standbys

    def _on_broken(self, event: ops.charm.RelationEvent) -> None:
        del self._state.rels[event.relation.id]

    def _on_upgrade_charm(self, event: ops.charm.UpgradeCharmEvent) -> None:
        # Migrate leader's unit relation data to application relation data if necessary.
        # This is for upgrading from pre-operator-framework charms.
        if self.model.unit.is_leader():
            for rel in self.framework.model.relations[self.relation_name]:
                ldata = rel.data[self.model.unit]
                adata = rel.data[self.model.unit.app]
                for k in ['database', 'roles', 'extensions']:
                    if k in ldata and k not in adata:
                        adata[k] = ldata[k]

    def _mirror_appdata(self, event: ops.charm.RelationEvent) -> None:
        '''Mirror the relation configuration in relation app data to unit relation data.

        The PostgreSQL charm supports older versions of Juju and does not read application
        relation data, instead waiting on consensus in the units' relation data. Until it
        is updated to read application relation data, we mirror the application data to the
        unit relation data for backwards compatibility.
        '''
        ldata = event.relation.data[self.model.unit]
        adata = event.relation.data[self.model.unit.app]
        for key in ['database', 'roles', 'extensions']:
            if key in adata and adata[key] != ldata.get(key):
                ldata[key] = adata[key]


def _master(relation: ops.model.Relation, local_unit: str) -> str:
    '''The master database. None if there is currently no master.'''
    locdata = relation.data[local_unit]
    for _, reldata in sorted((str(k), v) for k, v in relation.data.items()):
        conn_str = reldata.get('master')
        if conn_str and _is_authorized(locdata, reldata):
            return conn_str
    return None


def _standbys(relation: ops.model.Relation, local_unit: str) -> List[str]:
    '''All hot standby databases (read only replicas).'''
    locdata = relation.data[local_unit]
    for _, reldata in sorted((str(k), v) for k, v in relation.data.items()):
        raw = reldata.get('standbys')
        if raw and _is_authorized(locdata, reldata):
            return (conn_str for conn_str in raw.splitlines() if conn_str)
    return []


def _is_authorized(locdata: ops.model.RelationData, reldata: ops.model.RelationData) -> bool:
    # To ensure clients do not attempt to connect to the database until the PostgreSQL server
    # has authorized them, the PostgreSQL charm publishes the egress-subnets that it has granted
    # access to the relation.
    allowed_subnets = set(_csplit(reldata.get('allowed-subnets')))
    my_egress = set(_csplit(locdata.get('egress-subnets')))
    return my_egress <= allowed_subnets


def _csplit(s) -> Iterable[str]:
    if s:
        for b in s.split(','):
            b = b.strip()
            if b:
                yield b
