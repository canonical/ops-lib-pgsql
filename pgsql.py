import logging
import subprocess
from typing import Dict, Iterable, List, Mapping

import ops.charm
import ops.framework
import ops.model
import yaml

from connstr import ConnectionString


# Leadership settings key prefix used by PostgreSQLClient
LEADER_KEY = 'interface.pgsql'


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
        return ConnectionString(_master(self.log, self.relation, self._local_unit))

    @property
    def standbys(self) -> List[ConnectionString]:
        '''All hot standby databases (read only replicas).'''
        return [ConnectionString(s) for s in _standbys(self.log, self.relation, self._local_unit)]

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
        # Per https://bugs.launchpad.net/juju/+bug/1869915, non-leaders
        # can't read application relation data and thus unable to tell
        # if the remote end has applied requested configuration, because
        # it can't tell what was requested. Instead for now, we stuff a
        # copy in leadership settings.
        # appdata = self.relation.data[self.unit.app]
        appdata = _get_pgsql_leader_data().get(self.relation.id, {})
        return appdata.get('database') or None

    @database.setter
    def database(self, dbname: str) -> None:
        self.relation.data[self._local_unit.app]['database'] = dbname
        # Deprecated, per PostgreSQLClient._mirror_appdata()
        self.relation.data[self._local_unit]['database'] = dbname
        self.log.debug('database set to %s on relation %s', dbname, self.relation.id)

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
        # Per https://bugs.launchpad.net/juju/+bug/1869915, non-leaders
        # can't read application relation data and thus unable to tell
        # if the remote end has applied requested configuration, because
        # it can't tell what was requested. Instead for now, we stuff a
        # copy in leadership settings.
        # appdata = self.relation.data[self.unit.app]
        appdata = _get_pgsql_leader_data().get(self.relation.id, {})
        sroles = appdata.get('roles') or ''
        return list(sroles.split(','))

    @roles.setter
    def roles(self, roles: Iterable[str]) -> None:
        sroles = ','.join(sorted(roles))
        self.relation.data[self._local_unit.app]['roles'] = sroles
        # Deprecated, per PostgreSQLClient._mirror_appdata()
        self.relation.data[self._local_unit]['roles'] = sroles
        self.log.debug('roles set to %s on relation %s', sroles, self.relation.id)

    @property
    def extensions(self) -> List[str]:
        '''Ensure PostgreSQL extensions are installed into the database.

        May only be set by the leader.

        The PostgreSQL provider will install the requested extensions
        into the database. Extensions not bundled with PostgreSQL are
        normally installed onto the PostgreSQL application using the
        `extra_packages` config setting.
        '''
        # Per https://bugs.launchpad.net/juju/+bug/1869915, non-leaders
        # can't read application relation data and thus unable to tell
        # if the remote end has applied requested configuration, because
        # it can't tell what was requested. Instead for now, we stuff a
        # copy in leadership settings.
        # appdata = self.relation.data[self.unit.app]
        appdata = _get_pgsql_leader_data().get(self.relation.id, {})
        sext = appdata.get('extensions') or ''
        return list(sext.split(','))

    @extensions.setter
    def extensions(self, extensions: Iterable[str]) -> None:
        sext = ','.join(sorted(extensions))
        self.relation.data[self._local_unit.app]['extensions'] = sext
        # Deprecated, per PostgreSQLClient._mirror_appdata()
        self.relation.data[self._local_unit]['extensions'] = sext  # Deprecated, should be app reldata
        self.log.debug('extensions set to %s on relation %s', sext, self.relation.id)

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

    @property
    def log(self):
        return logging.getLogger('pgsql.client.{}.event'.format(self.relation.name))


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


class PostgreSQLClientEvents(ops.framework.EventSetBase):
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
        self.framework.observe(charm.on.leader_elected, self._on_leader_change)
        self.framework.observe(charm.on.leader_settings_changed, self._on_leader_change)

    def _db_event_args(self, relation_event):
        return dict(
            relation=relation_event.relation,
            app=relation_event.app,
            unit=relation_event.unit,
            local_unit=self.model.unit,
        )

    def _on_joined(self, event: ops.charm.RelationEvent) -> None:
        self.log.debug('_on_joined for relation %r', event.relation.id)
        self._mirror_appdata()  # PostgreSQL charm backwards compatibility
        self.log.info('emitting database_joined event for relation %r', event.relation.id)
        self.on.database_joined.emit(**self._db_event_args(event))
        self._state.rels[event.relation.id] = dict(master=None, standbys=None)

    def _on_changed(self, event: ops.charm.RelationEvent) -> None:
        self.log.debug('_on_changed for relation %r', event.relation.id)
        self._mirror_appdata()  # PostgreSQL charm backwards compatibility
        kwargs = self._db_event_args(event)

        rel = event.relation
        relid = rel.id

        prev_master = self._state.rels[relid]['master']
        prev_standbys = self._state.rels[relid]['standbys']
        new_master = _master(self.log, rel, self.model.unit)
        new_standbys = _standbys(self.log, rel, self.model.unit)

        database_available = False
        database_changed = False
        database_gone = False

        if prev_master is None and new_master is not None:
            self.log.info('emitting master_available event for relation %r', event.relation.id)
            self.on.master_available.emit(**kwargs)
            database_available = True

        if str(prev_master) != str(new_master):
            self.log.info('emitting master_changed event for relation %r', event.relation.id)
            self.on.master_changed.emit(**kwargs)
            database_changed = True

        if prev_master is not None and new_master is None:
            self.log.info('emitting master_gone event for relation %r', event.relation.id)
            self.on.master_gone.emit(**kwargs)
            database_gone = True

        if prev_standbys == [] and new_standbys != []:
            self.log.info('emitting standby_available event for relation %r', event.relation.id)
            self.on.standby_available.emit(**kwargs)
            database_available = True

        if prev_standbys != new_standbys:
            self.log.info('emitting standby_changed event for relation %r', event.relation.id)
            self.on.standby_changed.emit(**kwargs)
            database_changed = True

        if prev_standbys != [] and new_standbys == []:
            self.log.info('emitting standby_gone event for relation %r', event.relation.id)
            self.on.standby_gone.emit(**kwargs)
            database_gone = True

        if database_available:
            self.log.info('emitting database_available event for relation %r', event.relation.id)
            self.on.database_available.emit(**kwargs)

        if database_changed:
            self.log.info('emitting database_changed event for relation %r', event.relation.id)
            self.on.database_changed.emit(**kwargs)

        if (prev_master is not None or prev_standbys != []) and database_gone:
            self.log.info('emitting database_gone event for relation %r', event.relation.id)
            self.on.database_gone.emit(**kwargs)

        self._state.rels[relid]['master'] = new_master
        self._state.rels[relid]['standbys'] = new_standbys

    def _on_broken(self, event: ops.charm.RelationEvent) -> None:
        self.log.debug('_on_broken for relation %r', event.relation.id)
        if event.relation.id in self._state.rels:
            self.log.info('cleaning up broken relation %r', event.relation.id)
            del self._state.rels[event.relation.id]

    def _on_upgrade_charm(self, event: ops.charm.UpgradeCharmEvent) -> None:
        self.log.debug('_on_upgrade_charm for relation %r', self.relation_name)
        # Migrate leader's unit relation data to application relation data if necessary.
        # This is for upgrading from pre-operator-framework charms.
        if self.model.unit.is_leader():
            new_lead_data = {}
            for rel in self.model.relations[self.relation_name]:
                self.log.info('leader migrating legacy relation data to app relation data for relation %r', rel.id)
                new_lead_data[rel.id] = {}
                ldata = rel.data[self.model.unit]
                adata = rel.data[self.model.unit.app]
                for k in ['database', 'roles', 'extensions']:
                    if k in ldata and k not in adata:
                        adata[k] = ldata[k]
                    new_lead_data[rel.id][k] = adata.get(k, '')
            _set_pgsql_leader_data(new_lead_data)
        elif _get_pgsql_leader_data():
            self._mirror_appdata()
        else:
            event.defer()

    def _on_leader_change(self, event: ops.charm.HookEvent) -> None:
        self._mirror_appdata()  # PostgreSQL charm backwards compatibility

    def _mirror_appdata(self) -> None:
        '''Mirror the relation configuration in relation app data to unit relation data.

        The PostgreSQL charm supports older versions of Juju and does
        not read application relation data, instead waiting on
        consensus in the units' relation data. Until it is updated to
        read application relation data, we mirror the application data
        to the unit relation data for backwards compatibility.

        Per https://bugs.launchpad.net/bugs/1869915, non-lead units
        cannot read their own application relation data, so we instead
        use leadership settings to share a copy of the application
        relation data to the non-lead units.

        This mess allows us to provide an API where only the lead unit
        configures the relation, while providing backwards
        compatibility for old versions of the PostgreSQL charm or new
        versions of the PostgreSQL charm running on old versions.
        '''
        cur_lead_data = _get_pgsql_leader_data()

        new_lead_data = {}
        rewrite_lead_data = False

        for relation in self.model.relations[self.relation_name]:
            self.log.debug('mirroring app relation data for relation %r', relation.id)

            cur_relid_lead_data = cur_lead_data.get(relation.id, {})
            loc_data = relation.data[self.model.unit]

            if self.model.unit.is_leader():
                new_lead_data[relation.id] = {}
                app_data = relation.data[self.model.unit.app]
                for k in ['database', 'roles', 'extensions']:
                    v = app_data.get(k, '')

                    # Mirror application relation data to unit relation
                    # data for old versions of the PostgeSQL charm or
                    # when running with older versions of Juju.
                    loc_data[k] = v

                    # Mirror application relation data to leadership storage,
                    # so non-lead units can mirror it to unit relation data.
                    # This is a workaround for https://bugs.launchpad.net/bugs/1869915
                    new_lead_data[relation.id][k] = v
                    if cur_relid_lead_data.get(k) != v:
                        rewrite_lead_data = True
            else:
                for k, v in cur_relid_lead_data.items():
                    loc_data[k] = v

        if rewrite_lead_data:
            self.log.debug('storing update app relation data in leadership settings')
            _set_pgsql_leader_data(new_lead_data)


def _master(log: logging.Logger, relation: ops.model.Relation, local_unit: ops.model.Unit) -> str:
    '''The master database. None if there is currently no master.'''
    # Per https://bugs.launchpad.net/juju/+bug/1869915, non-leaders
    # can't read application relation data and thus unable to tell
    # if the remote end has applied requested configuration, because
    # it can't tell what was requested. Instead for now, we stuff a
    # copy in leadership settings.
    # appdata = relation.data[local_unit.app]
    appdata = _get_pgsql_leader_data().get(relation.id, {})
    locdata = relation.data[local_unit]
    for key, reldata in sorted((k.name, v) for k, v in relation.data.items() if k != local_unit.app):
        if key == local_unit.app:
            continue  # Avoid land mine, special case per lp:1869915
        conn_str = reldata.get('master')
        if conn_str:
            if _is_ready(log, appdata, locdata, reldata):
                log.debug('ready master found on relation %s', relation.id)
                return conn_str
            log.debug('unready master found on relation %s', relation.id)
    log.debug('no ready master found on relation %s', relation.id)
    return None


def _standbys(log: logging.Logger, relation: ops.model.Relation, local_unit: ops.model.Unit) -> List[str]:
    '''All hot standby databases (read only replicas).'''
    # Per https://bugs.launchpad.net/juju/+bug/1869915, non-leaders
    # can't read application relation data and thus unable to tell
    # if the remote end has applied requested configuration, because
    # it can't tell what was requested. Instead for now, we stuff a
    # copy in leadership settings.
    # appdata = relation.data[local_unit.app]
    appdata = _get_pgsql_leader_data().get(relation.id, {})
    locdata = relation.data[local_unit]
    for _, reldata in sorted((k.name, v) for k, v in relation.data.items() if k != local_unit.app):
        raw = reldata.get('standbys')
        if raw:
            if _is_ready(log, appdata, locdata, reldata):
                log.debug('ready standbys found on relation %s', relation.id)
                return (conn_str for conn_str in raw.splitlines() if conn_str)
            log.debug('unready standbys found on relation %s', relation.id)
    log.debug('no ready standbys found on relation %s', relation.id)
    return []


def _is_ready(
    log: logging.Logger, appdata: Mapping, locdata: ops.model.RelationData, reldata: ops.model.RelationData,
) -> bool:
    # The relation is not ready for use if the server has not yet
    # mirrored relation config set by the client. This is how we
    # know that the server has acted on requests like setting the
    # database name.
    for k in ['database', 'roles', 'extensions']:
        got, want = reldata.get(k) or '', appdata.get(k) or ''
        if got != want:
            log.debug('not ready because got %s==%r, requested %r', k, got, want)
            return False

    # To ensure clients do not attempt to connect to the database
    # until the PostgreSQL server has authorized them, the PostgreSQL
    # charm publishes to the relation the egress-subnets that it has
    # granted access.
    allowed_subnets = set(_csplit(reldata.get('allowed-subnets')))
    my_egress = set(_csplit(locdata.get('egress-subnets')))
    if my_egress <= allowed_subnets:
        log.debug('relation is ready')
        return True
    else:
        log.debug('egress not granted access (%s > %s)', my_egress, allowed_subnets)
        return False


def _csplit(s) -> Iterable[str]:
    if s:
        for b in s.split(','):
            b = b.strip()
            if b:
                yield b


def _get_pgsql_leader_data() -> Dict[int, Dict[str, str]]:
    return yaml.safe_load(_leader_get(LEADER_KEY) or '{}')


def _set_pgsql_leader_data(d: Dict[int, Dict[str, str]]) -> None:
    _leader_set({LEADER_KEY: yaml.dump(d)})


def _leader_get(attribute: str):
    cmd = ['leader-get', '--format=yaml', attribute]
    return yaml.safe_load(subprocess.check_output(cmd).decode('UTF-8'))


def _leader_set(settings: Dict[str, str]):
    cmd = ['leader-set'] + ['{}={}'.format(k, v or '') for k, v in settings.items()]
    subprocess.check_call(cmd)
