# This file is part of the ops-lib-pgsql component for Juju Operator
# Framework Charms.
# Copyright 2020 Canonical Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the Lesser GNU General Public License version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the Lesser GNU General Public License for more details.
#
# You should have received a copy of the Lesser GNU General Public
# License along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

import logging
import subprocess
from typing import Dict, Iterable, List, Mapping

from pgconnstr import ConnectionString
import ops.charm
import ops.framework
from ops.model import Application, Relation, RelationData, Unit
import yaml


__all__ = [
    "DatabaseAvailableEvent",
    "DatabaseChangedEvent",
    "DatabaseGoneEvent",
    "DatabaseRelationBrokenEvent",
    "DatabaseRelationJoinedEvent",
    "MasterAvailableEvent",
    "MasterChangedEvent",
    "MasterGoneEvent",
    "PostgreSQLClient",
    "PostgreSQLRelationEvent",
    "StandbyAvailableEvent",
    "StandbyChangedEvent",
    "StandbyGoneEvent",
]


# Leadership settings key prefix used by PostgreSQLClient
LEADER_KEY = "interface.pgsql"


class PostgreSQLRelationEvent(ops.charm.RelationEvent):
    def __init__(
        self,
        handle,
        relation: Relation,
        app: Application,
        unit: Unit,
        local_unit: Unit,
    ):
        super().__init__(handle, relation, app, unit)
        self._local_unit = local_unit

    @property
    def master(self) -> ConnectionString:
        """The master database. None if there is currently no master."""
        conn_str = _master(self.log, self.relation, self._local_unit)
        if conn_str:
            return ConnectionString(conn_str)
        return None

    @property
    def standbys(self) -> List[ConnectionString]:
        """All hot standby databases (read only replicas)."""
        return [ConnectionString(s) for s in _standbys(self.log, self.relation, self._local_unit)]

    @property
    def database(self) -> str:
        """Connect relation to the named database.

        May only be set by the leader.

        The PostgreSQL provider will create the database if necessary.
        It will never remove it.

        Applications needing to share data need to agree on the
        database name to use. This is normally set at deployment time
        in charm config, or propagated by relation to cooperating charms,
        and then passed to this method.
        """
        # Per https://bugs.launchpad.net/juju/+bug/1869915, non-leaders
        # can't read application relation data and thus unable to tell
        # if the remote end has applied requested configuration, because
        # it can't tell what was requested. Instead for now, we stuff a
        # copy in leadership settings.
        # appdata = self.relation.data[self.unit.app]
        appdata = _get_pgsql_leader_data().get(self.relation.id, {})
        return appdata.get("database") or None

    @database.setter
    def database(self, dbname: str) -> None:
        if dbname is None:
            dbname = ""
        self.relation.data[self._local_unit.app]["database"] = dbname
        # Deprecated, per PostgreSQLClient._mirror_appdata()
        self.relation.data[self._local_unit]["database"] = dbname
        # Inform our peers, since they can't read the appdata.
        d = _get_pgsql_leader_data()
        d.setdefault(self.relation.id, {})["database"] = dbname
        _set_pgsql_leader_data(d)

        self.log.debug("database set to %s on relation %s", dbname, self.relation.id)

    @property
    def roles(self) -> List[str]:
        """Assign PostgreSQL roles to the application's database user.

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
        """
        # Per https://bugs.launchpad.net/juju/+bug/1869915, non-leaders
        # can't read application relation data and thus unable to tell
        # if the remote end has applied requested configuration, because
        # it can't tell what was requested. Instead for now, we stuff a
        # copy in leadership settings.
        # appdata = self.relation.data[self.unit.app]
        appdata = _get_pgsql_leader_data().get(self.relation.id, {})
        sroles = appdata.get("roles") or ""
        return list(r for r in sroles.split(",") if r)

    @roles.setter
    def roles(self, roles: Iterable[str]) -> None:
        if roles is None:
            roles = []
        sroles = ",".join(sorted(roles))
        self.relation.data[self._local_unit.app]["roles"] = sroles
        # Deprecated, per PostgreSQLClient._mirror_appdata()
        self.relation.data[self._local_unit]["roles"] = sroles
        # Inform our peers, since they can't read the appdata
        d = _get_pgsql_leader_data()
        d.setdefault(self.relation.id, {})["roles"] = sroles
        _set_pgsql_leader_data(d)
        self.log.debug("roles set to %s on relation %s", sroles, self.relation.id)

    @property
    def extensions(self) -> List[str]:
        """Ensure PostgreSQL extensions are installed into the database.

        May only be set by the leader.

        The PostgreSQL provider will install the requested extensions
        into the database. Extensions not bundled with PostgreSQL are
        normally installed onto the PostgreSQL application using the
        `extra_packages` config setting.
        """
        # Per https://bugs.launchpad.net/juju/+bug/1869915, non-leaders
        # can't read application relation data and thus unable to tell
        # if the remote end has applied requested configuration, because
        # it can't tell what was requested. Instead for now, we stuff a
        # copy in leadership settings.
        # appdata = self.relation.data[self.unit.app]
        appdata = _get_pgsql_leader_data().get(self.relation.id, {})
        sext = appdata.get("extensions") or ""
        return list(e for e in sext.split(",") if e)

    @extensions.setter
    def extensions(self, extensions: Iterable[str]) -> None:
        if extensions is None:
            extensions = []
        sext = ",".join(sorted(extensions))
        self.relation.data[self._local_unit.app]["extensions"] = sext
        # Deprecated, per PostgreSQLClient._mirror_appdata()
        self.relation.data[self._local_unit]["extensions"] = sext  # Deprecated, should be app reldata
        # Inform our peers, since they can't read the appdata
        d = _get_pgsql_leader_data()
        d.setdefault(self.relation.id, {})["extensions"] = sext
        _set_pgsql_leader_data(d)
        self.log.debug("extensions set to %s on relation %s", sext, self.relation.id)

    @property
    def version(self) -> str:
        """Return the PostgreSQL version provided on this relation."""
        # Prefer version information from remote app relation data, fall back to remote unit relation data.
        return self.relation.data[self.app].get("version", None) or self.relation.data[self.unit].get("version", None)

    def snapshot(self):
        s = [
            super().snapshot(),
            dict(local_unit_name=self._local_unit.name),
        ]
        return s

    def restore(self, snapshot) -> None:
        sup, mine = snapshot
        super().restore(sup)
        self._local_unit = self.framework.model.get_unit(mine["local_unit_name"])

    @property
    def log(self):
        return logging.getLogger("pgsql.client.{}.event".format(self.relation.name))


class DatabaseRelationJoinedEvent(PostgreSQLRelationEvent):
    """The pgsql relation has been joined.

    This is the best time to configure the relation, setting the database name,
    and required roles and extensions.
    """

    pass


class DatabaseRelationBrokenEvent(PostgreSQLRelationEvent):
    """The pgsql relation is gone.

    Charms can watch for this event, setting their workload status to
    'blocked' and shutting down services that require access to a
    database to function.

    This is subtly different to MasterGoneEvent, StandbyGoneEvent
    & DatabaseGoneEvent in that the databases are not going to ever
    come back.
    """

    pass


class DatabaseAvailableEvent(PostgreSQLRelationEvent):
    """A new database is available for use on this relation."""

    pass


class MasterAvailableEvent(PostgreSQLRelationEvent):
    """A master database is available for use on this relation."""

    pass


class StandbyAvailableEvent(PostgreSQLRelationEvent):
    """A new hot standby database is available for use (read only replica)."""

    pass


class DatabaseChangedEvent(PostgreSQLRelationEvent):
    """Connection details to one of the databases on this relation have changed."""

    pass


class MasterChangedEvent(PostgreSQLRelationEvent):
    """Connection details to the master database on this relation have changed."""

    pass


class StandbyChangedEvent(PostgreSQLRelationEvent):
    """Connection details to one or more standby databases on this relation have changed."""

    pass


class DatabaseGoneEvent(PostgreSQLRelationEvent):
    """All databases have gone from this relation; there are no databases.

    The relation may still be active, and the databases may come back.
    Charms requireing access to a database may set their workload
    status to 'waiting' to indicate this state.

    Charms generally won't watch for this event, instead watching for
    DatabaseChangedEvent and seeing if the master attribute is None
    and the standbys list empty.
    """

    pass


class MasterGoneEvent(PostgreSQLRelationEvent):
    """The master database is gone from this relation; there may still be standby databases.

    The relation may still be active, and the master may come back.
    Charms requireing access to a master may set their workload status
    to 'waiting' to indicate this state.

    Charms generally won't watch for this event, instead watching for
    MasterChangedEvent and seeing if the master attribute is None.
    """

    pass


class StandbyGoneEvent(PostgreSQLRelationEvent):
    """All standby databases are gone from this relation; there may still be a master database.

    The relation may still be active, and the standbys may come back.
    Charms requireing access to hot standbys may set their workload
    status to 'waiting' to indicate this state.

    Charms generally won't watch for this event, instead watching
    for StandbyChangedEvent and seeing if the standbys attribute is
    an empty list.
    """

    pass


class PostgreSQLClientEvents(ops.framework.ObjectEvents):
    database_relation_joined = ops.framework.EventSource(DatabaseRelationJoinedEvent)
    database_relation_broken = ops.framework.EventSource(DatabaseRelationBrokenEvent)
    database_available = ops.framework.EventSource(DatabaseAvailableEvent)
    master_available = ops.framework.EventSource(MasterAvailableEvent)
    standby_available = ops.framework.EventSource(StandbyAvailableEvent)
    database_changed = ops.framework.EventSource(DatabaseChangedEvent)
    master_changed = ops.framework.EventSource(MasterChangedEvent)
    standby_changed = ops.framework.EventSource(StandbyChangedEvent)
    database_gone = ops.framework.EventSource(DatabaseGoneEvent)
    master_gone = ops.framework.EventSource(MasterGoneEvent)
    standby_gone = ops.framework.EventSource(StandbyGoneEvent)


class PostgreSQLClient(ops.framework.Object):
    """Requires side of a PostgreSQL (pgsql) Endpoint"""

    on = PostgreSQLClientEvents()
    _state = ops.framework.StoredState()

    relation_name: str = None
    log: logging.Logger = None

    def __init__(self, charm: ops.charm.CharmBase, relation_name: str):
        super().__init__(charm, relation_name)

        self.relation_name = relation_name
        self.log = logging.getLogger("pgsql.client.{}".format(relation_name))

        self._state.set_default(rels={})

        self.framework.observe(charm.on[relation_name].relation_joined, self._on_joined)
        self.framework.observe(charm.on[relation_name].relation_changed, self._on_changed)
        self.framework.observe(charm.on[relation_name].relation_broken, self._on_broken)
        self.framework.observe(charm.on.upgrade_charm, self._on_upgrade_charm)
        self.framework.observe(charm.on.leader_elected, self._on_leader_change)
        self.framework.observe(charm.on.leader_settings_changed, self._on_leader_change)

    def _db_event_args(self, relation_event: ops.charm.RelationEvent):
        return dict(
            relation=relation_event.relation,
            app=relation_event.app,
            unit=relation_event.unit,
            local_unit=self.model.unit,
        )

    def _on_joined(self, event: ops.charm.RelationEvent) -> None:
        self.log.debug("_on_joined for relation %r", event.relation.id)
        self._mirror_appdata()  # PostgreSQL charm backwards compatibility
        if event.relation.id in self._state.rels:
            self.log.debug("database_relation_joined event already emitted for relation %r", event.relation.id)
        else:
            self.log.info("emitting database_relation_joined event for relation %r", event.relation.id)
            self.on.database_relation_joined.emit(**self._db_event_args(event))
            self._state.rels[event.relation.id] = dict(master=None, standbys=None)

    def _on_changed(self, event: ops.charm.RelationEvent) -> None:  # noqa: C901
        self.log.debug("_on_changed for relation %r", event.relation.id)

        rel = event.relation
        relid = rel.id

        # It has been observed, but not reproduced, situations where the
        # relation-changed hook gets run before the relation-joined hook.
        # Log this and defer. We don't want to continue, as we want
        # _on_joined to be invoked and emit the join events.
        if relid not in self._state.rels:
            self.log.error(
                f"{self.relation_name}-relation-changed hook run before {self.relation_name}-joined! Deferring event."
            )
            event.defer()
            return

        self._mirror_appdata()  # PostgreSQL charm backwards compatibility
        kwargs = self._db_event_args(event)

        prev_master = self._state.rels.get(relid, {}).get("master", None)
        prev_standbys = self._state.rels.get(relid, {}).get("standbys", []) or []
        new_master = _master(self.log, rel, self.model.unit)
        new_standbys = _standbys(self.log, rel, self.model.unit)

        database_available = False
        database_changed = False
        database_gone = False

        if prev_master is None and new_master is not None:
            self.log.info("emitting master_available event for relation %r", event.relation.id)
            self.on.master_available.emit(**kwargs)
            database_available = True

        if str(prev_master) != str(new_master):
            self.log.info("emitting master_changed event for relation %r", event.relation.id)
            self.on.master_changed.emit(**kwargs)
            database_changed = True

        if prev_master is not None and new_master is None:
            self.log.info("emitting master_gone event for relation %r", event.relation.id)
            self.on.master_gone.emit(**kwargs)
            database_gone = True

        if prev_standbys == [] and new_standbys != []:
            self.log.info("emitting standby_available event for relation %r", event.relation.id)
            self.on.standby_available.emit(**kwargs)
            database_available = True

        if prev_standbys != new_standbys:
            self.log.info("emitting standby_changed event for relation %r", event.relation.id)
            self.on.standby_changed.emit(**kwargs)
            database_changed = True

        if prev_standbys != [] and new_standbys == []:
            self.log.info("emitting standby_gone event for relation %r", event.relation.id)
            self.on.standby_gone.emit(**kwargs)
            database_gone = True

        if database_available:
            self.log.info("emitting database_available event for relation %r", event.relation.id)
            self.on.database_available.emit(**kwargs)

        if database_changed:
            self.log.info("emitting database_changed event for relation %r", event.relation.id)
            self.on.database_changed.emit(**kwargs)

        if new_master is None and new_standbys == [] and database_gone:
            self.log.info("emitting database_gone event for relation %r", event.relation.id)
            self.on.database_gone.emit(**kwargs)

        self._state.rels[relid]["master"] = new_master
        self._state.rels[relid]["standbys"] = new_standbys

    def _on_broken(self, event: ops.charm.RelationEvent) -> None:
        relid = event.relation.id
        self.log.debug("_on_broken for relation %r", relid)
        rd = self._state.rels.get(relid, {})
        db_gone = False
        kwargs = self._db_event_args(event)

        # We need to handle the final changed and gone events here,
        # because we don't handle departed events. We don't handle
        # departed events because with Juju 2.7.5 we can't tell if the
        # remote unit is departing, or if the local unit is. If the
        # local unit is departing, it likely already has had its
        # access revoked to some of the databases, but doesn't know
        # about it yet because there are several other
        # relation-departed hooks to run. By not handling
        # relation-departed hooks, we can avoid alerting the charm
        # with updated lists of connection strings that may not
        # actually be valid. It also has the side effect of alerting
        # the charm with a single event when the relation is torn
        # down, rather than one for every remote unit. Note that we
        # can avoid handling departed only because the pgsql relation
        # protocol ensures that if a remote unit departes, the other
        # remote units update their relation data and we get a
        # relation-changed event. Also note that most relations don't
        # share this problem because the relation data for a remote
        # unit only contains information about that particular remote
        # unit, while the pgsql interface contains information about
        # the remote unit and its peers (this choice was made to
        # support proxies like pgbouncer, where 2 pgbouncer units can
        # both present endpoints for each of 3 or more postgresql
        # units).
        if rd.get("master"):
            self.log.info("emitting master_changed event for relation %r", relid)
            self.on.master_changed.emit(**kwargs)
            self.log.info("emitting master_gone event for relation %r", relid)
            self.on.master_gone.emit(**kwargs)
            db_gone = True
        if rd.get("standbys"):
            self.log.info("emitting standby_changed event for relation %r", relid)
            self.on.standby_changed.emit(**kwargs)
            self.log.info("emitting standby_gone event for relation %r", relid)
            self.on.standby_gone.emit(**kwargs)
            db_gone = True
        if db_gone:
            self.log.info("emitting database_changed event for relation %r", relid)
            self.on.database_changed.emit(**kwargs)
            self.log.info("emitting database_gone event for relation %r", relid)
            self.on.database_gone.emit(**kwargs)

        self.log.info("emitting database_relation_broken event for relation %r", relid)
        self.on.database_relation_broken.emit(**kwargs)

        if relid in self._state.rels:
            self.log.info("cleaning up broken relation %r", relid)
            del self._state.rels[relid]

    def _on_upgrade_charm(self, event: ops.charm.UpgradeCharmEvent) -> None:
        self.log.debug("_on_upgrade_charm for relation %r", self.relation_name)
        # Migrate leader's unit relation data to application relation data if necessary.
        # This is for upgrading from pre-operator-framework charms.
        if self.model.unit.is_leader():
            new_lead_data = {}
            for rel in self.model.relations[self.relation_name]:
                logged = False
                new_lead_data[rel.id] = {}
                ldata = rel.data[self.model.unit]
                adata = rel.data[self.model.unit.app]
                for k in ["database", "roles", "extensions"]:
                    if k in ldata and k not in adata:
                        if not logged:
                            self.log.info(
                                "leader migrating legacy relation data to app relation data for relation %r", rel.id
                            )
                            logged = True
                        adata[k] = ldata[k]
                    new_lead_data[rel.id][k] = adata.get(k, "")
            _set_pgsql_leader_data(new_lead_data)
        elif _get_pgsql_leader_data():
            self._mirror_appdata()
        else:
            event.defer()

    def _on_leader_change(self, event: ops.charm.HookEvent) -> None:
        self._mirror_appdata()  # PostgreSQL charm backwards compatibility

    def _mirror_appdata(self) -> None:
        """Mirror the relation configuration in relation app data to unit relation and leadership data.

        The PostgreSQL charm supports older versions of Juju and does
        not read application relation data, instead waiting on
        consensus in the units' relation data. Until it is updated to
        read application relation data directly, we mirror the application
        data to the unit relation data for backwards compatibility.

        Per https://bugs.launchpad.net/bugs/1869915, non-lead units
        cannot read their own application relation data, so we instead
        use leadership settings to share a copy of the application
        relation data to the non-lead units.

        This mess allows us to provide an API where only the lead unit
        configures the relation, while providing backwards
        compatibility for old versions of the PostgreSQL charm or new
        versions of the PostgreSQL charm running on old versions.
        """
        cur_lead_data = _get_pgsql_leader_data()

        new_lead_data = {}
        rewrite_lead_data = False

        for relation in self.model.relations[self.relation_name]:
            self.log.debug("mirroring app relation data for relation %r", relation.id)

            cur_relid_lead_data = cur_lead_data.get(relation.id, {})
            loc_data = relation.data[self.model.unit]

            if self.model.unit.is_leader():
                new_lead_data[relation.id] = {}
                app_data = relation.data[self.model.unit.app]
                for k in ["database", "roles", "extensions"]:
                    v = app_data.get(k, "")

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
            self.log.debug("storing update app relation data in leadership settings")
            _set_pgsql_leader_data(new_lead_data)


def _master(log: logging.Logger, relation: Relation, local_unit: Unit) -> str:
    """The master database. None if there is currently no master."""
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
        conn_str = reldata.get("master")
        if conn_str:
            if _is_ready(log, appdata, locdata, reldata):
                log.debug("ready master found on relation %s", relation.id)
                return conn_str
            log.debug("unready master found on relation %s", relation.id)
    log.debug("no ready master found on relation %s", relation.id)
    return None


def _standbys(log: logging.Logger, relation: Relation, local_unit: Unit) -> List[str]:
    """All hot standby databases (read only replicas)."""
    # Per https://bugs.launchpad.net/juju/+bug/1869915, non-leaders
    # can't read application relation data and thus unable to tell
    # if the remote end has applied requested configuration, because
    # it can't tell what was requested. Instead for now, we stuff a
    # copy in leadership settings.
    # appdata = relation.data[local_unit.app]
    appdata = _get_pgsql_leader_data().get(relation.id, {})
    locdata = relation.data[local_unit]
    for _, reldata in sorted((k.name, v) for k, v in relation.data.items() if k != local_unit.app):
        raw = reldata.get("standbys")
        if raw:
            if _is_ready(log, appdata, locdata, reldata):
                log.debug("ready standbys found on relation %s", relation.id)
                return [conn_str for conn_str in raw.splitlines() if conn_str]
            log.debug("unready standbys found on relation %s", relation.id)
    log.debug("no ready standbys found on relation %s", relation.id)
    return []


def _is_ready(
    log: logging.Logger,
    appdata: Mapping,  # peer-shared data; leadership data or peer relation app data
    locdata: RelationData,
    reldata: RelationData,
) -> bool:
    # The relation is not ready for use if the server has not yet
    # mirrored relation config set by the client. This is how we
    # know that the server has acted on requests like setting the
    # database name.
    for k in ["database", "roles", "extensions"]:
        got, want = reldata.get(k) or "", appdata.get(k) or ""
        if got != want:
            log.debug("not ready because got %s==%r, requested %r", k, got, want)
            return False

    # To ensure clients do not attempt to connect to the database
    # until the PostgreSQL server has authorized them, the PostgreSQL
    # charm publishes to the relation the egress-subnets that it has
    # granted access.
    allowed_subnets = set(_csplit(reldata.get("allowed-subnets")))
    my_egress = set(_csplit(locdata.get("egress-subnets")))
    if my_egress <= allowed_subnets:
        log.debug("relation is ready")
        return True
    else:
        log.debug("egress not granted access (%s > %s)", my_egress, allowed_subnets)
        return False


def _csplit(s) -> Iterable[str]:
    if s:
        for b in s.split(","):
            b = b.strip()
            if b:
                yield b


def _get_pgsql_leader_data() -> Dict[int, Dict[str, str]]:
    """Returns the dictionary stored as yaml under LEADER_KEY in the Juju leadership settings.

    The keys of this dict are relation ids, with the value another dict
    containing the settings for that relation.

    ie. leader_get()[LEADER_KEY][relation_id]['database'] is what the leader set the database
    property on that relation to.

    TODO: Replace with a better wrapper around leadership data, with the option of sharing
    this data between peers using peer relation application data rather than leadership settings.
    """
    return yaml.safe_load(_leader_get(LEADER_KEY) or "{}")


def _set_pgsql_leader_data(d: Dict[int, Dict[str, str]]) -> None:
    _leader_set({LEADER_KEY: yaml.dump(d)})


def _leader_get(attribute: str) -> str:
    cmd = ["leader-get", "--format=yaml", attribute]
    return yaml.safe_load(subprocess.check_output(cmd).decode("UTF-8"))


def _leader_set(settings: Dict[str, str]):
    cmd = ["leader-set"] + ["{}={}".format(k, v or "") for k, v in settings.items()]
    subprocess.check_call(cmd)
