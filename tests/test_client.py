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

import re
from textwrap import dedent
import unittest
from unittest.mock import patch

import ops.charm
import ops.lib
import ops.testing

from pgsql import client, ConnectionString


class Charm(ops.charm.CharmBase):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.db = client.PostgreSQLClient(self, "db")

        for event in self.db.on.events().values():
            self.framework.observe(event, self.on_event)

        self.reset()

    def reset(self):
        for event_name in self.db.on.events().keys():
            setattr(self, f"{event_name}_event", None)
            setattr(self, f"{event_name}_called", False)
            setattr(self, f"{event_name}_count", 0)

    def on_event(self, event):
        # We can't pass in the name, because the Framework insists on
        # handlers being real methods and not for example functools.partial
        # wrappers. So reverse engineer the event name from the class name.
        # Which works for the events we care about.
        event_name = re.sub(r"([A-Z]+)", r"_\1", event.__class__.__name__).lower()[1:-6]
        # Store the event and set a flag, so that tests can tell
        # what has happened.
        setattr(self, f"{event_name}_event", event)
        setattr(self, f"{event_name}_called", True)
        count = getattr(self, f"{event_name}_count")
        setattr(self, f"{event_name}_count", count + 1)


class TestPGSQLBase(unittest.TestCase):
    def setUp(self):
        # TODO: Operator Framework should expose leadership, and be mocked there.
        self.leadership_data = {}
        leader_patch = unittest.mock.patch.multiple(
            client,
            _get_pgsql_leader_data=self.leadership_data.copy,
            _set_pgsql_leader_data=self.leadership_data.update,
        )
        leader_patch.start()
        self.addCleanup(leader_patch.stop)

        meta = dedent(
            """\
            name: pgclient
            requires:
              db:
                interface: pgsql
                limit: 1
            """
        )
        self.harness = ops.testing.Harness(Charm, meta=meta)
        self.addCleanup(self.harness.cleanup)
        self.relation_id = self.harness.add_relation("db", "postgresql")
        self.remote_app_name = "postgresql"
        self.remote_unit_names = [f"{self.remote_app_name}/{i}" for i in range(3, 5)]
        for n in self.remote_unit_names:
            self.harness.add_relation_unit(self.relation_id, n)

        self.harness.begin_with_initial_hooks()

        self.ev = self.harness.charm.database_relation_joined_event
        self.relation = self.harness.model.relations["db"][0]
        self.log = self.harness.charm.db.log
        self.local_unit = self.harness.model.unit
        self.remote_app = self.ev.app
        self.remote_units = sorted((u for u in self.relation.units if u.app == self.remote_app), key=lambda x: x.name)
        self.charm = self.harness.charm


class TestPGSQLHarness(TestPGSQLBase):
    def test_leadership_mock(self):
        self.leadership_data["foo"] = "bar"
        self.assertEqual(client._get_pgsql_leader_data(), self.leadership_data)
        self.assertIsNot(client._get_pgsql_leader_data(), self.leadership_data)

        client._set_pgsql_leader_data({"one": "two"})
        self.assertEqual(client._get_pgsql_leader_data(), {"foo": "bar", "one": "two"})

        client._set_pgsql_leader_data({"foo": "baz"})
        self.assertEqual(client._get_pgsql_leader_data(), {"foo": "baz", "one": "two"})

        self.assertEqual(self.leadership_data, {"foo": "baz", "one": "two"})


class TestPGSQLHelpers(TestPGSQLBase):
    def setUp(self):
        super().setUp()
        self.harness.disable_hooks()

    @patch("pgsql.client._is_ready")
    def test_master_unset(self, is_ready):
        # Master helper returns None when no relation data is set.
        is_ready.return_value = True

        # No connection details present on relation, yet.
        self.assertIsNone(client._master(self.log, self.relation, self.local_unit))
        self.assertFalse(is_ready.called)

    @patch("pgsql.client._is_ready")
    def test_master_unready(self, is_ready):
        # Master helper returns None when relation app data is set but not yet ready.
        is_ready.return_value = False
        standbys = ["host=standby1", "host=standby2"]
        rd = {"master": "host=master", "standbys": "\n".join(standbys)}
        self.harness.update_relation_data(self.relation_id, self.remote_app_name, rd)

        self.assertIsNone(client._master(self.log, self.relation, self.local_unit))
        self.assertTrue(is_ready.called)
        is_ready.assert_called_once_with(
            self.log, self.leadership_data, self.relation.data[self.local_unit], self.relation.data[self.remote_app],
        )

    @patch("pgsql.client._is_ready")
    def test_master_ready(self, is_ready):
        # Master helper returns connection string when relation app data is set and ready.
        is_ready.return_value = True
        standbys = ["host=standby1", "host=standby2"]
        rd = {"master": "host=master", "standbys": "\n".join(standbys)}
        self.harness.update_relation_data(self.relation_id, self.remote_app_name, rd)

        self.assertEqual(client._master(self.log, self.relation, self.local_unit), rd["master"])
        self.assertTrue(is_ready.called)
        is_ready.assert_called_once_with(
            self.log, self.leadership_data, self.relation.data[self.local_unit], self.relation.data[self.remote_app],
        )

    @patch("pgsql.client._is_ready")
    def test_master_legacy(self, is_ready):
        # Ensure we fall back to using unit relation data if the app relation data is unset.
        is_ready.return_value = True
        standbys = ["host=standby1", "host=standby2"]
        rd = {"master": "host=master", "standbys": "\n".join(standbys)}
        self.harness.update_relation_data(self.relation_id, self.remote_unit_names[1], rd)

        self.assertEqual(client._master(self.log, self.relation, self.local_unit), rd["master"])
        self.assertTrue(is_ready.called)
        is_ready.assert_called_once_with(
            self.log,
            self.leadership_data,
            self.relation.data[self.local_unit],
            self.relation.data[self.remote_units[1]],
        )

    @patch("pgsql.client._is_ready")
    def test_standbys_unset(self, is_ready):
        # Standbys helper returns None when no relation data is set.
        is_ready.return_value = True

        # No connection details present on relation, yet.
        self.assertEqual(client._standbys(self.log, self.relation, self.local_unit), [])
        self.assertFalse(is_ready.called)

    @patch("pgsql.client._is_ready")
    def test_standbys_unready(self, is_ready):
        # Standbys helper returns None when relation app data is set but not yet ready.
        is_ready.return_value = False
        standbys = ["host=standby1", "host=standby2"]
        rd = {"master": "host=master", "standbys": "\n".join(standbys)}
        self.harness.update_relation_data(self.relation_id, self.remote_app_name, rd)

        self.assertEqual(client._standbys(self.log, self.relation, self.local_unit), [])
        self.assertTrue(is_ready.called)
        is_ready.assert_called_once_with(
            self.log, self.leadership_data, self.relation.data[self.local_unit], self.relation.data[self.remote_app],
        )

    @patch("pgsql.client._is_ready")
    def test_standbys_ready(self, is_ready):
        # Master helper returns connection string when relation app data is set and ready.
        is_ready.return_value = True
        standbys = ["host=standby1", "host=standby2"]
        rd = {"master": "host=master", "standbys": "\n".join(standbys)}
        self.harness.update_relation_data(self.relation_id, self.remote_app_name, rd)

        self.assertEqual(client._standbys(self.log, self.relation, self.local_unit), standbys)
        self.assertTrue(is_ready.called)
        is_ready.assert_called_once_with(
            self.log, self.leadership_data, self.relation.data[self.local_unit], self.relation.data[self.remote_app],
        )

    @patch("pgsql.client._is_ready")
    def test_standbys_legacy(self, is_ready):
        # Ensure we fall back to using unit relation data if the app relation data is unset.
        is_ready.return_value = True
        standbys = ["host=standby1", "host=standby2"]
        rd = {"master": "host=master", "standbys": "\n".join(standbys)}
        self.harness.update_relation_data(self.relation_id, self.remote_unit_names[1], rd)

        self.assertEqual(client._standbys(self.log, self.relation, self.local_unit), standbys)
        self.assertTrue(is_ready.called)
        is_ready.assert_called_once_with(
            self.log,
            self.leadership_data,
            self.relation.data[self.local_unit],
            self.relation.data[self.remote_units[1]],
        )

    def test_is_ready_no_egress(self):
        # The relation is considered ready if the client has published
        # no egress-subnets. This unexpected result is to support old
        # versions of Juju that predate cross-model relation support.
        # This should not happen with supported Juju versions.
        self.assertTrue(client._is_ready(self.log, {}, {}, {}))
        self.assertTrue(client._is_ready(self.log, {}, {}, {"allowed-subnets": "127.23.0.0/24"}))

    def test_is_ready_no_allowed(self):
        # The relation is not ready if allowed-subnets does not contain our egress-subnets.
        # The remote end has not yet granted the local unit access.
        self.assertFalse(client._is_ready(self.log, {}, {"egress-subnets": "127.23.0.0/24"}, {}))
        self.assertFalse(
            client._is_ready(self.log, {}, {"egress-subnets": "127.23.0.0/24"}, {"allowed-subnets": "127.0.1/24"})
        )

    def test_is_ready_defaults(self):
        # allowed-subnets grants access, and default database settings requested.
        self.assertTrue(
            client._is_ready(
                self.log, {}, {"egress-subnets": "127.23.1.0/24"}, {"allowed-subnets": "127.23.0.0/24,127.23.1.0/24"}
            )
        )

    def test_is_ready_mismatch(self):
        # The relation is not ready if database settings (such as the
        # database name) have not been mirrored back.
        for k in ["database", "roles", "extensions"]:
            with self.subTest(f"{k} mismatch"):
                # Requested setting should be available in application
                # shared data. This could be leadership data or a peer
                # relation application databag.
                self.assertFalse(
                    client._is_ready(
                        self.log,
                        {k: "value"},
                        {"egress-subnets": "127.23.0.0/24"},
                        {"allowed-subnets": "127.23.1.0/24"},
                    )
                )
                self.assertFalse(
                    client._is_ready(
                        self.log,
                        {k: "value"},
                        {"egress-subnets": "127.23.0.0/24"},
                        {"allowed-subnets": "127.23.1.0/24", k: "different"},
                    )
                )

    def test_is_ready_match(self):
        # The relation is ready if its egress has been allowed access and its
        # settings have been mirrored back, indicating they have been applied.
        app = {}
        loc = {"egress-subnets": "127.0.0.0/24"}
        rel = {"allowed-subnets": "127.0.0.0/24"}
        for k in ["database", "roles", "extensions"]:
            with self.subTest(f"{k} match"):
                # Requested setting should be available in application
                # shared data. This could be leadership data or a peer
                # relation application databag.
                app[k] = "value"
                self.assertFalse(client._is_ready(self.log, app, loc, rel))
                rel[k] = "value"
                self.assertTrue(client._is_ready(self.log, app, loc, rel))


class TestPostgreSQLRelationEvent(TestPGSQLBase):
    @patch("pgsql.client._master")
    def test_master(self, master):
        c = "host=master dbname=foo"
        master.return_value = c
        self.assertEqual(self.ev.master, ConnectionString(c))
        master.assert_called_once_with(self.ev.log, self.relation, self.local_unit)

    @patch("pgsql.client._standbys")
    def test_standbys(self, standbys):
        c1 = "host=standby1 dbname=foo"
        c2 = "host=standby2 dbname=foo"
        standbys.return_value = [c1, c2]
        self.assertEqual(self.ev.standbys, [ConnectionString(c1), ConnectionString(c2)])
        standbys.assert_called_once_with(self.ev.log, self.relation, self.local_unit)

    def test_database(self):
        ev = self.ev
        self.harness.set_leader(True)

        self.assertIsNone(ev.database)

        # Leader can change the database
        ev.database = "foo"
        self.assertEqual(ev.database, "foo")

        # It gets stored in leadership settings, where peers can find
        # it, allowing non-leaders to know what database name was
        # requested.
        self.assertIn(self.relation.id, self.leadership_data)
        self.assertIn("database", self.leadership_data[self.relation.id])
        self.assertEqual(self.leadership_data[self.relation.id]["database"], "foo")

        # It gets stored in application relation data, informing the server.
        self.assertIn("database", self.relation.data[self.local_unit.app])
        self.assertEqual(self.relation.data[self.local_unit.app]["database"], "foo")

        # It gets mirrored to unit relation data, for backwards with older servers.
        self.assertIn("database", self.relation.data[self.local_unit])
        self.assertEqual(self.relation.data[self.local_unit]["database"], "foo")

        ev.database = None
        self.assertIsNone(ev.database)
        self.assertNotIn("database", self.relation.data[self.local_unit.app])
        self.assertNotIn("database", self.relation.data[self.local_unit])

    def test_database_non_leader(self):
        ev = self.ev
        self.harness.set_leader(False)

        # Non leaders can read the requested database name, pulling it
        # from leadership settings.
        self.assertIsNone(ev.database)
        self.leadership_data[self.relation.id] = {"database": "foo"}
        self.assertEqual(ev.database, "foo")

        # Only the leader can set the property
        with self.assertRaises(ops.model.RelationDataError):
            ev.database = "bar"

    def test_roles(self):
        ev = self.ev
        self.harness.set_leader(True)

        self.assertEqual(ev.roles, [])

        # Leader can request database roles to be automatically created
        ev.roles = {"foo", "bar"}  # unsorted
        self.assertEqual(ev.roles, ["bar", "foo"])  # sorted

        # It gets stored in leadership settings, where peers can find
        # it, allowing non-leaders to know what roles where requested.
        self.assertIn(self.relation.id, self.leadership_data)
        self.assertIn("roles", self.leadership_data[self.relation.id])
        self.assertEqual(self.leadership_data[self.relation.id]["roles"], "bar,foo")

        # It gets stored in application relation data, informing the server.
        self.assertIn("roles", self.relation.data[self.local_unit.app])
        self.assertEqual(self.relation.data[self.local_unit.app]["roles"], "bar,foo")

        # It gets mirrored to unit relation data, for backwards with older servers.
        self.assertIn("roles", self.relation.data[self.local_unit])
        self.assertEqual(self.relation.data[self.local_unit]["roles"], "bar,foo")

        ev.roles = []
        self.assertEqual(ev.roles, [])
        self.assertNotIn("roles", self.relation.data[self.local_unit.app])
        self.assertNotIn("roles", self.relation.data[self.local_unit])

        ev.roles = None
        self.assertEqual(ev.roles, [])
        self.assertNotIn("roles", self.relation.data[self.local_unit.app])
        self.assertNotIn("roles", self.relation.data[self.local_unit])

    def test_roles_non_leader(self):
        ev = self.ev
        self.harness.set_leader(False)

        # Non leaders can read the requested roles, pulling from
        # leadership settings, which allows them to tell when the
        # server has created them.
        self.assertEqual(ev.roles, [])
        self.leadership_data[self.relation.id] = {"roles": "bar,foo"}
        self.assertEqual(ev.roles, ["bar", "foo"])

        # Only the leader can set the property
        with self.assertRaises(ops.model.RelationDataError):
            ev.roles = ["bar"]

    def test_extensions(self):
        ev = self.ev
        self.harness.set_leader(True)

        self.assertEqual(ev.extensions, [])

        # Leader can request database extensions to be installed into
        # the provided database.
        ev.extensions = {"foo", "bar"}  # unsorted
        self.assertEqual(ev.extensions, ["bar", "foo"])  # sorted

        # It gets stored in leadership settings, where peers can find
        # it, allowing non-leaders to know what extensions where requested.
        self.assertIn(self.relation.id, self.leadership_data)
        self.assertIn("extensions", self.leadership_data[self.relation.id])
        self.assertEqual(self.leadership_data[self.relation.id]["extensions"], "bar,foo")

        # It gets stored in application relation data, informing the server.
        self.assertIn("extensions", self.relation.data[self.local_unit.app])
        self.assertEqual(self.relation.data[self.local_unit.app]["extensions"], "bar,foo")

        # It gets mirrored to unit relation data, for backwards with older servers.
        self.assertIn("extensions", self.relation.data[self.local_unit])
        self.assertEqual(self.relation.data[self.local_unit]["extensions"], "bar,foo")

        ev.extensions = None
        self.assertEqual(ev.roles, [])
        self.assertNotIn("extensions", self.relation.data[self.local_unit.app])
        self.assertNotIn("extensions", self.relation.data[self.local_unit])

        ev.extensions = []
        self.assertEqual(ev.roles, [])
        self.assertNotIn("extensions", self.relation.data[self.local_unit.app])
        self.assertNotIn("extensions", self.relation.data[self.local_unit])

    def test_extensions_non_leader(self):
        ev = self.ev
        self.harness.set_leader(False)

        # Non leaders can read the requested roles, pulling from
        # leadership settings, which allows them to tell when the
        # server has created them.
        self.assertEqual(ev.extensions, [])
        self.leadership_data[self.relation.id] = {"extensions": "bar,foo"}
        self.assertEqual(ev.extensions, ["bar", "foo"])

        # Only the leader can set the property
        with self.assertRaises(ops.model.RelationDataError):
            ev.extensions = ["bar"]

    def test_snapshot_and_restore(self):
        # The snapshot and restore methods provide the interface used
        # by the Operator Framework to serialize objects. In particular,
        # it is how our event gets stored when the charm defers it.
        org = self.ev
        self.harness.framework.save_snapshot(org)
        new = self.harness.framework.load_snapshot(org.handle)
        self.assertEqual(org._local_unit, new._local_unit)  # PostgreSQLRelationEvent attribute
        self.assertIs(org.app, new.app)  # RelationEvent parent class attribute


class TestPostgreSQLClient(TestPGSQLBase):
    def setUp(self):
        super().setUp()
        with self.harness.hooks_disabled():
            self.harness.set_leader(True)
            d = self.harness.get_relation_data(self.relation_id, self.local_unit.name)
            d["egress-subnets"] = "127.1.2.3/24"
            for ru in self.remote_unit_names:
                d = self.harness.get_relation_data(self.relation_id, ru)
                d["allowed-subnets"] = "127.1.2.3/24"

    def set_master(self, master_connstr):
        # Set the master, and don't touch the standbys.
        self.harness.update_relation_data(self.relation_id, self.remote_app.name, {"master": str(master_connstr or "")})

    def set_standbys(self, *standby_connstrs):
        # Set the standbys, and don't touch the masters.
        self.harness.update_relation_data(
            self.relation_id, self.remote_app.name, {"standbys": "\n".join(str(c) for c in standby_connstrs)}
        )

    def set_dbs(self, master_connstr, *standby_connstrs):
        # Set both the master and the standbys.
        self.harness.update_relation_data(
            self.relation_id,
            self.remote_app.name,
            {"master": str(master_connstr or ""), "standbys": "\n".join(str(c) for c in standby_connstrs)},
        )

    def assert_only_events(self, *event_names):
        # Iterate over all events, ensuring only the listed ones
        # have been set in our mock charm. We do this to ensure
        # that not only are the events we expect have been emitted,
        # but also importantly that events we *don't* expect have
        # *not* been emitted.
        for n in self.charm.db.on.events():
            is_set = getattr(self.charm, f"{n}_event") is not None
            if n in event_names:
                self.assertTrue(is_set, f"{n}_event should be set")
                count = getattr(self.charm, f"{n}_count")
                self.assertEqual(count, 1, f"{n}_event was emitted {count} times, expected 1")
            else:
                self.assertFalse(is_set, f"{n}_event should not be set")

    def test_master_available(self):
        # When the master becomes available,
        # MasterAvailableEvent, DatabaseAvailableEvent,
        # MasterChangedEvent and DatabaseChangedEvent are emitted.
        self.charm.reset()
        ev_names = ["master_available", "database_available", "master_changed", "database_changed"]

        master_c = ConnectionString("dbname=master")
        self.set_master(master_c)

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"master available triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertEqual(ev.master, master_c)
                self.assertEqual(ev.standbys, [])

    def test_standby_available(self):
        # When the standby becomes available,
        # StandbyAvailableEvent, DatabaseAvailableEvent,
        # StandbyChangedEvent and DatabaseChangedEvent are emitted.
        self.charm.reset()
        ev_names = ["standby_available", "database_available", "standby_changed", "database_changed"]

        standby_c = ConnectionString("dbname=standby")
        self.set_standbys(standby_c)

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"standby available triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertEqual(ev.standbys, [standby_c])
                self.assertIsNone(ev.master)

    def test_databases_available(self):
        # When both master and standby become available, all the
        # [...]Available events are emitted.
        self.charm.reset()
        ev_names = [
            "master_available",
            "standby_available",
            "database_available",
            "master_changed",
            "standby_changed",
            "database_changed",
        ]

        master_c = ConnectionString("dbname=master")
        standby1_c = ConnectionString("dbname=standby1")
        standby2_c = ConnectionString("dbname=standby2")

        self.set_dbs(master_c, standby1_c, standby2_c)

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"master & standby available triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertEqual(ev.master, master_c)
                self.assertEqual(ev.standbys, [standby1_c, standby2_c])

    def test_master_changed(self):
        # When the master connection string is changed,
        # MasterChangedEvent and DatabaseChangedEvent are emitted.
        master_c = ConnectionString("dbname=master")
        standby_c = ConnectionString("dbname=standby")
        self.set_dbs(ConnectionString("dbname=org_master"), standby_c)
        self.charm.reset()
        ev_names = ["master_changed", "database_changed"]

        self.set_master(master_c)

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"master change triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertEqual(ev.master, master_c)
                self.assertEqual(ev.standbys, [standby_c])

    def test_standby_changed(self):
        # When the master connection string is changed,
        # MasterChangedEvent and DatabaseChangedEvent are emitted.
        master_c = ConnectionString("dbname=master")
        standby_c = ConnectionString("dbname=standby")
        self.set_dbs(master_c, ConnectionString("dbname=org_standby"))
        self.charm.reset()
        ev_names = ["standby_changed", "database_changed"]

        self.set_standbys(standby_c)

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"standby change triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertEqual(ev.master, master_c)
                self.assertEqual(ev.standbys, [standby_c])

    def test_databases_changed(self):
        # When the master and standby connection strings are changed,
        # all the [...]ChangedEvents are emitted.
        self.set_dbs(ConnectionString("dbname=org_master"), ConnectionString("dbname=org_standby"))
        self.charm.reset()
        ev_names = ["master_changed", "standby_changed", "database_changed"]

        master_c = ConnectionString("dbname=master")
        standby_c = ConnectionString("dbname=standby")
        self.set_dbs(master_c, standby_c)

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"standby change triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertEqual(ev.master, master_c)
                self.assertEqual(ev.standbys, [standby_c])

    def test_master_gone_no_standbys(self):
        # When the master connection string disappears, and there are
        # no standbys, all of MasterGoneEvent, MasterChangedEvent,
        # DatabaseGoneEvent and DatabaseChangedEvent are emitted.
        master_c = ConnectionString("dbname=master")
        self.set_master(master_c)
        self.charm.reset()
        ev_names = ["master_gone", "database_gone", "master_changed", "database_changed"]

        self.set_master(None)

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"master gone triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertIsNone(ev.master)
                self.assertEqual(ev.standbys, [])

    def test_master_gone_standbys_remain(self):
        # When the master connection string disappears, but standbys
        # remain, all of MasterGoneEvent, MasterChangedEvent and
        # DatabaseChangedEvent are emitted. DatabaseGoneEvent is not
        # emitted.
        master_c = ConnectionString("dbname=master")
        standby_c = ConnectionString("dbname=standby")
        self.set_dbs(master_c, standby_c)
        self.charm.reset()
        ev_names = ["master_gone", "master_changed", "database_changed"]

        self.set_master(None)

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"master gone triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertIsNone(ev.master)
                self.assertEqual(ev.standbys, [standby_c])

    def test_standbys_gone_no_master(self):
        # When a standby connection string disappears, but the master
        # remains, all of StandbyGoneEvent, StrandbyChangedEvent,
        # and DatabaseChangedEvent are emitted. DatabaseGoneEvent is
        # not emitted.
        standby_c = ConnectionString("dbname=standby")
        self.set_standbys(standby_c)
        self.charm.reset()
        ev_names = ["standby_gone", "database_gone", "standby_changed", "database_changed"]

        self.set_standbys()

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"standby gone triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertIsNone(ev.master, None)
                self.assertEqual(ev.standbys, [])

    def test_standbys_gone_master_remains(self):
        # When a standby connection string disappears, but the master
        # remains, all of StandbyGoneEvent, StrandbyChangedEvent,
        # and DatabaseChangedEvent are emitted. DatabaseGoneEvent is
        # not emitted.
        master_c = ConnectionString("dbname=master")
        standby_c = ConnectionString("dbname=standby")
        self.set_dbs(master_c, standby_c)
        self.charm.reset()
        ev_names = ["standby_gone", "standby_changed", "database_changed"]

        self.set_standbys()

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"standby gone triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertEqual(ev.master, master_c)
                self.assertEqual(ev.standbys, [])

    def test_standbys_and_master_gone(self):
        # When both the master and standbys disappear, all of
        # MasterGoneEvent, StandbyGoneEvent, DatabaseGoneEvent,
        # MasterChangedEvent, StandbyChangedEvent and
        # DatabaseChangedEvent are emitted.
        master_c = ConnectionString("dbname=master")
        standby_c = ConnectionString("dbname=standby")
        self.set_dbs(master_c, standby_c)
        self.charm.reset()
        ev_names = [
            "master_gone",
            "standby_gone",
            "database_gone",
            "master_changed",
            "standby_changed",
            "database_changed",
        ]

        self.set_dbs(None)

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"standby gone triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertEqual(ev.master, None)
                self.assertEqual(ev.standbys, [])

    def test_not_ready(self):
        # When the remote DB stops being ready for some reason,
        # it counts as gone.
        master_c = ConnectionString("dbname=master")
        standby_c = ConnectionString("dbname=standby")
        self.set_dbs(master_c, standby_c)
        self.charm.reset()
        ev_names = [
            "master_gone",
            "standby_gone",
            "database_gone",
            "master_changed",
            "standby_changed",
            "database_changed",
        ]

        # No longer providing the requested database, no longer ready.
        self.harness.update_relation_data(self.relation_id, self.remote_app.name, {"database": "foo"})

        self.assert_only_events(*ev_names)

        for ev_name in ev_names:
            with self.subTest(f"unready relation triggers {ev_name}"):
                ev = getattr(self.charm, f"{ev_name}_event")
                self.assertIsNone(ev.master)
                self.assertEqual(ev.standbys, [])

    def test_relation_joined(self):
        # The DatabaseRelationJoined event is emitted when the
        # relation is joined, at the same time as the standard
        # Operator Framework relation joined event for the relation.
        # The DatabaseRelationJoined event is more useful though,
        # providing the necessary methods to configure the PostgreSQL
        # relation. The DatabaseRelationJoined event has already been
        # fired when the test harness was initialized.
        self.assert_only_events("database_relation_joined")

    def test_relation_broken(self):
        # Departing the relation triggers the DatabaseRelationBroken
        # event, which is probably only useful for updating the
        # Charm's workload status.
        self.charm.reset()
        self.charm.on.db_relation_broken.emit(self.relation)
        self.assert_only_events("database_relation_broken")

    def test_relation_broken_master(self):
        # MasterChanged, DatabaseChanged, MasterGone and DatabaseGone
        # events are emitted if the master was available when the
        # relation is lost.
        self.set_master(ConnectionString("dbname=master"))
        self.charm.reset()
        self.charm.on.db_relation_broken.emit(self.relation)
        self.assert_only_events(
            "database_relation_broken", "master_changed", "database_changed", "master_gone", "database_gone"
        )

    def test_relation_broken_standby(self):
        # MasterChanged, DatabaseChanged, MasterGone and DatabaseGone
        # events are emitted if the master was available when the
        # relation is lost.
        self.set_standbys(ConnectionString("dbname=standby"))
        self.charm.reset()
        self.charm.on.db_relation_broken.emit(self.relation)
        self.assert_only_events(
            "database_relation_broken", "standby_changed", "database_changed", "standby_gone", "database_gone"
        )

    def test_relation_broken_full(self):
        # MasterChanged, DatabaseChanged, MasterGone and DatabaseGone
        # events are emitted if the master was available when the
        # relation is lost.
        self.set_dbs(ConnectionString("dbname=master"), ConnectionString("dbname=standby"))
        self.charm.reset()
        self.charm.on.db_relation_broken.emit(self.relation)
        self.assert_only_events(
            "database_relation_broken",
            "master_changed",
            "standby_changed",
            "database_changed",
            "master_gone",
            "standby_gone",
            "database_gone",
        )
