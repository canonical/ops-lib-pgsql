pgsql Endpoint Protocol
=======================

The pgsql Endpoint protocol has evolved over more than a decade. It attempts to minimize
race conditions inherent with simpler protocols by providing a mechanism for clients to
know when the resources provided by the server are actually ready for use.

With a simpler protocol, a client may join a relation, discover server connection details,
and attempt to use them. Maybe the connection works, but often the server still has setup
to perform and the connection will fail. With a simpler protocol, clients do not know if
they should block the hook, retrying until the server becomes available (and creating
problematic long running hooks), or if they should ignore the failure, trying again in a
future hook, or if they have hit a bug that should fail hard and be investigated. The
`pgsql` protocol chooses to mirror requests back to clients, allowing them to determine
if the state the server claims to be in matches the state the client needs it to be in,
and is ready for connections from a specific client unit. Most importantly, it knows that
the database name that was requested has been provided, and that IP level access has been
granted to the client.

The `ops-lib-pgsql` library supports both v1, v2 and most known legacy variants of the
protocol. This can make its code somewhat confusing in parts, but anything not directly
related to the v2 protocol should be clearly marked as legacy code. It is expected that
the legacy protocol support will be dropped after more Charms have migrated to the
Juju Operator Framework or the Juju Reactive Framework and are making use of `ops-lib-pgsql`
or the `interface:pgsql` Reactive Charm Layer.


v2
--

The v2 protocol makes use of Application Relation Data, available with Juju 2.7 and later.
These are two new bags of data on the relation, one for each Application in the relation,
settable by the local leader, and readable by the remote units and the local leader.
Unfortunately, per Juju design, local units cannot read their local Application Relation Data,
placing the burden on the Charm to share this information with the non-leader local units.

The following chart shows the v2 protocol workflow, for a relation endpoint `foo` with the relation id `0`.

<table><thead><tr><th colspan=2>Client</th><th>Server</th></tr></thead>
<tbody>
<tr>
<td>Peer data</td><td>App Relation Data</td><td>App Relation Data</td>
</tr>
<tr>
<td><!-- Peer data -->

```python
{
  0: {  # relation id
    "database": "foo",
    "extensions": "citext,debversion",
    "roles": "pos,management",
  },
}
```

</td>
<td><!-- Client Application Relation data -->

```python
{
  "database": "foo",
  "extensions": "citext,debversion",
  "roles": "pos,management",
  "egress-subnets": "198.51.100.0/24,192.0.2.0/24",
}
```

</td>
<td><!-- Server Application Relation data -->
</td>
</tr>
<tr><td colspan=2></td><td>&downarrow; foo-relation-changed</td></tr>
<td><!-- Peer data -->

```python
{
  0: {  # relation id
    "database": "foo",
    "extensions": "citext,debversion",
    "roles": "pos,management",
  },
}
```

</td>
<td><!-- Client Application Relation data -->

```python
{
  "database": "foo",
  "extensions": "citext,debversion",
  "roles": "pos,management",
  "egress-subnets": "198.51.100.0/24,192.0.2.0/24",
}
```

</td>
<td><!-- Server Application Relation data -->

```python
{
  "database": "foo",
  "extensions": "citext,debversion",
  "roles": "pos,management",
  "allowed-subnets": "192.0.2.0/24,203.0.113.0/24,198.51.100.0/24,192.0.2.0/24",
  "master": "dbname=foo user=username host=prod1 connect_timeout=3",
  "standbys": "dbname=foo user=username host=prod2\ndbname=foo user=username host=prod3",
}
```

</td>
</tr>
<tr><td colspan=2>&downarrow; foo-relation-changed</td><td></td></tr>
</tbody>
</table>

The lead client unit starts, normally in the `foo-relation-created` or `foo-relation-joined` hook,
by setting the client Application's requirements in the Application Relation Data. Supported keys
are `database`, `extensions` and `roles`. More may be added in the future. `roles`
is a comma separated list strings. `extensions` is also a comma seaprated list of strings, but each
item may also specify the database schema to install the extension into using notation such as
`citext:myschema,debversion:public`. The `egress-subnets` setting is maintained by Juju.

The lead server unit responds, triggering a `foo-relation-changed` hook on the clients. It mirrors
back the database settings requested by the lead client unit, identically. It also sets the
`allowed-subnets` key to a comma separated list of all client egress subnet ranges that have been
granted access (both the client lead unit and non-lead units). And the server also publishes the
libpq connection strings to use to connect to the master database in the `master` key, and a
line separated list of standby database connection strings in the `standbys` key.

The lead unit knows when the database is ready for use when the settings have been mirrored back
identically, it finds all of its egress subnets in the `allowed-subnets` key, and any necessary
`master` and `standbys` libpq database connection strings have been provided. The `standbys` key
may be missing if there are no standby databases, and the `master` key may be missing if the master
database is not yet available; clients should wait for it to become available in a future
relation-changed hook.

The non-lead client units know when the databae is ready in a similar manner to the lead unit,
except that they need to examine the shared peer data to determine what settings have been
requested, providing them with the values they need to compare with the server's Application
Relation Data.


v1
--

The v1 protocol is similar to the v2 protocol, except that it pre-dates Application Relation Data.
Rather than the lead units setting Application Relation Data, in v1 all Units must set their
local Unit Relation Data. Unit data for each Application should be eventually consistent.

<table><thead><tr><th>Client</th><th>Server</th></tr></thead>
<tbody>
<tr>
<td>Unit Relation Data</td><td>Unit Relation Data</td>
</tr>
<tr>
<td><!-- Client Unit Relation data -->

```python
{
  "database": "foo",
  "extensions": "citext,debversion",
  "roles": "pos,management",
  "egress-subnets": "198.51.100.0/24,192.0.2.0/24",
}
```

</td>
<td><!-- Server Unit Relation data -->
</td>
</tr>
<tr><td></td><td>&downarrow; foo-relation-changed</td></tr>
<tr>
<td><!-- Client Unit Relation data -->

```python
{
  "database": "foo",
  "extensions": "citext,debversion",
  "roles": "pos,management",
  "egress-subnets": "198.51.100.0/24,192.0.2.0/24",
}
```

</td>
<td><!-- Server Unit Relation data -->

```python
{
  "database": "foo",
  "extensions": "citext,debversion",
  "roles": "pos,management",
  "allowed-subnets": "192.0.2.0/24,203.0.113.0/24,198.51.100.0/24,192.0.2.0/24",
  "master": "dbname=foo user=username host=prod1 connect_timeout=3",
  "standbys": "dbname=foo user=username host=prod2\ndbname=foo user=username host=prod3",
}
```

</td>
</tr>
<tr><td>&downarrow; foo-relation-changed</td><td></td></tr>
</tbody>
</table>

v0 (legacy)
------------

v0 protocol should not be used as it does not support Juju Cross Model Relations.
It additionally does not support proxies such as pgbouncer being inserted between
the client and the PostgreSQL server Application.
