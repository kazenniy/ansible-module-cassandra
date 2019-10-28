#!/usr/bin/python
# -*- coding: utf-8 -*-


DOCUMENTATION = '''
---
module: cassandra_keyspace

short_description: Manage Cassandra Keyspaces
description:
    - Add/remove Cassandra Keyspaces
    - requires `pip install cassandra-driver`
    - Related Docs: https://datastax.github.io/python-driver/api/cassandra/query.html
    - Related Docs: https://docs.datastax.com/en/archived/cql/3.3/cql/cql_reference/cqlCreateKeyspace.html
author: "Sergey Kazenniy"
options:
  name:
    description:
      - name of the keyspace to add or remove
    required: true
    alias: role
  topology:
    description:
      - Set the keyspace's replication strategy class
    choices: [ "SimpleStrategy", "NetworkTopologyStrategy" ]
    required: true
  datacenter:
    description:
      - Set datacenter name for NetworkTopologyStrategy
    required: false
   replication_factor:
    description:
      - Set Replication strategy factor
    required: false
    default: 1
  durable_writes:
    description:
      - Disable write commit log for the cycling keyspace
    default: true
  login_hosts:
    description:
      - List of hosts to login to Cassandra with
    required: true
  login_user:
    description:
      - The superuser to login to Cassandra with
    required: true
  login_password:
    description:
      - The superuser password to login to Cassandra with
    required: true
  login_port:
    description:
      - Port to connect to cassandra on
    default: 9042
  state:
    description:
      - Whether the role should exist.  When C(absent), removes
        the role.
    required: false
    default: present
    choices: [ "present", "absent" ]

notes:
   - "requires cassandra-driver to be installed"

'''

EXAMPLES = '''
# Create Keyspace
- cassandra_role: name='foo' topology='simple' state=present replication_factor=1 login_hosts=localhost login_pass=cassandra login_user=cassandra

# Remove Keyspace
- cassandra_role: name='foo' state=absent login_hosts=localhost login_pass=cassandra login_user=cassandra
'''

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import dict_factory
except ImportError:
    cassandra_dep_found = False
else:
    cassandra_dep_found = True

GET_KEYSPACE = "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = %s limit 1;"
DROP_KEYSPACE = "DROP KEYSPACE %s"


def keyspace_delete(session, in_check_mode, name):
    existing_keyspace = get_keyspace(session, name)
    if bool(existing_keyspace):
        if not in_check_mode:
            session.execute(DROP_KEYSPACE, [name])
        return True
    else:
        return False


def get_keyspace(session, name):
    rows = session.execute(GET_KEYSPACE, [name])
    for row in rows:
        return row


def keyspace_create(session, check_mode, name, topology, datacenter, replication_factor, durable_writes):
    existing_keyspace = get_keyspace(session, name)
    if bool(existing_keyspace):
        if check_mode:
            return False
    else:
        if check_mode:
            return True
        if topology == 'SimpleStrategy':
            query = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : '%s', 'replication_factor' : '%s' } AND DURABLE_WRITES = %s;"
            session.execute(query % (name, topology, replication_factor, durable_writes))
        else:
            query = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : '%s', '%s' : '%s' } AND DURABLE_WRITES = %s;"
            session.execute(query % (name, topology, datacenter, replication_factor, durable_writes))
    new_keyspace = get_keyspace(session, name)
    return bool(new_keyspace != existing_keyspace)


def main():
    module = AnsibleModule(
        argument_spec={
            'login_user': {
                'required': True,
                'type': 'str'
            },
            'login_password': {
                'required': True,
                'no_log': True,
                'type': 'str'
            },
            'login_hosts': {
                'required': True,
                'type': 'list'
            },
            'login_port': {
                'default': 9042,
                'type': 'int'
            },
            'name': {
                'required': True,
                'aliases': ['keyspace']
            },
            'topology': {
                'required': True,
                'choices': [ "SimpleStrategy", "NetworkTopologyStrategy" ]
            },
            'datacenter': {
                'default': 'datacenter1',
                'type': 'str'
            },
            'replication_factor': {
                'default': 1,
                'type': 'int'
            },
            'durable_writes': {
                'default': True,
                'type': 'bool'
            },
            'state': {
                'default': "present",
                'choices': ["absent", "present"]
            }
        },
        supports_check_mode=True
    )
    login_user = module.params["login_user"]
    login_password = module.params["login_password"]
    login_hosts = module.params["login_hosts"]
    login_port = module.params["login_port"]
    topology = module.params["topology"]
    datacenter = module.params["datacenter"]
    name = module.params["name"]
    replication_factor = module.params["replication_factor"]
    durable_writes = module.params["durable_writes"]
    state = module.params["state"]

    if not cassandra_dep_found:
        module.fail_json(msg="the python cassandra-driver module is required")

    session = None
    changed = False
    try:
        if not login_user:
            cluster = Cluster(login_hosts, port=login_port)

        else:
            auth_provider = PlainTextAuthProvider(username=login_user, password=login_password)
            cluster = Cluster(login_hosts, auth_provider=auth_provider, protocol_version=3, port=login_port)
        session = cluster.connect()
        session.row_factory = dict_factory
    except Exception as e:
        module.fail_json(
            msg="unable to connect to cassandra, check login_user and login_password are correct. Exception message: %s"
                % e)

    if state == "present":
        try:
            changed = keyspace_create(session, module.check_mode, name, topology, datacenter, replication_factor, durable_writes)
        except Exception as e:
            module.fail_json(msg=str(e))
    elif state == "absent":
        try:
            changed = keyspace_delete(session, module.check_mode, name)
        except Exception as e:
            module.fail_json(msg=str(e))
    module.exit_json(changed=changed, name=name)


from ansible.module_utils.basic import *

if __name__ == '__main__':
    main()
