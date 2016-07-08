from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

import socket


def _is_ip_address(candidate):
    ip_address = False
    try:
        socket.inet_pton(socket.AF_INET, candidate)
        ip_address = True
    except socket.error:
        # Not IPv4
        pass
    try:
        socket.inet_pton(socket.AF_INET6, candidate)
        ip_address = True
    except socket.error:
        # Not IPv6
        pass
    return ip_address


@operation
def validate(ctx):
    # Check properties are valid
    if ctx.node.properties['memory'] < 4:
        raise NonRecoverableError(
            'ECSContainers must have at least 4MB of RAM assigned.'
        )

    tcp_port_mappings = ctx.node.properties['tcp_port_mappings']
    udp_port_mappings = ctx.node.properties['udp_port_mappings']
    # Check port mappings
    if len(tcp_port_mappings) + len(udp_port_mappings) > 100:
        raise NonRecoverableError(
            'At most 100 TCP and UDP port mappings may be specified.'
        )
    for port_mappings, proto in ((tcp_port_mappings, 'tcp'),
                                 (udp_port_mappings, 'udp')):
        for host, container in port_mappings.items():
            try:
                container = int(container)
                host = int(host)

                restricted_ports = (22, 2375, 2376, 51678)
                valid = False
                if (
                    1 <= container <= 65535 and
                    1 <= host <= 65535 and
                    host not in restricted_ports
                ):
                    valid = True

                if not valid:
                    raise NonRecoverableError(
                        'A port mapping in {proto}_port_mappings did not '
                        'refer to a valid port. Valid ports are between '
                        '1 and 65535, and host ports may not be in the '
                        'restricted ports list ( '
                        '{restricted_ports} ). Error occurred in mapping : '
                        '{container}: {host}'.format(
                            proto=proto,
                            restricted_ports=','.join([
                                str(port) for port in restricted_ports
                            ]),
                            container=container,
                            host=host,
                        )
                    )

            except ValueError:
                raise NonRecoverableError(
                    'A parameter specified in {proto}_port_mappings could '
                    'not be interpreted as an integer. Error occurred in: '
                    '{container}:{host}'.format(
                        proto=proto,
                        container=container,
                        host=host,
                    )
                )

    for link in ctx.node.properties['links']:
        if len(link.split(':')) != 2:
            raise NonRecoverableError(
                'links must be specified as a list of strings in the form '
                '<name>:<alias>. There should be no other : in the line. '
                '{link} was invalid.'.format(link=link)
            )

    for server in ctx.node.properties['dns_servers']:
        if not _is_ip_address(server):
            raise NonRecoverableError(
                'dns_servers must be a list of IP addresses. '
                '{server} does not appear to be an IP address.'.format(
                    server=server,
                )
            )

    for hostname, ip in ctx.node.properties['extra_hosts_entries'].items():
        if not _is_ip_address(ip):
            raise NonRecoverableError(
                'extra_hosts_entries must be a dictionary mapping host names '
                'to IPs. "{host}: {ip}" does not map to a valid IP '
                'address.'.format(
                    host=hostname,
                    ip=ip,
                )
            )

    for prop in ('mount_points', 'volumes_from', 'ulimits'):
        for entry in ctx.node.properties[prop]:
            if not isinstance(entry, dict):
                raise NonRecoverableError(
                    '{prop} must be a list of dictionaries'.format(
                        prop=prop,
                    )
                )
            problems = []
            keys = set(entry.keys())
            if prop == 'mount_points':
                expected_keys = [
                    'sourceVolume',
                    'containerPath',
                    'readOnly',
                ]
            elif prop == 'volumes_from':
                expected_keys = [
                    'sourceContainer',
                    'readOnly',
                ]
            elif prop == 'ulimits':
                expected_keys = [
                    'name',
                    'softLimit',
                    'hardLimit',
                ]
            expected_keys = set(expected_keys)
            missing_keys = expected_keys - keys
            extra_keys = keys - expected_keys
            for key in missing_keys:
                problems.append('missing {key} key'.format(key=key))
            for key in extra_keys:
                problems.append('found unknown key {key}'.format(key=key))
            if prop == 'ulimits':
                valid_ulimits = [
                    'core',
                    'cpu',
                    'data',
                    'fsize',
                    'locks',
                    'memlock',
                    'msgqueue',
                    'nice',
                    'nofile',
                    'nproc',
                    'rss',
                    'rtprio',
                    'rttime',
                    'sigpending',
                    'stack',
                ]
                if 'name' in keys and entry['name'] not in valid_ulimits:
                    problems.append(
                        'ulimit name must be one of {valid}'.format(
                            valid=','.join(valid_ulimits)
                        )
                    )
                for limit in 'softLimit', 'hardLimit':
                    if limit in keys and not isinstance(entry[limit], int):
                        problems.append('{limit} must be an integer'.format(
                            limit=limit,
                        ))
            if 'readOnly' in keys:
                if not isinstance(entry['readOnly'], bool):
                    problems.append(
                        'readOnly key must be true or false (boolean), but '
                        'was set to {value}'.format(value=entry['readOnly'])
                    )
            if problems:
                raise NonRecoverableError(
                    'Bad entry found in {section}: {problems}'.format(
                        section=prop,
                        problems=';'.join(problems),
                    )
                )

    if ctx.node.properties['log_driver'] != '':
        valid_drivers = [
            'json-file',
            'syslog',
            'journald',
            'gelf',
            'fluentd',
            'awslogs',
        ]
        if ctx.node.properties['log_driver'] not in valid_drivers:
            raise NonRecoverableError(
                'log_driver must be one of {valid}'.format(
                    valid=valid_drivers,
                )
            )

    # Check we have a relationship with a task
    # TODO: We could really do with validating this in some way
    return
    rel = 'cloudify.aws.relationships.ecs_container_for_task'
    node_type = 'cloudify.aws.nodes.ECSTask'
    valid_relationships = 0
    for relationship in ctx.instance.relationships:
        if (
            relationship.type == rel and
            relationship.target.node.type == node_type
        ):
            valid_relationships += 1
    if valid_relationships != 1:
        raise NonRecoverableError(
            'ECSContainers must have exactly one relationship of type {rel} '
            'with a node of type {node_type}'.format(
                rel=rel,
                node_type=node_type,
            )
        )
