from botocore.exceptions import ClientError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from ecs import connection


def get_appropriate_relationship_targets(
    relationships,
    target_relationship,
    target_node,
):
    results = []
    for relationship in relationships:
        if relationship.type == target_relationship:
            if relationship.target.node.type == target_node:
                results.append(relationship.target.node)
            else:
                raise NonRecoverableError(
                    '{rel} may only be made against nodes of type {correct}, '
                    'but was made against node type {actual}'.format(
                        rel=target_relationship,
                        correct=target_node,
                        actual=relationship.target.node.type,
                    )
                )
    return results


def construct_volume_definitions(ctx):
    volumes = get_appropriate_relationship_targets(
        relationships=ctx.instance.relationships,
        target_relationship='cloudify.aws.relationships.ecs_volume_for_task',
        target_node='cloudify.aws.nodes.ECSVolume',
    )

    return [
        {'name': volume.properties['name']}
        for volume in volumes
    ]


def construct_container_definitions(ctx):
    containers = get_appropriate_relationship_targets(
        relationships=ctx.instance.relationships,
        target_relationship=(
            'cloudify.aws.relationships.ecs_container_for_task'
        ),
        target_node='cloudify.aws.nodes.ECSContainer',
    )

    container_definitions = []

    for container in containers:
        props = container.properties

        definition = {
            'name': props['name'],
            'image': props['image'],
            'memory': props['memory'],
        }

        # We will trust the container node to have validated itself
        # We will, however, take pains not to include any parameters that have
        # not been set, and instead let AWS defaults rule in those cases

        # Set up port mappings
        tcp_mappings = props['tcp_port_mappings']
        udp_mappings = props['udp_port_mappings']
        port_mappings = [
            {
                'containerPort': int(container_port),
                'hostPort': int(host_port),
                'protocol': 'tcp',
            }
            for host_port, container_port in tcp_mappings.items()
        ]
        port_mappings.extend([
            {
                'containerPort': int(container_port),
                'hostPort': int(host_port),
                'protocol': 'udp',
            }
            for host_port, container_port in udp_mappings.items()
        ])
        if len(port_mappings) > 0:
            definition['portMappings'] = port_mappings

        # Using -1 as 'not set'
        if props['cpu_units'] > -1:
            definition['cpu'] = props['cpu_units']

        definition['essential'] = props['essential']

        if props['entrypoint'] != []:
            definition['entryPoint'] = props['entrypoint']

        if props['command'] != []:
            definition['command'] = props['command']

        if props['workdir'] != '':
            definition['workingDirectory'] = props['workdir']

        if len(props['env_vars']) > 0:
            definition['environment'] = [
                {
                    'name': name,
                    'value': value,
                }
                for name, value in props['env_vars'].items()
            ]

        if props['disable_networking'] is not None:
            definition['disableNetworking'] = props['disable_networking']

        if len(props['links']) > 0:
            definition['links'] = props['links']

        if props['hostname'] != '':
            definition['hostname'] = props['hostname']

        if len(props['dns_servers']) > 0:
            definition['dnsServers'] = props['dns_servers']

        if len(props['dns_search_domains']) > 0:
            definition['dnsSearchDomains'] = props['dns_search_domains']

        if len(props['extra_hosts_entries']) > 0:
            definition['extraHosts'] = [
                {
                    'hostname': host,
                    'ipAddress': ip,
                }
                for host, ip in props['extra_hosts_entries'].items()
            ]

        definition['readonlyRootFilesystem'] = (
            props['read_only_root_filesystem']
        )

        if len(props['mount_points']) > 0:
            definition['mountPoints'] = props['mount_points']

        if len(props['volumes_from']) > 0:
            definition['volumesFrom'] = props['volumes_from']

        if props['log_driver'] != '':
            definition['logConfiguration'] = {
                'logConfiguration': props['log_driver'],
            }
            if len(props['log_driver_options']) > 0:
                definition['logConfiguration']['options'] = (
                    props['log_driver_options']
                )

        definition['privileged'] = props['privileged']

        if props['user'] != '':
            definition['username'] = props['user']

        if len(props['security_options']) > 0:
            definition['dockerSecurityOptions'] = props['security_options']

        if len(props['ulimits']) > 0:
            definition['ulimits'] = props['ulimits']

        if len(props['docker_labels']) > 0:
            definition['dockerLabels'] = props['docker_labels']

        container_definitions.append(definition)

    return container_definitions


@operation
def create(ctx):
    ctx.instance.runtime_properties['arn'] = None

    containers = construct_container_definitions(ctx)

    ctx.instance.runtime_properties['container_names'] = [
        container['name'] for container in containers
    ]

    volumes = construct_volume_definitions(ctx)

    task_definition = {
        'family': ctx.node.properties['name'],
        'containerDefinitions': containers,
        'volumes': volumes,
    }

    ecs_client = connection.ECSConnectionClient().client()
    result = ecs_client.register_task_definition(**task_definition)

    ctx.instance.runtime_properties['arn'] = (
        result['taskDefinition']['taskDefinitionArn']
    )


@operation
def delete(ctx):
    task = ctx.node.properties['name']

    if ctx.instance.runtime_properties.get('arn', None) is None:
        raise NonRecoverableError(
            'Task {task} was not created by this deployment so will not be '
            'deleted.'.format(task=task)
        )

    ecs_client = connection.ECSConnectionClient().client()

    try:
        ecs_client.deregister_task_definition(
            taskDefinition=ctx.instance.runtime_properties['arn'],
        )
    except ClientError as err:
        raise NonRecoverableError(
            'Task deletion failed: {}'.format(err.message)
        )
