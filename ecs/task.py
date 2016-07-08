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
                        rel=target_relationships,
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
        definition = {
            'name': container.properties['name'],
            'image': container.properties['image'],
            'memory': container.properties['memory'],
        }
        container_definitions.append(definition)

    return container_definitions

@operation
def create(ctx):
    ctx.instance.runtime_properties['arn'] = None

    containers = construct_container_definitions(ctx)
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
