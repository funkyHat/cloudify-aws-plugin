from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError


@operation
def validate(ctx):
    # Check properties are valid

    # Check we have a relationship with a task
    rel = 'cloudify.aws.relationships.ecs_volume_for_task'
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
