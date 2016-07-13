from botocore.exceptions import ClientError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from ecs import connection


def get_target_attribute(relationships, from_relationship, desired_attribute):
    arn = None

    for relationship in relationships:
        if relationship.type == from_relationship:
            arn = relationship.target.instance.runtime_properties.get(
                desired_attribute,
                None
            )
            break

    return arn


def get_arn(relationships, from_relationship):
    return get_target_attribute(
        relationships=relationships,
        from_relationship=from_relationship,
        desired_attribute='arn',
    )


def get_container_instance_count(relationships):
    return get_target_attribute(
        relationships=relationships,
        from_relationship=(
            'cloudify.aws.relationships.ecs_service_running_on_cluster'
        ),
        desired_attribute='instances',
    )


@operation
def create(ctx):
    ecs_client = connection.ECSConnectionClient().client()

    # Don't try to do anything until the cluster has instances...
    # Worth noting that this will
    container_instances = get_container_instance_count(
        ctx.instance.relationships,
    )
    if len(container_instances) == 0 or container_instances is None:
        return ctx.operation.retry(
            'Waiting for cluster to have available instances.',
        )

    cluster_arn = get_arn(
        ctx.instance.relationships,
        'cloudify.aws.relationships.ecs_service_running_on_cluster',
    )
    task_arn = get_arn(
        ctx.instance.relationships,
        'cloudify.aws.relationships.ecs_service_runs_task',
    )

    if None in (cluster_arn, task_arn):
        raise NonRecoverableError(
            'Could not create Service {service}. ECS Services must have '
            'relationships to both a cluster '
            '(cloudify.aws.relationships.ecs_service_running_on_cluster) '
            'and a task '
            '(cloudify.aws.relationships.ecs_service_runs_task). '
            'Related cluster was {cluster}. '
            'Related task was {task}.'.format(
                service=ctx.node.properties['name'],
                cluster=cluster_arn,
                task=task_arn,
            )
        )

    response = ecs_client.create_service(
        cluster=cluster_arn,
        serviceName=ctx.node.properties['name'],
        desiredCount=ctx.node.properties['desired_count'],
        taskDefinition=task_arn,
        #clientToken=ctx.instance.id,
    )
    ctx.instance.runtime_properties['arn'] = response['service']['serviceArn']


@operation
def delete(ctx):
    cluster_arn = get_arn(
        ctx.instance.relationships,
        'cloudify.aws.relationships.ecs_service_running_on_cluster',
    )
    arn = ctx.instance.runtime_properties.get('arn', None)

    if arn is None:
        raise NonRecoverableError(
            'Service {service} creation failed, so it will not be '
            'deleted.'.format(service=ctx.node.properties['name'])
        )

    ecs_client = connection.ECSConnectionClient().client()

    try:
        ecs_client.update_service(service=arn, cluster=cluster_arn,
                                  desiredCount=0)
        ecs_client.delete_service(service=arn, cluster=cluster_arn)
    except ClientError as err:
        raise NonRecoverableError(
            'Service deletion failed: {}'.format(err.message)
        )
