from botocore.exceptions import ClientError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError
from ec2 import constants as ec2_constants

from ecs import connection


@operation
def create(ctx):
    ecs_client = connection.ECSConnectionClient().client()
    cluster_name = ctx.node.properties['name']

    cluster_exists = False
    cluster_arns = ecs_client.list_clusters()['clusterArns']
    cluster_details = ecs_client.describe_clusters(clusters=cluster_arns)
    clusters = []
    for cluster in cluster_details['clusters']:
        clusters.append('clusterArn')
        clusters.append('clusterName')
    if cluster_name in clusters:
        cluster_exists = True

    if ctx.node.properties['use_existing_resource']:
        if cluster_exists:
            return True
        else:
            raise NonRecoverableError(
                'Attempt to use existing cluster {cluster} failed, as no '
                'cluster by that name exists.'.format(cluster=cluster_name)
            )
    else:
        if cluster_exists:
            raise NonRecoverableError(
                'Cluster {cluster} already exists, but use_existing_resource '
                'is not set to true.'.format(cluster=cluster_name)
            )

    response = ecs_client.create_cluster(
        clusterName=ctx.node.properties['name'],
    )
    ctx.instance.runtime_properties['arn'] = response['cluster']['clusterArn']
    ctx.instance.runtime_properties['instances'] = []


@operation
def delete(ctx):
    if ctx.node.properties['use_existing_resource']:
        return True

    arn = ctx.instance.runtime_properties.get('arn', None)

    if arn is None:
        raise NonRecoverableError(
            'Cluster {cluster} creation failed, so it will not be '
            'deleted.'.format(cluster=ctx.node.properties['name'])
        )

    ecs_client = connection.ECSConnectionClient().client()

    container_arns = ecs_client.list_container_instances(
        cluster=arn,
    ).get('containerInstanceArns', [])

    for container_arn in container_arns:
        ctx.logger.warn(
            'Cluster still has attached container instances. '
            'Deregistering {arn}.'.format(arn=container_arn)
        )
        ecs_client.deregister_container_instance(
            cluster=arn,
            containerInstance=container_arn,
        )

    try:
        ecs_client.delete_cluster(cluster=arn)
    except ClientError as err:
        raise NonRecoverableError(
            'Cluster deletion failed: {}'.format(err.message)
        )


def get_container_instance_arn_from_ec2_id(ec2_id, cluster_arn):
    ecs_client = connection.ECSConnectionClient().client()

    container_instance_arns = ecs_client.list_container_instances(
        cluster=cluster_arn,
    ).get('containerInstanceArns', [])

    if container_instance_arns == []:
        # Boto is aggravating and will traceback on an empty list
        container_instance_arns = ['this_stops_an_error_on_the_next_call']
    container_instances = ecs_client.describe_container_instances(
        cluster=cluster_arn,
        containerInstances=container_instance_arns,
    ).get('containerInstances', [])
    container_instance_mapping = {
        item['ec2InstanceId']: item['containerInstanceArn']
        for item in container_instances
    }

    if ec2_id in container_instance_mapping.keys():
        return container_instance_mapping[ec2_id]
    else:
        return None


@operation
def add_container_instance(ctx):
    aws_id = ctx.source.instance.runtime_properties.get(
        ec2_constants.EXTERNAL_RESOURCE_ID,
        None
    )
    if aws_id is None:
        # TODO: I think this might be nonrecoverable?
        return ctx.operation.retry(
            message='Could not retrieve EC2 ID from instance.'
        )
    else:
        instance_arn = get_container_instance_arn_from_ec2_id(
            ec2_id=aws_id,
            cluster_arn=ctx.target.instance.runtime_properties['arn'],
        )
        if instance_arn is None:
            # We're not registered with the cluster yet.
            # This is something the (correct) AMI will do by itself.
            return ctx.operation.retry(
                message='Waiting for EC2 instance to register with cluster.'
            )

        # We store the ec2 ID so that we can look up and delete the associated
        # ARN when removing the container instance later if it fails to.
        # It has failed to more than once in initial testing.
        # We have to do the little retrieve,append,replace dance because
        # trying to just append on the object results in no instances in the
        # list.
        instances = ctx.target.instance.runtime_properties['instances']
        instances.append(aws_id)
        ctx.target.instance.runtime_properties['instances'] = instances


@operation
def remove_container_instance(ctx):
    aws_id = ctx.source.instance.runtime_properties.get(
        ec2_constants.EXTERNAL_RESOURCE_ID,
        None
    )
    if aws_id is None:
        raise NonRecoverableError(
            'AWS instance details missing from context. '
            'Instance may not have been created.'
        )
    else:
        instance_arn = get_container_instance_arn_from_ec2_id(
            ec2_id=aws_id,
            cluster_arn=ctx.target.instance.runtime_properties['arn'],
        )
        if instance_arn is not None:
            ecs_client = connection.ECSConnectionClient().client()
            ecs_client.deregister_container_instance(
                cluster=ctx.target.instance.runtime_properties['arn'],
                containerInstance=instance_arn,
            )

        ctx.target.instance.runtime_properties['instances'].remove(aws_id)
