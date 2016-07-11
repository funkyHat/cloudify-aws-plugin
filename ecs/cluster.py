from botocore.exceptions import ClientError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from ecs import connection


# TODO: Make this not be bucketty
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

    try:
        ecs_client.delete_bucket(cluster=arn)
    except ClientError as err:
        raise NonRecoverableError(
            'Cluster deletion failed: {}'.format(err.message)
        )
