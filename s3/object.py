from botocore.exceptions import ClientError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from s3 import connection


def _get_bucket_details(ctx):
    relationships = ctx.instance.relationships

    if len(relationships) == 0:
        raise NonRecoverableError(
            'S3 objects must be contained in S3 buckets using a '
            'cloudify.aws.relationships.s3_object_contained_in_bucket '
            'relationship.'
        )

    bucket_name = None
    target = 'cloudify.aws.relationships.s3_object_contained_in_bucket'
    for relationship in relationships:
        if relationship.type == target:
            bucket_name = relationship.target.node.properties['name']

    if bucket_name is None:
        raise NonRecoverableError(
            'Could not get containing bucket name from related node.'
        )

    # If we got here, we should be dealing with a real bucket, so we'll just
    # retrieve the permissions
    bucket_permissions = relationship.target.node.properties['permissions']

    return bucket_name, bucket_permissions


@operation
def create(ctx):
    ctx.instance.runtime_properties['created'] = False

    s3_client = connection.S3ConnectionClient().client()

    bucket_name, bucket_permissions = _get_bucket_details(ctx)

    keys = s3_client.list_objects(Bucket=bucket_name).get('Contents', [])
    keys = [key['Key'] for key in keys]
    if ctx.node.properties['name'] in keys:
        raise NonRecoverableError(
            'Cannot create key {name} in bucket {bucket} as a key by this '
            'name already exists in the bucket.'.format(
                name=ctx.node.properties['name'],
                bucket=bucket_name,
            )
        )

    # Get the contents var
    is_file = False
    if ctx.node.properties['load_contents_from_file']:
        # It's a file, get a file handle for it
        contents = ctx.download_resource(ctx.node.properties['contents'])
        contents = open(contents)
        is_file = True
    else:
        # It's a string, provide it
        contents = ctx.node.properties['contents']

    # Create key
    s3_client.put_object(
        ACL=bucket_permissions,
        Body=contents,
        Bucket=bucket_name,
        Key=ctx.node.properties['name'],
        ContentType=ctx.node.properties['content_type'],
    )

    if is_file:
        contents.close()

    ctx.instance.runtime_properties['created'] = True


@operation
def delete(ctx):
    bucket_name, _ = _get_bucket_details(ctx)

    if ctx.instance.runtime_properties.get('created', False):
        s3_client = connection.S3ConnectionClient().client()

        try:
            s3_client.delete_object(
                Bucket=bucket_name,
                Key=ctx.node.properties['name'],
            )
        except ClientError as err:
            raise NonRecoverableError(
                'Deletion of key {name} from bucket {bucket} failed with '
                'error: {error}'.format(
                    name=ctx.node.properties['name'],
                    bucket=bucket_name,
                    error=err.message,
                )
            )
    else:
        raise NonRecoverableError(
            'Creation of key {name} in bucket {bucket} failed, so it will not '
            'be deleted.'.format(
                name=ctx.node.properties['name'],
                bucket=bucket_name,
            )
        )
