from botocore.exceptions import ClientError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from s3 import connection


@operation
def create(ctx):
    ctx.instance.runtime_properties['created'] = False

    s3_client = connection.S3ConnectionClient().client()
    bucket_name = ctx.node.properties['name']

    existing_buckets = s3_client.list_buckets()['Buckets']
    existing_buckets = [bucket['Name'] for bucket in existing_buckets]
    if ctx.node.properties['use_existing_resource']:
        if bucket_name in existing_buckets:
            return True
        else:
            raise NonRecoverableError(
                'Attempt to use existing bucket {bucket} failed, as no '
                'bucket by that name exists.'.format(bucket=bucket_name)
            )
    else:
        if bucket_name in existing_buckets:
            raise NonRecoverableError(
                'Bucket {bucket} already exists, but use_existing_resource '
                'is not set to true.'.format(bucket=bucket_name)
            )

    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            ACL=ctx.node.properties['permissions'],
        )
        ctx.instance.runtime_properties['created'] = True
    except ClientError as err:
        raise NonRecoverableError(
            'Bucket creation failed: {}'.format(err.msg)
        )

    # See if we should configure this as a website
    index = ctx.node.properties['website_index_page']
    error = ctx.node.properties['website_error_page']
    if (index, error) == ('', ''):
        # Neither the index nor the error page were defined, this bucket is
        # not intended to be a website
        pass
    elif '' in (index, error):
        raise NonRecoverableError(
            'For the bucket to be configured as a website, both '
            'website_index_page and website_error_page must be set.'
        )
    else:
        if '/' in index:
            raise NonRecoverableError(
                'S3 bucket website default page must not contain a /'
            )
        s3_client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={
                'ErrorDocument': {
                    'Key': error,
                },
                'IndexDocument': {
                    'Suffix': index,
                },
            },
        )

    bucket_region = s3_client.head_bucket(
        Bucket=bucket_name,
    )['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']

    ctx.instance.runtime_properties['url'] = (
        'http://{bucket}.s3-website-{region}.amazonaws.com'.format(
            bucket=bucket_name,
            region=bucket_region,
        )
    )


@operation
def delete(ctx):
    if ctx.node.properties['use_existing_resource']:
        return True

    bucket_name = ctx.node.properties['name']

    if not ctx.instance.runtime_properties.get('created', False):
        raise NonRecoverableError(
            'Bucket {bucket} creation failed, so it will not be '
            'deleted.'.format(bucket=bucket_name)
        )

    s3_client = connection.S3ConnectionClient().client()

    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as err:
        raise NonRecoverableError(
            'Bucket deletion failed: {}'.format(err.message)
        )
