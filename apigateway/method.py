#########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

from core.boto3_connection import connection

from cloudify.exceptions import NonRecoverableError
from cloudify.decorators import operation
from .resource import get_parents


lambda_uri_template = (
    "arn:aws:apigateway:{region}:lambda:path/"
    "{api_version}/functions/{lambda_arn}/invocations")
api_uri_template = (
    "arn:aws:execute-api:{region}:{account_id}:{api_id}/*/POST/DynamoDBManager"
    )


def generate_lambda_uri(ctx, client, lambda_arn):
    return lambda_uri_template.format(
        region=client.meta.region_name,
        api_version=client.meta.service_model.api_version,
        lambda_arn=lambda_arn,
        )


def generate_api_uri(ctx, client, api_id):
    account_id = ctx.target.instance.runtime_properties[
            'arn'].split(':')[4]
    # Only the account id field is all-digits
    assert int(account_id)

    return api_uri_template.format(
        region=client.meta.region_name,
        account_id=account_id,
        api_id=api_id,
        )


@operation
def creation_validation(ctx):
    if 'cloudify.aws.relationships.method_in_resource' not in [
            rel.type for rel in ctx.node.relationships]:
        raise NonRecoverableError(
                "An API Method must be related to either an ApiResource or "
                "a RestApi (root resource) via "
                "'cloudify.aws.relationships.method_in_resource'")


@operation
def create(ctx):
    props = ctx.node.properties
    client = connection(props['aws_config']).client('apigateway')

    parent, api = get_parents(ctx.instance)

    client.put_method(
        restApiId=api.runtime_properties['id'],
        resourceId=parent.runtime_properties['resource_id'],
        httpMethod=props['http_method'],
        authorizationType=props['auth_type'],
        )


@operation
def delete(ctx):
    props = ctx.node.properties
    client = connection(props['aws_config']).client('apigateway')

    parent, api = get_parents(ctx.instance)

    client.delete_method(
        restApiId=api.runtime_properties['id'],
        resourceId=parent.runtime_properties['resource_id'],
        httpMethod=props['http_method'],
        )


def get_connected_lambda(source, target):
    props = source.instance.runtime_properties
    linked = props.setdefault(
        'linked_lambdas', {})
    props._set_changed()
    return linked.setdefault(target.node.name, {})


@operation
def connect_lambda(ctx):
    sprops = ctx.source.node.properties
    sclient = connection(sprops['aws_config']).client('apigateway')
    tclient = connection(sprops['aws_config']).client('lambda')

    parent, api = get_parents(ctx.source.instance)

    lambda_uri = generate_lambda_uri(
        ctx, sclient,
        ctx.target.instance.runtime_properties['arn'],
        )
    api_uri = generate_api_uri(
        ctx, sclient,
        api.runtime_properties['id'],
        )
    function_name = ctx.target.instance.runtime_properties['name']

    sclient.put_integration(
        restApiId=api.runtime_properties['id'],
        resourceId=parent.runtime_properties['resource_id'],
        type='AWS',
        httpMethod=sprops['http_method'],
        integrationHttpMethod=sprops['http_method'],
        uri=lambda_uri,
        )

    runtime_props = get_connected_lambda(ctx.source, ctx.target)

    runtime_props['statement_id'] = '{}-{}'.format(
        ctx.source.node.name, ctx.target.node.name)

    tclient.add_permission(
        FunctionName=function_name,
        StatementId=runtime_props['statement_id'],
        Action='lambda:InvokeFunction',
        Principal='apigateway.amazonaws.com',
        SourceArn=api_uri,
        )


@operation
def disconnect_lambda(ctx):
    sprops = ctx.source.node.properties
    sclient = connection(sprops['aws_config']).client('apigateway')
    tclient = connection(sprops['aws_config']).client('lambda')

    parent, api = get_parents(ctx.source.instance)

    tclient.remove_permission(
        FunctionName=ctx.target.instance.runtime_properties['name'],
        StatementId=get_connected_lambda(
            ctx.source,
            ctx.target)['statement_id'],
        )

    sclient.delete_integration(
        restApiId=api.runtime_properties['id'],
        resourceId=parent.runtime_properties['resource_id'],
        httpMethod=sprops['http_method'],
        )
