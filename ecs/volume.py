from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError


@operation
def validate(ctx):
    # Nothing currently validated
    pass
