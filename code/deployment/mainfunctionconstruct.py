from constructs import Construct
from aws_cdk import (
    aws_lambda as lambda_,
    aws_stepfunctions_tasks as tasks,
    aws_stepfunctions as sfn
)


class MainFunctionConstruct(Construct):

    def __init__(self, scope: Construct, id: str, lmd: lambda_.Function, action: str, **kwargs) -> None:
        super().__init__(scope, id)
        self.task = tasks.LambdaInvoke(
            self, f"{id}-invoke",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': action,
                'kwargs': kwargs
            })
        )
