from aws_cdk import (
    aws_apigateway as apigateway,
    aws_ec2 as ec2,
    aws_lambda as lambda_,
    aws_rds as rds,
    Duration
)
from constructs import Construct


class ApiServerConstruct(Construct):

    def __init__(self, scope: Construct, id: str, db: rds.IDatabaseInstance, vpc: ec2.IVpc) -> None:
        super().__init__(scope, id)

        apiserver_lmd = lambda_.Function(
            self, "apiserver_lambda",
            code=lambda_.Code.from_asset("deployment/bundle/app.zip"),
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="main.api_server_handler",
            environment={
                'PRODUCTION': '1'
            },
            timeout=Duration.seconds(29),
            vpc=vpc,
        )
        db.connections.allow_default_port_from(apiserver_lmd, "ConnectionFromApiserver")
        self.apigateway = apigateway.LambdaRestApi(self, "apiserver", handler=apiserver_lmd)