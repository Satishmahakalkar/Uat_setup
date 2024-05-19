#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    # aws_sqs as sqs,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_lambda as lambda_,
    aws_events as events,
)
from constructs import Construct

from deployment.algoparallelcontructold import AlgoParallelConstruct as AlgoParallelConstructOld
from deployment.algoparallelconstruct import AlgoParallelConstruct
from deployment.apiserverconstruct import ApiServerConstruct
from deployment.failurealertsnsconstruct import FailureAlertSNSConstruct
from deployment.runmainconstruct import RunMainConstruct


class StallionStack(Stack):


    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc.from_lookup(self, "vpc", vpc_id="vpc-068c605ae0d7d91a6", is_default=True)
        self.vpc = vpc

        dbsge = ec2.SecurityGroup.from_lookup_by_id(self, "dbsge", "sg-03b641bb3365ef3a0")

        db: rds.IDatabaseInstance = rds.DatabaseInstance.from_database_instance_attributes(
            self, "rds",
            instance_identifier="database-1",
            instance_endpoint_address="database-1.csdocnbiweli.ap-south-1.rds.amazonaws.com",
            port=5432,
            security_groups=[dbsge],
            instance_resource_id="db-L3KELD4JJ4H2NZCQLJF4TIVOBI"
        )

        lmdsg = ec2.SecurityGroup(
            self, "lmdsg", vpc=vpc,
            allow_all_outbound=True,
        )

        lmd = lambda_.Function(
            self, "lambdamain",
            code=lambda_.Code.from_asset("deployment/bundle/app.zip"),
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="main.lambda_handler",
            security_groups=[lmdsg],
            timeout=Duration.minutes(15),
            memory_size=512,
            environment={
                'PRODUCTION': '1'
            },
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnets=[ec2.Subnet.from_subnet_id(self, "private-subnet-1", "subnet-05d9d7d22e60fb14d")])
        )

        db.connections.allow_default_port_from(lmd, "ConnectionFromLambda")

        FailureAlertSNSConstruct(self, "failure_notify_test")

        ApiServerConstruct(self, "api_server", db, self.vpc)
        AlgoParallelConstruct(
            self, "parallel_start", lmd, mode="START",
            schedules=[events.Schedule.cron(minute='50', hour='3', month='*', week_day='MON-FRI', year='*')]
        )
        AlgoParallelConstruct(
            self, "parallel_930", lmd, mode="930",
            schedules=[events.Schedule.cron(minute='0', hour='4', month='*', week_day='MON-FRI', year='*')]
        )
        AlgoParallelConstruct(
            self, "parallel_ongoing_trades", lmd, mode="ONGOINGTRADES",
            schedules=[events.Schedule.cron(minute='15', hour='4', month='*', week_day='MON-FRI', year='*')]
        )
        AlgoParallelConstruct(
            self, "parallel_end", lmd, mode="END",
            schedules=[events.Schedule.cron(minute='45', hour='9', month='*', week_day='MON-FRI', year='*')]
        )
        AlgoParallelConstruct(
            self, "parallel_intraday", lmd, mode="INTRADAY",
            schedules=[
                events.Schedule.cron(minute='30,45', hour='4', month='*', week_day='MON-FRI', year='*'),
                events.Schedule.cron(minute='*/15', hour='5-8', month='*', week_day='MON-FRI', year='*'),
                events.Schedule.cron(minute='0', hour='9', month='*', week_day='MON-FRI', year='*')
            ]
        )
        AlgoParallelConstruct(
            self, "parallel_intraday_exit", lmd, mode="INTRADAYEXIT",
            schedules=[events.Schedule.cron(minute='15,30', hour='9', month='*', week_day='MON-FRI', year='*')]
        )
        AlgoParallelConstruct(
            self, "parallel_rollover", lmd, mode="ROLLOVER",
            schedules=[]
        )
        AlgoParallelConstructOld(
            self, "algo_parallel_regular", lmd, rollover=False, mode="REGULAR", exit_only=True, shadow_only=False, mailer=True, 
            schedules=[events.Schedule.cron(minute='50', hour='9', month='*', week_day='MON-FRI', year='*')]
        )
        AlgoParallelConstructOld(
            self, "algo_parallel_rectification", lmd, rollover=False, mode="RECTIFICATION", exit_only=False, shadow_only=True, mailer=True, 
            schedules=[events.Schedule.cron(minute='47', hour='3', month='*', week_day='MON-FRI', year='*')]
        )
        # AlgoParallelConstructOld(
        #     self, "algo_parallel_ongoing", lmd, rollover=False, mode="ONGOINGTRADES", exit_only=False, shadow_only=False, mailer=True, 
        #     schedules=[events.Schedule.cron(minute='0', hour='4', month='*', week_day='MON-FRI', year='*')]
        # )
        # AlgoParallelConstructOld(
        #     self, "algo_parallel_intraday", lmd, rollover=False, mode="INTRADAY", exit_only=False, shadow_only=False, mailer=True, 
        #     schedules=[
        #         events.Schedule.cron(minute='15,30,45', hour='4', month='*', week_day='MON-FRI', year='*'),
        #         events.Schedule.cron(minute='*/15', hour='5-8', month='*', week_day='MON-FRI', year='*'),
        #         events.Schedule.cron(minute='0,15', hour='9', month='*', week_day='MON-FRI', year='*')
        #     ]
        # )
        # AlgoParallelConstructOld(
        #     self, "algo_parallel_rollover", lmd, rollover=True, mode="REGULAR", exit_only=False, shadow_only=False, mailer=True, 
        #     schedules=[]
        # )
        # AlgoParallelConstruct(
        #     self, "algo_parallel_shadow_only", lmd, rollover=False, mode="REGULAR", exit_only=False, shadow_only=True, mailer=False, 
        #     schedules=[]
        # )
        # RunMainConstruct(
        #     self, "price_band_exit", 
        #     lmd, ltp_eq=False, ltp_fo=False, ltp_ohlc=False,
        #     schedules=[
        #         events.Schedule.cron(minute='2,16,32,48', hour='4-8', month='*', week_day='MON-FRI', year='*'),
        #         # events.Schedule.cron(minute='0,15', hour='9', month='*', week_day='MON-FRI', year='*')
        #     ],
        #     actions=[{
        #         "action": "run_algo",
        #         "kwargs": {
        #             "algo_name": "PriceBandExitAlgo",
        #             "send_no_trades": False
        #         }
        #     }]
        # )
        # RunMainConstruct(
        #     self, "nifty_gap_check", 
        #     lmd, ltp_eq=True, ltp_fo=True, ltp_ohlc=False,
        #     schedules=[
        #         events.Schedule.cron(minute='46', hour='3', month='*', week_day='MON-FRI', year='*')
        #     ],
        #     actions=[{
        #         'action': 'run_algo',
        #         'kwargs': {
        #             'algo_name': 'NiftyGapExit',
        #             'mailer': False
        #         }
        #     }, {
        #         'action': 'run_algo',
        #         'kwargs': {
        #             'algo_name': 'NiftyNext50GapExit',
        #             'mailer': False
        #         }
        #     }]
        # )
        # RunMainConstruct(
        #     self, "nifty_price_band_exit", 
        #     lmd, ltp_eq=True, ltp_fo=True, ltp_ohlc=False,
        #     schedules=[
        #         events.Schedule.cron(minute='*/10', hour='4-9', month='*', week_day='MON-FRI', year='*')
        #     ],
        #     actions=[{
        #         'action': 'run_algo',
        #         'kwargs': {
        #             'algo_name': 'NiftyPriceBandExit',
        #             'send_no_trades': False
        #         }
        #     }, {
        #         'action': 'run_algo',
        #         'kwargs': {
        #             'algo_name': 'NiftyNext50PriceBandExit',
        #             'send_no_trades': False
        #         }
        #     }]
        # )
        RunMainConstruct(
            self, "shadow_sheet_positions", 
            lmd, ltp_eq=False, ltp_fo=False, ltp_ohlc=False,
            schedules=[
                events.Schedule.cron(minute='1', hour='4,10', month='*', week_day='MON-FRI', year='*'),
                events.Schedule.cron(minute='55', hour='3', month='*', week_day='MON-FRI', year='*')
            ],
            actions=[{
                'action': 'shadow_sheet',
                'kwargs': {
                    'futures_price_only': False
                }
            }]
        )
        RunMainConstruct(
            self, "shadow_sheet_futures_price", 
            lmd, ltp_eq=False, ltp_fo=False, ltp_ohlc=False,
            schedules=[events.Schedule.cron(minute='2,17,32,47', hour='4-9', month='*', week_day='MON-FRI', year='*')],
            actions=[{
                'action': 'shadow_sheet',
                'kwargs': {
                    'futures_price_only': True,
                    'append_mtms': True
                }
            }]
        )
        RunMainConstruct(
            self, "populate_instruments", 
            lmd, ltp_eq=False, ltp_fo=False, ltp_ohlc=False,
            schedules=[events.Schedule.cron(minute='1', hour='11', month='*', week_day='MON-FRI', year='*')],
            actions=[{
                'action': 'populate_instruments'
            }]
        )
        RunMainConstruct(
            self, "save_historical_data", 
            lmd, ltp_eq=False, ltp_fo=False, ltp_ohlc=False,
            schedules=[events.Schedule.cron(minute='1', hour='3', month='*', week_day='MON-FRI', year='*')],
            actions=[{
                'action': 'truedatasave'
            }]
        )
        RunMainConstruct(
            self, "save_pnl", 
            lmd, ltp_eq=False, ltp_fo=True, ltp_ohlc=False,
            schedules=[events.Schedule.cron(minute='30', hour='10', month='*', week_day='MON-FRI', year='*')],
            actions=[{
                'action': 'pnlsave'
            }, {
                'action': 'send_positions'
            }]
        )
        RunMainConstruct(
            self, "mail_trade_baskets",
            lmd, ltp_eq=False, ltp_fo=False, ltp_ohlc=False,
            schedules=[events.Schedule.cron(minute='5', hour='4', month='*', week_day='MON-FRI', year='*')],
            actions=[{
                'action': 'mail_trade_baskets'
            }]
        )
        RunMainConstruct(
            self, "trade_counter_calculate",
            lmd, ltp_eq=False, ltp_fo=False, ltp_ohlc=False,
            schedules=[events.Schedule.cron(minute='30', hour='3', month='*', week_day='MON-FRI', year='*')],
            actions=[{
                'action': 'trade_counter_calculate'
            }]
        )


app = cdk.App()
StallionStack(app, "StallionStack",
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.

    # Uncomment the next line to specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.

    #env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),

    # Uncomment the next line if you know exactly what Account and Region you
    # want to deploy the stack to. */

    #env=cdk.Environment(account='123456789012', region='us-east-1'),

    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    env=cdk.Environment(account="722943563809", region="ap-south-1")
    )

app.synth()
