from typing import List
from constructs import Construct
from aws_cdk import (
    aws_lambda as lambda_,
    aws_stepfunctions as sfn,
    aws_events as events,
    aws_events_targets as targets,
    Duration
)
from deployment.mainfunctionconstruct import MainFunctionConstruct


class ChainParallelConstruct(Construct):

    def __init__(
            self, scope: Construct, id: str, 
            lmd: lambda_.Function,
            ltp_eq: bool, ltp_fo: bool, ltp_ohlc: bool,
            chain: sfn.Chain,
            schedules: List[events.Schedule]
        ) -> None:
        super().__init__(scope, id)
        is_holiday = MainFunctionConstruct(self, "holiday", lmd, action="is_holiday")
        if ltp_eq or ltp_fo or ltp_ohlc:
            ltp_save = MainFunctionConstruct(self, "ltp_save", lmd, action="truedataltpsave", eq=ltp_eq, fo=ltp_fo, ohlc=ltp_ohlc)
            ltp_save_task = ltp_save.task
        else:
            ltp_save_task = sfn.Pass(self, "No ltp save")
        holiday_choice = sfn.Choice(self, "holiday_choice")
        success = sfn.Succeed(self, "finish")
        chain = is_holiday.task.next(
            holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
                ltp_save_task.next(chain).next(success)
            ).otherwise(success)
        )
        self.state_machine = sfn.StateMachine(
            self, f"{id}-sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10),
        )
        for i, schedule in enumerate(schedules):
            events.Rule(
                self, f"{id}-schedule-{i}",
                targets=[targets.SfnStateMachine(self.state_machine)],
                schedule=schedule
            )
