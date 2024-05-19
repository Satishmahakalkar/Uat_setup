from typing import Dict, List, TypedDict
from constructs import Construct
from aws_cdk import (
    aws_lambda as lambda_,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
)
from deployment.mainfunctionconstruct import MainFunctionConstruct
from deployment.chainparallelconstruct import ChainParallelConstruct


class ActionArg(TypedDict):
    action: str
    kwargs: Dict[str, str]


class RunMainConstruct(Construct):

    def __init__(
            self, scope: Construct, id: str, 
            lmd: lambda_.Function, 
            ltp_eq: bool, ltp_fo: bool, ltp_ohlc: bool,
            schedules: List[events.Schedule],
            actions: List[ActionArg]
        ) -> None:
        super().__init__(scope, id)
        _tasks: List[tasks.LambdaInvoke] = []
        for i, action_dict in enumerate(actions):
            action = action_dict['action']
            action_kwargs = action_dict.get('kwargs', {})
            main_function = MainFunctionConstruct(self, f"{id}-{action}-{i}", lmd, action=action, **action_kwargs)
            _tasks.append(main_function.task)
        if len(_tasks) == 1:
            chain = _tasks[0]
        else:
            first_task, rest_tasks = _tasks[0], _tasks[1:]
            chain = first_task
            for task in rest_tasks:
                chain = chain.next(task)
        ChainParallelConstruct(
            self, f"{id}-chain", lmd, 
            ltp_eq=ltp_eq, ltp_fo=ltp_fo, ltp_ohlc=ltp_ohlc,
            chain=chain,
            schedules=schedules
        )