#!/usr/bin/env python3
import os
from typing import List
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    # aws_sqs as sqs,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    aws_apigateway as apigateway,
)
from constructs import Construct


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

        self.run_algos_rectification(lmd)
        self.run_algos_regular(lmd)
        self.place_sre_trades(lmd)
        self.save_pnl(lmd)
        self.populate_instruments(lmd)
        self.run_price_band_check(lmd)
        # self.run_rollover(lmd)
        self.run_algo_shadow_only(lmd)
        self.save_historical_data(lmd)
        self.api_server(db)
        self.run_nifty_gap_check(lmd)
        self.run_nifty_price_band_exit(lmd)
        self.shadow_sheet_positions(lmd)
        self.shadow_sheet_futures_price(lmd)
        self.exit_all_trades(lmd)

        # self.notify_on_fail()


    def notify_on_fail(self, make_fail_test_sm=True):
        ## Does not work permission issue. Being handled manually.
        if make_fail_test_sm:
            fail_state = sfn.Fail(self, "FailureTest")
            sfn.StateMachine(
                self, "failtest",
                definition_body=sfn.DefinitionBody.from_chainable(fail_state)
            )
        topic = sns.Topic(self, "notifyfails")
        topic.add_subscription(sns_subscriptions.EmailSubscription(email_address="saurabh.shirke@algonauts.in"))
        events.Rule(
            self, "notify_on_fail",
            targets=[targets.SnsTopic(topic)],
            event_pattern=events.EventPattern(
                source=["aws.status"],
                detail_type=["Step Functions Execution Status Change"],
                detail={ "status": ["FAILED", "TIMED_OUT"] },
            )
        )


    def algo_job(self, lmd: lambda_.Function, algo_name: str, tag: str, **kwargs):
        kwargs['algo_name'] = algo_name
        return tasks.LambdaInvoke(
            self, f"algo_{algo_name}_{tag}",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'run_algo',
                'kwargs': kwargs
            })
        )


    def rollover_job(self, lmd: lambda_.Function, algo_name: str, **kwargs):
        kwargs['algo_name'] = algo_name
        return tasks.LambdaInvoke(
            self, f"rollover_algo_{algo_name}",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'rollover',
                'kwargs': kwargs
            })
        )


    def holiday_check_job(self, lmd: lambda_.Function, name: str):
        return tasks.LambdaInvoke(
            self, name,
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'is_holiday'
            })
        )


    def ltp_save_job(self, lmd: lambda_.Function, name: str, **kwargs):
        return tasks.LambdaInvoke(
            self, name,
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'truedataltpsave',
                'kwargs': kwargs
            })
        )
    

    def run_algo_shadow_only(self, lmd: lambda_.Function):
        ltp_save_job_shadow_only = self.ltp_save_job(lmd, "truedataltpsave_shadow_only", fo=True, eq=True, ohlc=False)
        futures_strategy2_mod_rms = self.algo_job(lmd, "NiftyFuturesAlgoModRMS", "shadow_only", mode="REGULAR", shadow_only=True, mailer=False)
        futures_strategy2_mod_2_rms = self.algo_job(lmd, "NiftyFuturesAlgoMod2RMS", "shadow_only", mode="REGULAR", shadow_only=True, mailer=False)
        next_50_futures_mod_rms = self.algo_job(lmd, "NiftyNext50FuturesAlgoModRMS", "shadow_only", mode="REGULAR", shadow_only=True, mailer=False)
        next_50_futures_mod_2_rms = self.algo_job(lmd, "NiftyNext50FuturesAlgoMod2RMS", "shadow_only", mode="REGULAR", shadow_only=True, mailer=False)
        next_50_futures_s7_rms = self.algo_job(lmd, "NiftyNext50FuturesAlgoS7RMS", "shadow_only", mode="REGULAR", shadow_only=True, mailer=False)
        futures_s7_rms = self.algo_job(lmd, "NiftyFuturesAlgoS7RMS", "shadow_only", mode="REGULAR", shadow_only=True, mailer=False)
        nifty_rms = self.algo_job(lmd, "NiftyIndexRMS", "shadow_only", mode="REGULAR", shadow_only=True, mailer=False)
        rms_parallel = sfn.Parallel(self, "rms_parallel_shadow_only")
        rms_parallel.branch(futures_strategy2_mod_rms)
        rms_parallel.branch(futures_strategy2_mod_2_rms)
        rms_parallel.branch(next_50_futures_mod_rms)
        rms_parallel.branch(futures_s7_rms)
        rms_parallel.branch(next_50_futures_mod_2_rms)
        rms_parallel.branch(next_50_futures_s7_rms)
        rms_parallel.branch(nifty_rms)
        chain = ltp_save_job_shadow_only.next(rms_parallel)
        sfn.StateMachine(
            self, "run_algos_shadow_only_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10),
        )


    def run_algos_regular(self, lmd: lambda_.Function):
        regular_or_rectification = tasks.LambdaInvoke(
            self, "regular_or_rectification_1",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'regular_or_rectification'
            })
        )
        is_holiday = self.holiday_check_job(lmd, "is_holiday_regular")
        ltp_save_job_regular = self.ltp_save_job(lmd, "truedataltpsave_regular", fo=True, eq=True, ohlc=False)
        futures_strategy2_mod_rms = self.algo_job(lmd, "NiftyFuturesAlgoModRMS", "regular", exit_only=True, mode="REGULAR")
        futures_strategy2_mod_2_rms = self.algo_job(lmd, "NiftyFuturesAlgoMod2RMS", "regular", exit_only=True, mode="REGULAR")
        next_50_futures_mod_rms = self.algo_job(lmd, "NiftyNext50FuturesAlgoModRMS", "regular", exit_only=True, mode="REGULAR")
        next_50_futures_mod_2_rms = self.algo_job(lmd, "NiftyNext50FuturesAlgoMod2RMS", "regular", exit_only=True, mode="REGULAR")
        next_50_futures_s7_rms = self.algo_job(lmd, "NiftyNext50FuturesAlgoS7RMS", "regular", exit_only=True, mode="REGULAR")
        futures_s7_rms = self.algo_job(lmd, "NiftyFuturesAlgoS7RMS", "regular", exit_only=True, mode="REGULAR")
        nifty_algo = self.algo_job(lmd, "NiftyIndexRMS", "regular", exit_only=True, mode="REGULAR")
        results_hedge = self.algo_job(lmd, "ResultsExitAlgo", "regular", send_no_trades=False)
        # nifty_hedge = self.algo_job(lmd, "NiftyOptionHedgeAlgo", "regular", send_no_trades=False)

        rms_parallel = sfn.Parallel(self, "rms_parallel_regular")
        rms_parallel.branch(futures_strategy2_mod_rms)
        rms_parallel.branch(futures_strategy2_mod_2_rms)
        rms_parallel.branch(next_50_futures_mod_rms)
        rms_parallel.branch(futures_s7_rms)
        rms_parallel.branch(next_50_futures_mod_2_rms)
        rms_parallel.branch(next_50_futures_s7_rms)
        rms_parallel.branch(nifty_algo)
        hedge_parallel = sfn.Parallel(self, "hedgeparallel_regular")
        hedge_parallel.branch(results_hedge)
        # hedge_parallel.branch(nifty_hedge)

        holiday_choice = sfn.Choice(self, "holiday_choice_regular")
        regular_choice = sfn.Choice(self, "regular_choice")
        success = sfn.Succeed(self, "finish_regular")
        chain = is_holiday.next(holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
            regular_or_rectification.next(regular_choice.when(sfn.Condition.string_equals("$.Payload.regular_or_rectification", "REGULAR"),
                ltp_save_job_regular.next(rms_parallel).next(hedge_parallel).next(success)
            ).otherwise(success))
        ).otherwise(success))
        sm = sfn.StateMachine(
            self, "run_algos_regular_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10),
        )
        events.Rule(
            self, "cron_regular",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='45', hour='9', month='*', week_day='MON-FRI', year='*')
        )

    
    def run_algos_rectification(self, lmd: lambda_.Function):
        regular_or_rectification = tasks.LambdaInvoke(
            self, "regular_or_rectification_2",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'regular_or_rectification'
            })
        )
        is_holiday = self.holiday_check_job(lmd, "is_holiday_rectification")
        ltp_save_job_rectification = self.ltp_save_job(lmd, "truedataltpsave_rectification", fo=True, eq=True, ohlc=True)
        futures_strategy2_mod_rms = self.algo_job(lmd, "NiftyFuturesAlgoModRMS", "rectification", mode="RECTIFICATION")
        futures_strategy2_mod_2_rms = self.algo_job(lmd, "NiftyFuturesAlgoMod2RMS", "rectification", mode="RECTIFICATION")
        next_50_futures_mod_rms = self.algo_job(lmd, "NiftyNext50FuturesAlgoModRMS", "rectification", mode="RECTIFICATION")
        next_50_futures_mod_2_rms = self.algo_job(lmd, "NiftyNext50FuturesAlgoMod2RMS", "rectification", mode="RECTIFICATION")
        next_50_futures_s7_rms = self.algo_job(lmd, "NiftyNext50FuturesAlgoS7RMS", "rectification", mode="RECTIFICATION")
        futures_s7_rms = self.algo_job(lmd, "NiftyFuturesAlgoS7RMS", "rectification", mode="RECTIFICATION")
        nifty_algo = self.algo_job(lmd, "NiftyIndexRMS", "rectification", mode="RECTIFICATION")
        results_hedge = self.algo_job(lmd, "ResultsExitAlgo", "rectification", send_no_trades=False)
        # nifty_hedge = self.algo_job(lmd, "NiftyOptionHedgeAlgo", "rectification", send_no_trades=False)

        rms_parallel = sfn.Parallel(self, "rms_parallel_rectification")
        rms_parallel.branch(futures_strategy2_mod_rms)
        rms_parallel.branch(futures_strategy2_mod_2_rms)
        rms_parallel.branch(next_50_futures_mod_rms)
        rms_parallel.branch(futures_s7_rms)
        rms_parallel.branch(next_50_futures_mod_2_rms)
        rms_parallel.branch(next_50_futures_s7_rms)
        rms_parallel.branch(nifty_algo)
        hedge_parallel = sfn.Parallel(self, "hedgeparallel_rectification")
        hedge_parallel.branch(results_hedge)
        holiday_choice = sfn.Choice(self, "holiday_choice_rectification")
        rectification_choice = sfn.Choice(self, "rectification_choice")
        success = sfn.Succeed(self, "finish_rectification")
        chain = is_holiday.next(holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
            regular_or_rectification.next(rectification_choice.when(sfn.Condition.string_equals("$.Payload.regular_or_rectification", "RECTIFICATION"),
                ltp_save_job_rectification.next(rms_parallel).next(hedge_parallel).next(success)
            ).otherwise(success))
        ).otherwise(success))
        sm = sfn.StateMachine(
            self, "run_algos_rectification_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10),
        )
        events.Rule(
            self, "cron_rectification",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='50', hour='3', month='*', week_day='MON-FRI', year='*')
        )


    def run_rollover(self, lmd: lambda_.Function):
        is_holiday = self.holiday_check_job(lmd, "is_holiday_rollover")
        place_trades = tasks.LambdaInvoke(
            self, "place_sre_trades_rollover",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'place_sre_trades'
            })
        )
        check_trades = tasks.LambdaInvoke(
            self, "check_sre_trades_rollover",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'check_sre_trades'
            })
        )

        ltp_save_job_rollover = self.ltp_save_job(lmd, "ltp_save_rollover")
        futures_strategy2_mod_rms = self.rollover_job(lmd, "NiftyFuturesAlgoModRMS")
        futures_strategy2_mod_2_rms = self.rollover_job(lmd, "NiftyFuturesAlgoMod2RMS")
        next_50_futures_mod_rms = self.rollover_job(lmd, "NiftyNext50FuturesAlgoModRMS")
        next_50_futures_mod_2_rms = self.rollover_job(lmd, "NiftyNext50FuturesAlgoMod2RMS")
        next_50_futures_s7_rms = self.rollover_job(lmd, "NiftyNext50FuturesAlgoS7RMS")
        futures_s7_rms = self.rollover_job(lmd, "NiftyFuturesAlgoS7RMS")
        nifty_algo = self.rollover_job(lmd, "NiftyIndexRMS")
        
        parallel = sfn.Parallel(self, "parallel_rollover")
        parallel.branch(futures_strategy2_mod_rms)
        parallel.branch(futures_strategy2_mod_2_rms)
        parallel.branch(next_50_futures_mod_rms)
        parallel.branch(futures_s7_rms)
        parallel.branch(next_50_futures_mod_2_rms)
        parallel.branch(next_50_futures_s7_rms)
        parallel.branch(nifty_algo)

        choice = sfn.Choice(self, "rollover_holiday")
        success = sfn.Succeed(self, "rollover_finish")
        wait = sfn.Wait(self, "rollover_wait_for_sre_trades", time=sfn.WaitTime.duration(Duration.minutes(2)))
        chain = is_holiday.next(
            choice.when(
                sfn.Condition.boolean_equals("$.Payload.is_holiday", False), 
                    ltp_save_job_rollover.next(parallel).next(place_trades).next(wait).next(check_trades).next(success)
            ).otherwise(success)
        )

        sm = sfn.StateMachine(
            self, "rollover_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10),
        )
        events.Rule(
            self, "rollover_cron",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='1', hour='6', month='*', week_day='MON-FRI', year='*')
        )


    def save_historical_data(self, lmd: lambda_.Function):
        is_holiday = self.holiday_check_job(lmd, "is_holiday_historical_save")
        historical_save = tasks.LambdaInvoke(
            self, "truedatasave",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'truedatasave'
            })
        )
        success = sfn.Succeed(self, "finish_historical_save")
        holiday_choice = sfn.Choice(self, "holiday_choice_historical_save")
        chain = is_holiday.next(
            holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
                historical_save.next(success)
            ).otherwise(success)
        )
        sm = sfn.StateMachine(
            self, "historical_save_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10)
        )
        events.Rule(
            self, "historical_save_sm_cron",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='1', hour='3', month='*', week_day='MON-FRI', year='*')
        )


    def save_pnl(self, lmd: lambda_.Function):
        is_holiday = self.holiday_check_job(lmd, "is_holiday_pnl")
        ltp_save = self.ltp_save_job(lmd, "ltp_save_pnl")

        pnl_save = tasks.LambdaInvoke(
            self, "pnl_save",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'pnlsave'
            })
        )
        positions_send = tasks.LambdaInvoke(
            self, "send_positions",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'send_positions'
            })
        )

        holiday_choice = sfn.Choice(self, "holiday_choice_pnl")
        success = sfn.Succeed(self, "finish_pnl")
        chain = is_holiday.next(
            holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
                ltp_save.next(pnl_save).next(positions_send).next(success)
            ).otherwise(success)
        )

        sm = sfn.StateMachine(
            self, "pnl_save_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10)
        )
        events.Rule(
            self, "pnl_save_sm_cron",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='30', hour='10', month='*', week_day='MON-FRI', year='*')
        )

    def run_price_band_check(self, lmd: lambda_.Function):
        is_holiday = self.holiday_check_job(lmd, "is_holiday_2")
        ltp_save = self.ltp_save_job(lmd, "ltp_save_price_band", fo=True, eq=True, ohlc=False)
        price_band = tasks.LambdaInvoke(
            self, "price_band",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'run_algo',
                'kwargs': {
                    'algo_name': 'PriceBandExitAlgo',
                    'send_no_trades': False
                }
            })
        )
        holiday_choice = sfn.Choice(self, "holiday_choice_2")
        success = sfn.Succeed(self, "finish_2")
        chain = is_holiday.next(
            holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False), 
                ltp_save.next(price_band.next(success))).otherwise(success)
        )
        sm = sfn.StateMachine(
            self, "price_band_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10)
        )
        events.Rule(
            self, "price_band_cron",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='*/15', hour='4-8', month='*', week_day='MON-FRI', year='*')
        )
        events.Rule(
            self, "price_band_cron_2",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='0,15', hour='9', month='*', week_day='MON-FRI', year='*')
        )

    
    def run_nifty_gap_check(self, lmd: lambda_.Function):
        is_holiday = self.holiday_check_job(lmd, "is_holiday_nifty_exit")
        ltp_save = self.ltp_save_job(lmd, "ltp_save_nifty_exit", eq=True, ohlc=False, fo=False)
        nifty_gap_exit = tasks.LambdaInvoke(
            self, "nifty_gap_exit",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'run_algo',
                'kwargs': {
                    'algo_name': 'NiftyGapExit',
                    'mailer': False
                }
            })
        )
        next_50_gap_exit = tasks.LambdaInvoke(
            self, "next_50_gap_exit",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'run_algo',
                'kwargs': {
                    'algo_name': 'NiftyNext50GapExit',
                    'mailer': False
                }
            })
        )
        holiday_choice = sfn.Choice(self, "holiday_choice_nifty_exit")
        success = sfn.Succeed(self, "finish_nifty_exit")
        chain = is_holiday.next(
            holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
                ltp_save.next(nifty_gap_exit).next(next_50_gap_exit).next(success)
            ).otherwise(success)
        )
        sm = sfn.StateMachine(
            self, "nifty_exit_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10)
        )
        events.Rule(
            self, "nifty_exit_rule",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='46', hour='3', month='*', week_day='MON-FRI', year='*')
        )


    def run_nifty_price_band_exit(self, lmd: lambda_.Function):
        is_holiday = self.holiday_check_job(lmd, "is_holiday_nifty_exit_2")
        ltp_save = self.ltp_save_job(lmd, "ltp_save_nifty_exit_2", eq=True, ohlc=False, fo=True)
        nifty_gap_exit = tasks.LambdaInvoke(
            self, "nifty_gap_exit_2",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'run_algo',
                'kwargs': {
                    'algo_name': 'NiftyPriceBandExit',
                    'send_no_trades': False
                }
            })
        )
        next_50_gap_exit = tasks.LambdaInvoke(
            self, "next_50_gap_exit_2",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'run_algo',
                'kwargs': {
                    'algo_name': 'NiftyNext50PriceBandExit',
                    'send_no_trades': False
                }
            })
        )
        holiday_choice = sfn.Choice(self, "holiday_choice_nifty_exit_2")
        success = sfn.Succeed(self, "finish_nifty_exit_2")
        chain = is_holiday.next(
            holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
                ltp_save.next(nifty_gap_exit).next(next_50_gap_exit).next(success)
            ).otherwise(success)
        )
        sm = sfn.StateMachine(
            self, "nifty_exit_sm_2",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10)
        )
        events.Rule(
            self, "nifty_exit_rule_2",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='*/10', hour='4-9', month='*', week_day='MON-FRI', year='*')
        )

    
    def place_sre_trades(self, lmd: lambda_.Function):
        is_holiday = self.holiday_check_job(lmd, "is_holiday_sre_trades")
        holiday_choice = sfn.Choice(self, "holiday_choice_sre_trades")
        ltp_save = self.ltp_save_job(lmd, "ltp_save_sre_trades", eq=False, ohlc=False, fo=True)
        success = sfn.Succeed(self, "finish_sre_trades")
        wait = sfn.Wait(self, "wait_for_sre_trades", time=sfn.WaitTime.duration(Duration.minutes(2)))
        place_trades = tasks.LambdaInvoke(
            self, "place_sre_trades",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'place_sre_trades'
            })
        )
        check_trades = tasks.LambdaInvoke(
            self, "check_sre_trades",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'check_sre_trades'
            })
        )
        chain = is_holiday.next(holiday_choice.when(
            sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
            ltp_save.next(place_trades).next(wait).next(check_trades).next(success)
        ).otherwise(success))
        sm = sfn.StateMachine(
            self, "sre_trades_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(5)
        )
        events.Rule(
            self, "sre_trades_rule",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='50', hour='9', month='*', week_day='MON-FRI', year='*')
        )
        events.Rule(
            self, "sre_trades_rule_2",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='55', hour='3', month='*', week_day='MON-FRI', year='*')
        )


    def populate_instruments(self, lmd: lambda_.Function):
        populate_job = tasks.LambdaInvoke(
            self, "populate_instruments",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'populate_instruments'
            })
        )
        is_holiday = self.holiday_check_job(lmd, "is_holiday_populate")
        holiday_choice = sfn.Choice(self, "holiday_choice_populate")
        success = sfn.Succeed(self, "finish_populate")
        chain = is_holiday.next(holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
            populate_job.next(success)
        ).otherwise(success))
        sm = sfn.StateMachine(
            self, "populate_instruments_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10)
        )
        events.Rule(
            self, "populate_instruments_cron",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='1', hour='11', month='*', week_day='MON-FRI', year='*')
        )


    def shadow_sheet_positions(self, lmd: lambda_.Function):
        job = tasks.LambdaInvoke(
            self, "shadow_sheet",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'shadow_sheet',
                'kwargs': {
                    'futures_price_only': False
                }
            })
        )
        is_holiday = self.holiday_check_job(lmd, "is_holiday_shadow_sheet")
        holiday_choice = sfn.Choice(self, "holiday_choice_shadow_sheet")
        success = sfn.Succeed(self, "finish_shadow_sheet")
        chain = is_holiday.next(holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
            job.next(success)
        ).otherwise(success))
        sm = sfn.StateMachine(
            self, "shadow_sheet_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10)
        )
        events.Rule(
            self, "shadow_sheet_cron",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='1', hour='4,10', month='*', week_day='MON-FRI', year='*')
        )


    def shadow_sheet_futures_price(self, lmd: lambda_.Function):
        job = tasks.LambdaInvoke(
            self, "shadow_prices",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'shadow_sheet',
                'kwargs': {
                    'futures_price_only': True,
                    'append_mtms': True
                }
            })
        )
        is_holiday = self.holiday_check_job(lmd, "is_holiday_shadow_prices")
        holiday_choice = sfn.Choice(self, "holiday_choice_shadow_prices")
        success = sfn.Succeed(self, "finish_shadow_prices")
        chain = is_holiday.next(holiday_choice.when(sfn.Condition.boolean_equals("$.Payload.is_holiday", False),
            job.next(success)
        ).otherwise(success))
        sm = sfn.StateMachine(
            self, "shadow_prices_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10)
        )
        events.Rule(
            self, "shadow_prices_cron",
            targets=[targets.SfnStateMachine(sm)],
            schedule=events.Schedule.cron(minute='2,17,32,47', hour='4-9', month='*', week_day='MON-FRI', year='*')
        )


    def exit_all_trades(self, lmd):
        ltp_save = self.ltp_save_job(lmd, "ltp_save_kill_switch", eq=False, ohlc=False, fo=True)
        kill_switch = tasks.LambdaInvoke(
            self, "kill_switch",
            lambda_function=lmd,
            payload=sfn.TaskInput.from_object({
                'action': 'exit_all_trades'
            })
        )
        success = sfn.Succeed(self, "finish_kill_switch")
        chain = ltp_save.next(kill_switch).next(success)
        sfn.StateMachine(
            self, "kill_switch_sm",
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=Duration.minutes(10)
        )


    def api_server(self, db: rds.IDatabaseInstance):
        apiserver_lmd = lambda_.Function(
            self, "apiserver_lambda",
            code=lambda_.Code.from_asset("deployment/bundle/app.zip"),
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="main.api_server_handler",
            environment={
                'PRODUCTION': '1'
            },
            timeout=Duration.seconds(25),
            vpc=self.vpc,
            # vpc_subnets=ec2.SubnetSelection(subnets=[ec2.Subnet.from_subnet_id(self, "private-subnet-1", "subnet-05d9d7d22e60fb14d")])
        )
        db.connections.allow_default_port_from(apiserver_lmd, "ConnectionFromApiserver")
        api = apigateway.LambdaRestApi(self, "apiserver", handler=apiserver_lmd)
        api.root.add_method("GET")
        api.root.add_method("POST")
        # api = apigateway.RestApi(self, "apiserver", rest_api_name="apiserver")
        # integration = apigateway.LambdaIntegration(apiserver_lmd)
        # api.root.add_method(http_method="ANY", target=integration)


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
