from typing import List, Literal
from constructs import Construct
from aws_cdk import (
    aws_lambda as lambda_,
    aws_stepfunctions as sfn,
    aws_events as events,
    Duration
)
from deployment.chainparallelconstruct import ChainParallelConstruct
from deployment.mainfunctionconstruct import MainFunctionConstruct


class AlgoParallelConstruct(Construct):

    def __init__(
            self, scope: Construct, id: str,
            lmd: lambda_.Function, 
            mode: Literal["END", "START", "930", "INTRADAY", "ONGOINGTRADES", "INTRADAYEXIT", "ROLLOVER"],
            schedules: List[events.Schedule]
        ) -> None:
        super().__init__(scope, id)
        if mode == "START":
            kwargs = dict(
                action="run_algo",
                shadow_mode="SHADOW",
                trade_mode="SHADOWEXIT",
                send_no_trades=False
            )
            reversal_kwargs = dict(
                action="run_algo",
                shadow_mode="NOOP",
                trade_mode="NOOP",
                send_no_trades=False,
                reversal_mail=True,
            )
        elif mode == "930":
            kwargs = dict(
                action="run_algo",
                shadow_mode="SHADOW_MTM",
                trade_mode="NOOP",
                send_no_trades=False
            )
            reversal_kwargs = dict(
                action="run_algo",
                shadow_mode="NOOP",
                trade_mode="NOOP",
                send_no_trades=False,
                reversal_mail=True,
            )
        elif mode == "ONGOINGTRADES":
            kwargs = dict(
                action="run_algo",
                shadow_mode="NOOP",
                trade_mode="ENTRY",
            )
            reversal_kwargs = dict(
                action="run_algo",
                shadow_mode="SHADOW_MTM",
                trade_mode="SHADOWCHECKREVERSE",
                send_no_trades=False,
                reversal_mail=True,
            )
        elif mode == "END":
            kwargs = dict(
                action="run_algo",
                shadow_mode="SHADOW_EXIT",
                trade_mode="EXIT",
            )
            reversal_kwargs = dict(
                action="run_algo",
                shadow_mode="NOOP",
                trade_mode="NOOP",
                send_no_trades=False,
                reversal_mail=True,
            )
        elif mode == "INTRADAY":
            kwargs = dict(
                action="run_algo",
                shadow_mode="NOOP",
                trade_mode="SHADOWCHECK",
                send_no_trades=False,
            )
            reversal_kwargs = dict(
                action="run_algo",
                shadow_mode="SHADOW_MTM",
                trade_mode="SHADOWCHECKREVERSE",
                send_no_trades=False,
                reversal_mail=True,
            )
        elif mode == "INTRADAYEXIT":
            kwargs = dict(
                action="run_algo",
                shadow_mode="SHADOW_MTM",
                trade_mode="SHADOWCHECKEXITONLY",
                send_no_trades=False,
            )
            reversal_kwargs = dict(
                action="run_algo",
                shadow_mode="NOOP",
                trade_mode="NOOP",
                send_no_trades=False,
                reversal_mail=True,
            )
        elif mode == "ROLLOVER":
            kwargs = dict(
                action="rollover"
            )
            reversal_kwargs = dict(
                action="run_algo",
                shadow_mode="NOOP",
                trade_mode="NOOP",
                send_no_trades=False,
                reversal_mail=True,
            )
        else:
            raise ValueError("Wrong Mode")
        nifty_s7 = MainFunctionConstruct(self, f"{id}-NiftyS7ShadowAnalysis", lmd, algo_name="NiftyS7ShadowAnalysis", **kwargs)
        nifty_s7_rev = MainFunctionConstruct(self, f"{id}-NiftyS7ShadowAnalysisRev", lmd, algo_name="NiftyS7ShadowAnalysis", **reversal_kwargs)
        nifty_s2 = MainFunctionConstruct(self, f"{id}-NiftyS2ShadowAnalysis", lmd, algo_name="NiftyS2ShadowAnalysis", **kwargs)
        nifty_s2_rev = MainFunctionConstruct(self, f"{id}-NiftyS2ShadowAnalysisRev", lmd, algo_name="NiftyS2ShadowAnalysis", **reversal_kwargs)
        nifty_s9 = MainFunctionConstruct(self, f"{id}-NiftyS9ShadowAnalysis", lmd, algo_name="NiftyS9ShadowAnalysis", **kwargs)
        nifty_s9_rev = MainFunctionConstruct(self, f"{id}-NiftyS9ShadowAnalysisRev", lmd, algo_name="NiftyS9ShadowAnalysis", **reversal_kwargs)
        next50_s2 = MainFunctionConstruct(self, f"{id}-NiftyNext50S2ShadowAnalysis", lmd, algo_name="NiftyNext50S2ShadowAnalysis", **kwargs)
        next50_s2_rev = MainFunctionConstruct(self, f"{id}-NiftyNext50S2ShadowAnalysisRev", lmd, algo_name="NiftyNext50S2ShadowAnalysis", **reversal_kwargs)
        next50_s7 = MainFunctionConstruct(self, f"{id}-NiftyNext50S7ShadowAnalysis", lmd, algo_name="NiftyNext50S7ShadowAnalysis", **kwargs)
        next50_s7_rev = MainFunctionConstruct(self, f"{id}-NiftyNext50S7ShadowAnalysisRev", lmd, algo_name="NiftyNext50S7ShadowAnalysis", **reversal_kwargs)
        next50_s9 = MainFunctionConstruct(self, f"{id}-NiftyNext50S9ShadowAnalysis", lmd, algo_name="NiftyNext50S9ShadowAnalysis", **kwargs)
        next50_s9_rev = MainFunctionConstruct(self, f"{id}-NiftyNext50S9ShadowAnalysisRev", lmd, algo_name="NiftyNext50S9ShadowAnalysis", **reversal_kwargs)
        nifty_index = MainFunctionConstruct(self, f"{id}-NiftyIndexShadowAnalysis", lmd, algo_name="NiftyIndexShadowAnalysis", **kwargs)
        nifty_index_rev = MainFunctionConstruct(self, f"{id}-NiftyIndexShadowAnalysisRev", lmd, algo_name="NiftyIndexShadowAnalysis", **reversal_kwargs)
        shadow_analyis_parallel = sfn.Parallel(self, "shadow-analysis-parallel")
        shadow_analyis_parallel.branch(nifty_s7_rev.task.next(nifty_s7.task))
        shadow_analyis_parallel.branch(nifty_s2_rev.task.next(nifty_s2.task))
        shadow_analyis_parallel.branch(nifty_index_rev.task.next(nifty_index.task))
        shadow_analyis_parallel.branch(next50_s2_rev.task.next(next50_s2.task))
        shadow_analyis_parallel.branch(next50_s7_rev.task.next(next50_s7.task))
        parallel_2 = sfn.Parallel(self, "shadow-analysis-parallel-2")
        parallel_2.branch(nifty_s9_rev.task.next(nifty_s9.task))
        parallel_2.branch(next50_s9_rev.task.next(next50_s9.task))
        # place_trades = MainFunctionConstruct(self, "place_sre_trades", lmd, "place_sre_trades")
        # check_trades = MainFunctionConstruct(self, "check_sre_trades", lmd, "check_sre_trades")
        # wait = sfn.Wait(self, "wait_for_sre_trades", time=sfn.WaitTime.duration(Duration.seconds(30)))
        # chain = shadow_analyis_parallel.next(place_trades.task).next(wait).next(check_trades.task).next(parallel_2)
        chain = shadow_analyis_parallel.next(parallel_2)
        if mode == "START":
            results_ban = MainFunctionConstruct(
                self, f"{id}-ResultsShadowBan-{mode}", lmd, 
                action="run_algo", algo_name="ResultsShadowBan", mailer=False
            )
            fno_ban = MainFunctionConstruct(
                self, f"{id}-FnOBanCheck-{mode}", lmd, 
                action="run_algo", algo_name="FnOBanCheck", mailer=False
            )
            ban_chain = results_ban.task.next(fno_ban.task)
            chain = ban_chain.next(chain)
        ChainParallelConstruct(
            self, f"{mode}_chain", lmd, 
            ltp_eq=True, ltp_fo=True, ltp_ohlc=False, 
            chain=chain, 
            schedules=schedules
        )