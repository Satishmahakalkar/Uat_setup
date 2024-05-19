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
            rollover: bool,
            mode: Literal["REGULAR", "RECTIFICATION", "INTRADAY", "ONGOINGTRADES"], exit_only: bool,
            shadow_only: bool, mailer: bool,
            schedules: List[events.Schedule]
        ) -> None:
        super().__init__(scope, id)
        if not rollover:
            kwargs = dict(
                exit_only=exit_only,
                mode=mode,
                shadow_only=shadow_only,
                mailer=mailer
            )
            action = "run_algo"
        else:
            kwargs = {}
            action = "rollover"
        if mode not in ("INTRADAY", "ONGOINGTRADES"):
            futures_strategy2_mod_rms = MainFunctionConstruct(
                self, f"{id}-NiftyFuturesAlgoModRMS-{mode}", lmd,
                action=action, algo_name="NiftyFuturesAlgoModRMS",  **kwargs
            )
            futures_strategy2_mod_2_rms = MainFunctionConstruct(
                self, f"{id}-NiftyFuturesAlgoMod2RMS-{mode}", lmd,
                action=action, algo_name="NiftyFuturesAlgoMod2RMS", **kwargs
            )
            next_50_futures_mod_rms = MainFunctionConstruct(
                self, f"{id}-NiftyNext50FuturesAlgoModRMS-{mode}", lmd,
                action=action, algo_name="NiftyNext50FuturesAlgoModRMS", **kwargs
            )
            next_50_futures_mod_2_rms = MainFunctionConstruct(
                self, f"{id}-NiftyNext50FuturesAlgoMod2RMS-{mode}", lmd,
                action=action, algo_name="NiftyNext50FuturesAlgoMod2RMS", **kwargs
            )
            next_50_futures_s7_rms = MainFunctionConstruct(
                self, f"{id}-NiftyNext50FuturesAlgoS7RMS-{mode}", lmd,
                action=action, algo_name="NiftyNext50FuturesAlgoS7RMS", **kwargs
            )
            futures_s7_rms = MainFunctionConstruct(
                self, f"{id}-NiftyFuturesAlgoS7RMS-{mode}", lmd,
                action=action, algo_name="NiftyFuturesAlgoS7RMS", **kwargs
            )
            s9 = MainFunctionConstruct(
                self, f"{id}-NiftyFuturesAlgoS9RMS-{mode}", lmd,
                action=action, algo_name="NiftyFuturesAlgoS9RMS", **kwargs
            )
            s9_next_50 = MainFunctionConstruct(
                self, f"{id}-NiftyNext50FuturesAlgoS9RMS-{mode}", lmd,
                action=action, algo_name="NiftyNext50FuturesAlgoS9RMS", **kwargs
            )
            # cap_alloc = MainFunctionConstruct(
            #     self, f"{id}-NiftyFuturesAlgoCapAllocS7-{mode}", lmd,
            #     action=action, algo_name="NiftyFuturesAlgoCapAllocS7", **kwargs
            # )
            nifty_algo = MainFunctionConstruct(
                self, f"{id}-NiftyIndexRMS-{mode}", lmd,
                action=action, algo_name="NiftyIndexRMS", **kwargs
            )
            results_hedge = MainFunctionConstruct(
                self, f"{id}-ResultsExitAlgo-{mode}", lmd, 
                action="run_algo", algo_name="ResultsExitAlgo", send_no_trades=False
            )
            rms_parallel = sfn.Parallel(self, f"rms-parallel-{mode}-batch-1")
            rms_parallel.branch(futures_strategy2_mod_rms.task)
            rms_parallel.branch(futures_strategy2_mod_2_rms.task)
            rms_parallel.branch(next_50_futures_mod_rms.task)
            rms_parallel.branch(futures_s7_rms.task)
            rms_parallel.branch(next_50_futures_mod_2_rms.task)
            rms_parallel.branch(next_50_futures_s7_rms.task)
            rms_parallel_2 = sfn.Parallel(self, f"rms-parallel-{mode}-batch-2")
            rms_parallel_2.branch(nifty_algo.task)
            rms_parallel_2.branch(s9.task)
            rms_parallel_2.branch(s9_next_50.task)
            # rms_parallel_2.branch(cap_alloc.task)
            hedge_parallel = sfn.Parallel(self, f"hedge-parallel-{mode}")
            hedge_parallel.branch(results_hedge.task)
            # place_trades = MainFunctionConstruct(self, "place_sre_trades", lmd, "place_sre_trades")
            # check_trades = MainFunctionConstruct(self, "check_sre_trades", lmd, "check_sre_trades")
            # wait = sfn.Wait(self, "wait_for_sre_trades", time=sfn.WaitTime.duration(Duration.minutes(2)))
            chain = rms_parallel.next(rms_parallel_2).next(hedge_parallel)
        else:
            chain = sfn.Pass(self, "IntradayPass")
        # if mode == "RECTIFICATION":
        #     nifty_s7_shadow_analysis = MainFunctionConstruct(
        #         self, f"{id}-NiftyS7ShadowAnalysis-9-20", lmd,
        #         action="run_algo", algo_name="NiftyS7ShadowAnalysis", shadow_mode="SHADOW", trade_mode="NOOP",
        #         send_no_trades=False
        #     )
        #     nifty_s2_shadow_analysis = MainFunctionConstruct(
        #         self, f"{id}-NiftyS2ShadowAnalysis-9-20", lmd,
        #         action="run_algo", algo_name="NiftyS2ShadowAnalysis", shadow_mode="SHADOW", trade_mode="NOOP",
        #         send_no_trades=False
        #     )
        # elif mode == "ONGOINGTRADES":
        #     nifty_s7_shadow_analysis = MainFunctionConstruct(
        #         self, f"{id}-NiftyS7ShadowAnalysis-9-30", lmd,
        #         action="run_algo", algo_name="NiftyS7ShadowAnalysis", shadow_mode="SHADOW_MTM", trade_mode="ENTRY",
        #     )
        #     nifty_s2_shadow_analysis = MainFunctionConstruct(
        #         self, f"{id}-NiftyS2ShadowAnalysis-9-30", lmd,
        #         action="run_algo", algo_name="NiftyS2ShadowAnalysis", shadow_mode="SHADOW_MTM", trade_mode="ENTRY",
        #     )
        # elif mode == "REGULAR":
        #     nifty_s7_shadow_analysis = MainFunctionConstruct(
        #         self, f"{id}-NiftyS7ShadowAnalysis-3-15", lmd,
        #         action="run_algo", algo_name="NiftyS7ShadowAnalysis", shadow_mode="SHADOW_EXIT", trade_mode="EXIT",
        #     )
        #     nifty_s2_shadow_analysis = MainFunctionConstruct(
        #         self, f"{id}-NiftyS2ShadowAnalysis-3-15", lmd,
        #         action="run_algo", algo_name="NiftyS2ShadowAnalysis", shadow_mode="SHADOW_EXIT", trade_mode="EXIT",
        #     )
        # else:
        #     nifty_s7_shadow_analysis = MainFunctionConstruct(
        #         self, f"{id}-NiftyS7ShadowAnalysis-15mins", lmd,
        #         action="run_algo", algo_name="NiftyS7ShadowAnalysis", shadow_mode="SHADOW_MTM", trade_mode="SHADOWCHECK",
        #         send_no_trades=False
        #     )
        #     nifty_s2_shadow_analysis = MainFunctionConstruct(
        #         self, f"{id}-NiftyS2ShadowAnalysis-15mins", lmd,
        #         action="run_algo", algo_name="NiftyS2ShadowAnalysis", shadow_mode="SHADOW_MTM", trade_mode="SHADOWCHECK",
        #         send_no_trades=False
        #     )
        # shadow_analyis_parallel = sfn.Parallel(self, "shadow-analysis-parallel")
        # shadow_analyis_parallel.branch(nifty_s2_shadow_analysis.task)
        # shadow_analyis_parallel.branch(nifty_s7_shadow_analysis.task)
        # chain = shadow_analyis_parallel.next(chain)
        ChainParallelConstruct(
            self, f"{mode}_chain", lmd, 
            ltp_eq=True, ltp_fo=True, ltp_ohlc=False, 
            chain=chain, 
            schedules=schedules
        )