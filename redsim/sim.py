import os
import datetime
import itertools
import numpy as np
import pandas as pd
from tqdm import tqdm
from typing import Dict, Any, List

from redsim.params import Params
from redsim.market import generate_market_variables_paths
from redsim.revenue import compute_revenue_metrics
from redsim.costs import compute_onchain_cost_metrics


def run_single_sim(sim_len: int, params: Params) -> pd.DataFrame:
    # Generate path from market variables
    market_params = params.get_market_params()
    market_path_dict = generate_market_variables_paths(sim_len, market_params)
    # Compute revenue metrics
    revenue_metrics_dict = compute_revenue_metrics(params, market_path_dict)
    revenue_eth_arr = revenue_metrics_dict["revenue_eth"]
    cum_revenue_eth_arr = revenue_eth_arr.cumsum()
    # Compute cost metrics
    cost_metrics_dict = compute_onchain_cost_metrics(sim_len, params, market_path_dict)
    onchain_cost_eth_arr = cost_metrics_dict["onchain_cost_eth"]
    cum_onchain_cost_eth_arr = onchain_cost_eth_arr.cumsum()
    infra_cost_usd_arr = market_path_dict["infra_cost_usd"]
    cum_infra_cost_usd_arr = infra_cost_usd_arr.cumsum()
    # Compute OP revenue share cost
    onchain_profit_eth_arr = cum_revenue_eth_arr - cum_onchain_cost_eth_arr
    op_rev_share_cost_eth_arr = np.maximum(
        0.025 * cum_revenue_eth_arr, 0.15 * onchain_profit_eth_arr
    )
    # Build output from sim results
    eth_price_usd = market_params["eth_price_usd"]
    total_profit_eth_arr = (
        onchain_profit_eth_arr
        - (cum_infra_cost_usd_arr / eth_price_usd)
        - op_rev_share_cost_eth_arr
    )
    sim_start = pd.to_datetime(datetime.date.today()).timestamp()
    output_dict = {
        "timestamp": pd.to_datetime(sim_start + 12 * np.arange(sim_len), unit="s"),
        "onchain_profit_eth": onchain_profit_eth_arr,
        "onchain_profit_usd": onchain_profit_eth_arr * eth_price_usd,
        "total_profit_eth": total_profit_eth_arr,
        "total_profit_usd": total_profit_eth_arr * eth_price_usd,
        "op_rev_share_cost_eth": op_rev_share_cost_eth_arr,
    }
    output_dict.update(market_path_dict)
    output_dict.update(revenue_metrics_dict)
    output_dict.update(cost_metrics_dict)
    output_df = pd.DataFrame(output_dict)
    return output_df


def run_mc_sim(
    sim_len: int, sim_iter: int, params: Params, agg_daily: bool = False
) -> pd.DataFrame:
    output_df = pd.DataFrame()
    for i in tqdm(range(sim_iter)):
        temp_df = run_single_sim(sim_len, params)
        if agg_daily:
            temp_df = compute_output_aggregate(temp_df, "D")
        temp_df["iter"] = i
        output_df = pd.concat([output_df, temp_df], ignore_index=True)
    return output_df


def run_mc_sim_for_param_dict(
    sim_len: int,
    sim_iter: int,
    param_ranges_dict: Dict[str, Any],
    agg_daily: bool = False,
    output_dir: str = "data",
    save: bool = False,
    file_name: str = None,
) -> pd.DataFrame:
    # Initialize sweep variables
    output_df = pd.DataFrame()
    iter_tuple_list = list(itertools.product(*param_ranges_dict.values()))
    key_list = param_ranges_dict.keys()
    ii = 0
    for iter_tuple in tqdm(iter_tuple_list):
        # Build Params for sweep iteration
        params = Params()
        params.set_default_params()
        for i, key in enumerate(key_list):
            params.set_param(key, iter_tuple[i])
        # For each parameter tuple:
        temp_df = run_mc_sim(sim_len, sim_iter, params)
        for i, key in enumerate(key_list):
            temp_df[key] = iter_tuple[i]
        # Compute daily agregates, if needed
        if agg_daily:
            temp_df = compute_output_aggregate(temp_df, "D", ["iter"] + key_list)
        output_df = pd.concat([output_df, temp_df], ignore_index=True)
        # Save current output, if needed
        if save:
            output_df_filename = f"{file_name}_{iter_tuple}_sim_{ii}.csv"
            output_df_filepath = os.path.join(output_dir, output_df_filename)
            output_df.to_csv(output_df_filepath, index=False)
        ii += 1
    return output_df


def compute_output_aggregate(
    output_df: pd.DataFrame, freq: str, groupby_vars: List[str] = []
) -> pd.DataFrame:
    agg_dict = {
        "onchain_profit_eth": "last",
        "onchain_profit_usd": "last",
        "total_profit_eth": "last",
        "total_profit_usd": "last",
        "op_rev_share_cost_eth": "last",
        "l1_base_fee_gwei": "mean",
        "l1_prio_fee_gwei": "mean",
        "l2_base_fee_gwei": "mean",
        "l2_prio_fee_gwei": "mean",
        "l2_gas": "sum",
        "da_challenge": "mean",
        "infra_cost_usd": "sum",
        "revenue_eth": "sum",
        "revenue_data_eth": "sum",
        "revenue_exec_eth": "sum",
        "onchain_cost_eth": "sum",
        "proposer_gas_cost_eth": "sum",
        "da_gas_cost_eth": "sum",
        "batcher_gas_cost_eth": "sum",
        "batcher_txs": "sum",
        "channel_l2_gas": "sum",
    }
    if len(groupby_vars) == 0:
        agg_df = (
            output_df.groupby(pd.Grouper(key="timestamp", freq=freq))
            .agg(agg_dict)
            .reset_index()
        )
    else:
        agg_df = (
            output_df.groupby([pd.Grouper(key="timestamp", freq=freq)] + groupby_vars)
            .agg(agg_dict)
            .reset_index()
        )
    agg_df["timestamp"] = pd.to_datetime(agg_df["timestamp"])
    return agg_df


if __name__ == "__main__":
    import timeit

    start = timeit.default_timer()
    l = int(60 * 60 * 24 / 12)  # 1 day of ETH blocks
    default_params = Params()
    default_params.set_default_params()
    df = run_single_sim(l, default_params)
    df.to_csv("debug_df.csv", index=False)
    stop = timeit.default_timer()
    print("Run time for single simulation: ", stop - start)
