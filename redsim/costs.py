import math
import numpy as np
from typing import Dict

from redsim.params import Params


def compute_onchain_cost_metrics(
    sim_len: int, params: Params, market_path_dict: Dict[str, np.array]
) -> Dict[str, np.array]:
    # Compute costs associated with the Batcher
    batcher_cost_metrics_dict = compute_batcher_cost_metrics(
        sim_len, params, market_path_dict
    )
    batcher_gas_cost_arr = batcher_cost_metrics_dict["batcher_gas_cost_eth"]
    channel_l2_gas_arr = batcher_cost_metrics_dict["channel_l2_gas"]
    # Compute costs associated with the Proposer
    proposer_gas_cost_arr = compute_proposer_gas_cost(sim_len, params, market_path_dict)
    # Compute costs associated with DA challenges
    da_gas_cost_arr = compute_da_gas_cost(params, market_path_dict, channel_l2_gas_arr)
    # Bring all metrics together
    cost_metrics_dict = {
        "onchain_cost_eth": batcher_gas_cost_arr
        + proposer_gas_cost_arr
        + da_gas_cost_arr,
        "proposer_gas_cost_eth": proposer_gas_cost_arr,
        "da_gas_cost_eth": da_gas_cost_arr,
    }
    cost_metrics_dict.update(batcher_cost_metrics_dict)
    return cost_metrics_dict


def compute_batcher_cost_metrics(
    sim_len: int, params: Params, market_path_dict: Dict[str, np.array]
) -> Dict[str, np.array]:
    # Get parameters dicts
    market_params = params.get_market_params()
    protocol_params = params.get_protocol_params()
    # Get parameters
    max_channel_duration = protocol_params["max_channel_duration"]
    batcher_est_compr_ratio = protocol_params["batcher_est_compr_ratio"]
    batcher_target_size = protocol_params["batcher_target_size"]
    batcher_commit_size = market_params["batcher_commit_size"]
    # Get market variables
    l2_gas_arr = market_path_dict["l2_gas"]
    l1_base_fee_arr = market_path_dict["l1_base_fee_gwei"]
    l1_prio_fee_arr = market_path_dict["l1_prio_fee_gwei"]
    # Initialize variables
    time_since_post = 0
    channel_l2_gas = 0.0
    channel_l2_gas_arr = np.zeros(sim_len)
    ### channel_l2_gas_arr = gas units in the channel posted at t (if it exists!)
    batcher_txs_arr = np.zeros(sim_len)
    batcher_gas_cost_arr = np.zeros(sim_len)
    # Run for loop
    for t in range(sim_len):
        channel_l2_gas += l2_gas_arr[t]
        channel_l2_size = channel_l2_gas * batcher_est_compr_ratio
        # check posting conditions
        if (time_since_post >= max_channel_duration) or (
            channel_l2_size >= batcher_target_size
        ):
            # Batcher posts
            time_since_post = 0
            ### compute how many posting txs the Batcher makes
            batcher_txs = max(math.floor(channel_l2_size / batcher_target_size), 1)
            ### reset channel
            included_channel_size = batcher_txs * batcher_target_size
            excluded_channel_size = channel_l2_size - included_channel_size
            channel_l2_gas = max(excluded_channel_size, 0.0)
            ### compute gas cost
            l1_gas_fee = l1_base_fee_arr[t] + l1_prio_fee_arr[t]
            batcher_gas_cost = batcher_txs * batcher_commit_size * l1_gas_fee
            ### update arrays
            batcher_txs_arr[t] = batcher_txs
            channel_l2_gas_arr[t] = included_channel_size
            batcher_gas_cost_arr[t] = batcher_gas_cost
        else:
            # Batcher does not post
            time_since_post = time_since_post + 1
    # Bring all variables together
    batcher_cost_metrics_dict = {
        "batcher_gas_cost_eth": batcher_gas_cost_arr * 0.000000001,
        "batcher_txs": batcher_txs_arr,
        "channel_l2_gas": channel_l2_gas_arr,
    }
    return batcher_cost_metrics_dict


def compute_proposer_gas_cost(
    sim_len: int, params: Params, market_path_dict: Dict[str, np.array]
) -> np.array:
    # Get parameters dicts
    market_params = params.get_market_params()
    protocol_params = params.get_protocol_params()
    # Get parameters
    proposer_commit_size = market_params["proposer_commit_size"]
    submission_interval = protocol_params["submission_interval"]
    # Get market variables
    l1_base_fee_arr = market_path_dict["l1_base_fee_gwei"]
    l1_prio_fee_arr = market_path_dict["l1_prio_fee_gwei"]
    l1_gas_fee_arr = l1_base_fee_arr + l1_prio_fee_arr
    # Init variable
    proposer_gas_cost_arr = np.zeros(sim_len, dtype=float)
    for t in range(0, sim_len, submission_interval):
        proposer_gas_cost_arr[t] = proposer_commit_size * l1_gas_fee_arr[t]
    proposer_gas_cost_arr = proposer_gas_cost_arr * 0.000000001
    return proposer_gas_cost_arr


def compute_da_gas_cost(
    params: Params,
    market_path_dict: Dict[str, np.array],
    channel_l2_gas_arr: np.array,
) -> np.array:
    # Get parameters dicts
    market_params = params.get_market_params()
    protocol_params = params.get_protocol_params()
    # Get parameters
    compress_ratio = market_params["compress_ratio"]
    resolver_refund_factor = protocol_params["resolver_refund_factor"]
    # Get market variables
    da_challenge_arr = market_path_dict["da_challenge"]
    l1_base_fee_arr = market_path_dict["l1_base_fee_gwei"]
    l1_prio_fee_arr = market_path_dict["l1_prio_fee_gwei"]
    # Compute factors
    channel_l2_size_arr = compress_ratio * channel_l2_gas_arr
    l1_gas_fee_arr = l1_base_fee_arr + l1_prio_fee_arr
    # Adjust time on variables
    da_challenge_adj_arr = np.roll(da_challenge_arr, 1)
    da_challenge_adj_arr[0] = 0.0
    channel_l2_size_adj_arr = np.roll(channel_l2_size_arr, 2)
    channel_l2_size_adj_arr[0] = 0.0
    channel_l2_size_adj_arr[1] = 0.0
    # Compute DA cost
    da_gas_cost_arr = (
        da_challenge_adj_arr
        * channel_l2_size_adj_arr
        * l1_gas_fee_arr
        * (1 - resolver_refund_factor)
    ) * 0.000000001
    return da_gas_cost_arr
