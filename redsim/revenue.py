import numpy as np
from typing import Dict

from redsim.params import Params


def compute_revenue_metrics(
    params: Params, market_path_dict: Dict[str, np.array]
) -> Dict[str, np.array]:
    # Get parameters dicts
    market_params = params.get_market_params()
    protocol_params = params.get_protocol_params()
    # Compute revenue from L2 pririty fees
    l2_gas_arr = market_path_dict["l2_gas"]
    l2_base_fee_arr = market_path_dict["l2_base_fee_gwei"]
    l2_prio_fee_arr = market_path_dict["l2_prio_fee_gwei"]
    revenue_exec_arr = l2_gas_arr * (l2_base_fee_arr + l2_prio_fee_arr)
    # Compute revenue from data fees
    l1_base_fee_arr = market_path_dict["l1_base_fee_gwei"]
    compress_ratio = market_params["compress_ratio"]
    l1_fee_overhead = protocol_params["l1_fee_overhead"]
    l1_fee_scalar = protocol_params["l1_fee_scalar"]
    l1_fee_scalar_adj = l1_fee_scalar / 1000000.0
    data_gas_arr = compress_ratio * l2_gas_arr
    revenue_data_arr = (
        (data_gas_arr + l1_fee_overhead) * l1_fee_scalar_adj * l1_base_fee_arr
    )
    # Bring all metrics together
    revenue_metrics_dict = {
        "revenue_eth": (revenue_data_arr + revenue_exec_arr) * 0.000000001,
        "revenue_data_eth": revenue_data_arr * 0.000000001,
        "revenue_exec_eth": revenue_exec_arr * 0.000000001,
    }
    return revenue_metrics_dict
