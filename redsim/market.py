import numpy as np
from typing import Dict, Any
from scipy.stats import gamma, bernoulli, norm


def generate_market_variables_paths(
    sim_len: int, market_params: Dict[str, Any]
) -> Dict[str, np.array]:
    path_dict = {
        # Note that fees are denominated in gwei!
        "l1_base_fee_gwei": generate_l1_base_fee_path(sim_len, market_params),
        "l1_prio_fee_gwei": generate_l1_prio_fee_path(sim_len, market_params),
        "l2_base_fee_gwei": generate_l2_base_fee_path(sim_len, market_params),
        "l2_prio_fee_gwei": generate_l2_prio_fee_path(sim_len, market_params),
        "l2_gas": generate_l2_gas_path(sim_len, market_params),
        "da_challenge": generate_da_challenge_path(sim_len, market_params),
        "infra_cost_usd": generate_infra_cost_path(sim_len, market_params),
    }
    return path_dict


def generate_l1_base_fee_path(sim_len: int, market_params: Dict[str, Any]) -> np.array:
    # Get parameters
    shape_param = market_params["l1_base_fee_shape"]
    loc_param = market_params["l1_base_fee_loc"]
    scale_param = market_params["l1_base_fee_scale"]
    # Generate samples from Gamma Distribution
    base_fee_arr = gamma.rvs(
        a=shape_param, loc=loc_param, scale=scale_param, size=sim_len
    )
    return base_fee_arr


def generate_l1_prio_fee_path(sim_len: int, market_params: Dict[str, Any]) -> np.array:
    # Get parameters
    shape_param = market_params["l1_prio_shape"]
    scale_param = market_params["l1_prio_scale"]
    # Generate samples from Gamma Distribution
    prio_fee_arr = gamma.rvs(a=shape_param, scale=scale_param, size=sim_len)
    return prio_fee_arr


def generate_l2_base_fee_path(sim_len: int, market_params: Dict[str, Any]) -> np.array:
    # Get parameters
    shape_param = market_params["l2_base_shape"]
    scale_param = market_params["l2_base_scale"]
    # Generate samples from Gamma Distribution
    base_fee_arr = gamma.rvs(a=shape_param, scale=scale_param, size=sim_len)
    return base_fee_arr


def generate_l2_prio_fee_path(sim_len: int, market_params: Dict[str, Any]) -> np.array:
    # Get parameters
    shape_param = market_params["l2_prio_shape"]
    scale_param = market_params["l2_prio_scale"]
    # Generate samples from Gamma Distribution
    prio_fee_arr = gamma.rvs(a=shape_param, scale=scale_param, size=sim_len)
    return prio_fee_arr


def generate_da_challenge_path(sim_len: int, market_params: Dict[str, Any]) -> np.array:
    # Get parameters
    p_param = market_params["da_chalenge_p"]
    # Generate samples from Bernoulli Distribution
    da_challenge_arr = bernoulli.rvs(p=p_param, size=sim_len)
    return da_challenge_arr


def generate_infra_cost_path(sim_len: int, market_params: Dict[str, Any]) -> np.array:
    # Get parameters
    mu_param = market_params["infra_costs_mu"]
    sigma_param = market_params["infra_costs_sigma"]
    # Generate samples from Bernoulli Distribution
    infra_cost_arr = norm.rvs(loc=mu_param, scale=sigma_param, size=sim_len)
    return infra_cost_arr


def generate_l2_gas_path(sim_len: int, market_params: Dict[str, Any]) -> np.array:
    # Get market parameters
    low_prob = market_params["gas_use_low_prob"]
    low_mu = market_params["gas_use_low_mu"]
    low_sigma = market_params["gas_use_low_sigma"]
    high_shape = market_params["gas_use_high_shape"]
    high_loc = market_params["gas_use_high_loc"]
    high_scale = market_params["gas_use_high_scale"]
    # Generate samples
    dist_samples = np.random.choice(
        ["low", "high"], size=sim_len, p=[low_prob, 1 - low_prob]
    )
    norm_samples = norm.rvs(loc=low_mu, scale=low_sigma, size=sim_len)
    gamma_samples = gamma.rvs(
        a=high_shape, loc=high_loc, scale=high_scale, size=sim_len
    )
    l2_gas_arr = np.where(dist_samples == "low", norm_samples, gamma_samples)
    return l2_gas_arr
