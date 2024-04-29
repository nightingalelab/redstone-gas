from typing import Any, Dict, Optional


class Params:
    def __init__(self) -> None:
        self.protocol_params = dict()
        self.market_params = dict()

    def set_default_params(self) -> None:
        self.set_default_protocol_params()
        self.set_default_market_params()

    def set_default_protocol_params(self) -> None:
        self.protocol_params = {
            # L2 gas fees
            "l1_fee_overhead": 2100.0,
            "l1_fee_scalar": 100000.0,
            # Batcher
            "max_channel_duration": 6,
            # TODO: Check conversion from bytes to gas units (param = 100000 bytes)
            "batcher_target_size": 100000.0 * 16.0,
            "batcher_est_compr_ratio": 0.4,
            # Proposer
            "submission_interval": 300,
            # DA resolver
            "resolver_refund_factor": 0.0,
        }

    def set_default_market_params(self) -> None:
        self.market_params = {
            # L1 gas prices
            "l1_base_fee_shape": 1.8,
            "l1_base_fee_loc": 6.3,
            "l1_base_fee_scale": 11.4,
            "l1_prio_shape": 0.0002,
            "l1_prio_scale": 80.0,
            # L2 gas prices
            "l2_base_shape": 0.5,
            "l2_base_scale": 0.044,
            "l2_prio_shape": 3.2e-05,
            "l2_prio_scale": 1.5,
            # L2 gas usage
            "gas_use_low_prob": 0.76,
            "gas_use_low_threshold": 350000,
            "gas_use_low_mu": 301000,
            "gas_use_low_sigma": 6685,
            "gas_use_high_shape": 0.06,
            "gas_use_high_loc": 0.35 * 1000000,
            "gas_use_high_scale": 7.2 * 1000000,
            # L1 gas costs
            "da_chalenge_p": 0.0007,
            "compress_ratio": 0.4,
            "batcher_commit_size": 21500.0,  # TODO: Check batcher_commit_size
            "proposer_commit_size": 90900.0,  # TODO: Check proposer_commit_size
            # Infra costs
            "infra_costs_mu": 0.047,
            "infra_costs_sigma": 0.01,
            # ETH exchange rate
            "eth_price_usd": 2500.0,
        }

    def get_param(self, param_name: str) -> Optional[Dict[str, Any]]:
        if self.is_market_param(param_name):
            return self.market_params[param_name]
        elif self.is_protocol_param(param_name):
            return self.protocol_params[param_name]
        else:
            return None

    def set_param(self, param_name: str, param_val: Any) -> None:
        if self.is_market_param(param_name):
            self.set_market_param(param_name, param_val)
        elif self.is_protocol_param(param_name):
            self.set_protocol_param(param_name, param_val)
        else:
            print("Invalid parameter name for Param")

    def get_protocol_params(self) -> Dict[str, Any]:
        return self.protocol_params

    def set_protocol_param(self, param_name: str, param_val: Any) -> None:
        self.protocol_params[param_name] = param_val

    def get_market_params(self) -> Dict[str, Any]:
        return self.market_params

    def set_market_param(self, param_name: str, param_val: Any) -> None:
        self.market_params[param_name] = param_val

    def is_protocol_param(self, param_name: str) -> bool:
        protocol_params = [
            "l1_fee_overhead",
            "l1_fee_scalar",
            "max_channel_duration",
            "batcher_target_size",
            "batcher_est_compr_ratio",
            "submission_interval",
        ]
        return param_name in protocol_params

    def is_market_param(self, param_name: str) -> bool:
        market_params = [
            "l1_base_fee_shape",
            "l1_base_fee_loc",
            "l1_base_fee_scale",
            "l1_base_fee_zero",
            "l1_prio_shape",
            "l1_prio_scale",
            "l2_prio_shape",
            "l2_prio_scale",
            "gas_use_low_prob",
            "gas_use_low_threshold",
            "gas_use_low_mu",
            "gas_use_low_sigma",
            "gas_use_high_shape",
            "gas_use_high_loc",
            "gas_use_high_scale",
            "da_chalenge_p",
            "compress_ratio",
            "batcher_commit_size",
            "proposer_commit_size",
            "infra_costs_mu",
            "infra_costs_sigma",
            "eth_price_usd",
        ]
        return param_name in market_params
