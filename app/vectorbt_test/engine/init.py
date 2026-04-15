
# Import all modules to register nodes
from vectorbt_test.core.indicators import register_indicators
from vectorbt_test.core.signals import register_signals
from vectorbt_test.core.factors import register_factors
from vectorbt_test.core.functions import register_functions
from vectorbt_test.core.data import register_data


def load_register_nodes():
    register_indicators()
    register_signals()
    register_factors()
    register_functions()
    register_data()
