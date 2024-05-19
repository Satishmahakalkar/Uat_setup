

from typing import Literal

import numpy as np


def process(price_array: np.ndarray, current_price: float) -> Literal["BUY", "SELL", "HOLD"]:
    ...