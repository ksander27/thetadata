# Get contracts 
from tdwrapper.wrapper import Option
from tdwrapper.wrapper import RootOrExpirationError,ResponseFormatError

import os
import pandas as pd
import numpy as np


DIR = "./data/AAPL"

args = {
    "root":"aapl"
    ,"exp":"20201120"
    ,"right":"cal"
    ,"strike":40
}

aapl = Option(**args)

params = {
    "start_date":"20200825"
    ,"end_date":"20200825"
    ,"s_of_day":12*3600
}

test = aapl.get_at_time_implied_volatility_verbose(**params)