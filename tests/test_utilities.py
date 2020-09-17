#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 18 13:57:45 2020

@author: ageiges
"""

import datatoolbox as dt
import numpy as np
import pandas as pd
import os

#def test_create_graph_basic():
#    from datatoolbox.utilities import get_data_trees,  plot_tree
#    kwargs = dict(model="MESSAGE", scenario="SSP2-19")
#    graphs, scenario = get_data_trees(**kwargs)
#    plot_tree(graphs["Emissions"], scenario, savefig_path=(os.path.join(os.getcwd(), "test.png")))
#
#def test_graph_from_search():
#    from datatoolbox.utilities import plot_query_as_graph
#    res = dt.find(entity = 'Area', source='WDI')
#    plot_query_as_graph(res)
#    
#
#def test_graph_integration():
#    dt.find(entity = 'Area', source='WDI').graph()

#if __name__ == '__main__':
#    test_create_graph_basic()
#    test_graph_from_search()
#    test_graph_integration()