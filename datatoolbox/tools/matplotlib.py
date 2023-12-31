#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains all relevant tools regarding the use of matplotlib


@author: ageiges
"""

def colorGenerator(reset=False):
    """
    Return a generator for longer list of well distinquiable
    colors for the use in for loops
    
    """
    
    _colors = ['#1f77b4',
            '#aec7e8', #aec7e8
            '#ff7f0e', #ff7f0e
            '#ffbb78', #ffbb78
            '#2ca02c', #2ca02c
            '#98df8a', #98df8a
            '#d62728', #d62728
            '#ff9896', #ff9896
            '#9467bd', #9467bd
            '#c5b0d5', #c5b0d5
            '#8c564b', #8c564b
            '#c49c94', #c49c94
            '#e377c2', #e377c2
            '#f7b6d2', #f7b6d2
            '#7f7f7f', #7f7f7f
            '#c7c7c7', #c7c7c7
            '#bcbd22', #bcbd22
            '#dbdb8d', #dbdb8d
            '#17becf', #17becf
            '#9edae5', #9edae5
            ]
    for color in _colors:
        yield(color)
        
        
    
def savefig(*args, **kwargs):
    """
    Wrapper around plt.savefig to append the filename of the creating scrip
    for re-producibiltiy
    """
    import inspect, os
    import matplotlib.pylab as plt
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    try:
        pyFileName = os.path.basename(module.__file__)
    except:
        pyFileName = 'unkown'
    
    plt.gcf().text(0.01, 0.02, 'file:' + pyFileName)
    plt.savefig(*args, **kwargs)