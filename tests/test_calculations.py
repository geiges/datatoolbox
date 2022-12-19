import datatoolbox as dt
import pandas as pd
import numpy as np
import numpy.testing as npt

#%%

metaDict = {'entity':'Emissions|Candy',
                           'category': '',
                           'scenario':'Historic',
                           'source': 'SANDBOX',
                           'unit' : 'Mt CO2'}

metaDict2 = {'entity':'Emissions|Candy',
                           'category': '',
                           'scenario':'Historic',
                           'source': 'SANDBOX',
                           'unit' : 'Gg CO2'}

df1 = dt.Datatable([[10,20,30,],
                   [40,40,50,]], 
                   columns = [2000, 2010, 2020],
                   index= ['DEU', 'IND'],
                   meta = metaDict)

df2 = dt.Datatable([[50.,50,50,],
                   [40,40,40,]], 
                   columns = [2000, 2010, 2020],
                   index= ['DEU', 'IND'],
                   meta = metaDict)

df3 = dt.Datatable([[50,50],
                   [40,40]], 
                   columns = [2000, 2010],
                   index= ['DEU', 'IND'],
                    meta = metaDict2)

def test_addition_int():
    
    # adding integer
    exp = dt.Datatable([[30,40,50,],
                       [60,60,70,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    obs = df1 + 20    
    # TODO use with new pandas version
    # assert_frame_equal(obs, exp, check_dtype=False)
    npt.assert_array_almost_equal(obs, exp, decimal = 6)
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'
    
    obs = 20 + df1
    # TODO use with new pandas version
    # assert_frame_equal(obs, exp, check_dtype=False)
    npt.assert_array_almost_equal(obs, exp, decimal = 6)
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

def test_addition_dfs():
        
    # adding two datatables
    obs = df1 + df2
    exp = dt.Datatable([[60,70,80,],
                       [80,80,90,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    # TODO use with new pandas version
    # assert_frame_equal(obs, exp, check_dtype=False)
    npt.assert_array_almost_equal(obs, exp, decimal = 6)
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

def test_raddition_dfs_conv():
    # adding two datatables + conversion
    obs = df1 + df3
    exp = dt.Datatable([[10.05,  20.05, np.nan],
                       [40.04,  40.04,  np.nan]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    # TODO use with new pandas version
    # assert_frame_equal(obs, exp, check_dtype=False)
    npt.assert_array_almost_equal(obs, exp, decimal = 6)
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

    
def test_laddition_dfs_conv():
    
    obs = df3 + df1
    exp = dt.Datatable([[10050.,  20050, np.nan],
                       [40040,  40040,  np.nan]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict2)

    # TODO: conversion makes this a float, fix?
    # TODO use with new pandas version
    # assert_frame_equal(obs, exp, check_dtype=False)
    npt.assert_array_almost_equal(obs, exp, decimal = 6)
    assert obs.loc[:,2020].isnull().all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

def test_sum():
    obs = sum([df3, df1])
    exp = dt.Datatable([[10050.,  20050, np.nan],
                       [40040,  40040,  np.nan]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict2)

    # TODO: conversion makes this a float, fix?
    # TODO:use with new pandas version
    #assert_frame_equal(obs, exp, check_dtype=False)
 
    npt.assert_array_almost_equal(obs, exp, decimal = 5)
    assert obs.loc[:,2020].isnull().all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'  
    
def test_rsubstraction_int():
    
    # adding integer
    exp = dt.Datatable([[-10, 0,10,],
                       [20,20,30,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    obs = df1 - 20
    #assert_frame_equal(obs, exp)
    npt.assert_array_almost_equal(obs, exp, decimal = 5)
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'
    
def test_lsubstraction_int():
    
    exp = dt.Datatable([[10, 0,-10,],
                       [-20,-20,-30,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    obs = 20 - df1
    #assert_frame_equal(obs, exp)
    npt.assert_array_almost_equal(obs, exp, decimal = 5)
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

def test_substraction_dfs():
    
    # adding two datatables
    obs = df1 - df2
    exp = dt.Datatable([[-40,-30,-20,],
                       [0,0,10,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    #assert_frame_equal(obs, exp)
    npt.assert_array_almost_equal(obs, exp, decimal = 5)
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

def test_rsubstraction_dfs_conv():

    # adding two datatables + conversion
    
    obs = df1 - df3
    exp = dt.Datatable([[9.95,  19.95, np.nan],
                       [39.96,  39.96,  np.nan]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    

    # TODO:use with new pandas version
    #assert_frame_equal(obs, exp)

    npt.assert_array_almost_equal(obs, exp, decimal = 5)
    assert obs.loc[:,2020].isnull().all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

def test_lsubstraction_dfs_conv():
    
    obs = df3 - df1
    exp = dt.Datatable([[-9950.0, -19950.0, np.nan],
                       [-39960.0, -39960.0,  np.nan]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict2)

    # TODO:use with new pandas version
    # assert_frame_equal(obs, exp)
    
    npt.assert_array_almost_equal(obs, exp, decimal = 5)
    assert obs.loc[:,2020].isnull().all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

ur = dt.core.ur
def test_computation_with_pint_quantities():
    df1 + (1* ur('Mt CO2'))
    df1 - (2* ur('Mt CO2'))
    df1 * (1* ur('kg /(Mt CO2)'))
    df1 / (1* ur('Mt CO2 /kg'))
    
def test_rhs_computation_with_pint_quantities():
    (100* ur('Mt CO2 /kg')) / df1
    (100* ur('Mt CO2 /kg')) * df1
