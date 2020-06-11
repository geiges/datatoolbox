import datatoolbox as dt

import numpy.testing as npt

def test_base_conversion1():
    #testing compound units and addtional units (CO2eq)
    hr_in_yr = dt.conversionFactor('yr', 'hr')
    npt.assert_almost_equal(hr_in_yr, 8766) # were counting leap years now?
    
    obs = dt.conversionFactor('Mt CO2eq / yr', 'kg CO2eq / hr')
    exp = 1e9 / hr_in_yr
    npt.assert_almost_equal(obs, exp)

def test_custom_base_conversion1():    
    obs = dt.conversionFactor('t oil_equivalent/capita', 'MJ/capita')
    exp = 41868
    npt.assert_almost_equal(obs, exp)


def test_GWP_conversion_CO():    
    obs = dt.conversionFactor("Mt CO", "Gg CO2eq", context="GWPAR4")
    exp = 1000
    npt.assert_almost_equal(obs, exp)

def test_GWP_conversion_CH4():        
    obs = dt.conversionFactor("Mt CH4", "Mt CO2eq", context="GWPAR4")
    exp = 25
    npt.assert_almost_equal(obs, exp)
    
def test_HFC_units():    
    dt.core.ur('HFC134aeq') 
    
def test_function_getUnit():
    
    dt.core.getUnit('°C')
    dt.core.getUnit('$')
    dt.core.getUnit('€')
