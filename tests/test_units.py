import datatoolbox as dt

def test_base_conversion():
    #testing compound units and addtional units (CO2eq)
    obs = dt.conversionFactor('Mt CO2eq / yr', 'kg CO2eq / hr')
    exp = 114079.55270694658
    
    assert obs == exp
    
    
    obs = dt.conversionFactor('t oil_equivalent/capita', 'MJ/capita')
    exp = 41868.00000000001
    assert obs == exp


def test_GWP_conversion():
    
    obs = dt.conversionFactor("Mt CO", "Gg CO2eq", context="GWPAR4")
    exp = 1000
    assert obs == exp
    
    obs = dt.conversionFactor("Mt CH4", "Mt CO2eq", context="GWPAR4")
    exp = 25
    assert obs == exp
    
def test_HFC_units():
    
    dt.core.ur('HFC134aeq') 