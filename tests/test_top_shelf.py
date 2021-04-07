import numpy as np

from datatoolbox import top_shelf

def test_share_of_emissions_default():
    # this test uses the last-year default and therefore may need to be
    # updated if historical data is updated
    obs = top_shelf.share_of_emissions(['CHN', 'USA'])
    exp = 0.4098739495798319
    assert np.isclose(obs, exp)

def test_share_of_emissions_year():
    obs = top_shelf.share_of_emissions(['CHN', 'USA'], year=2010)
    exp = 0.41070615034168567
    assert np.isclose(obs, exp)

def test_share_of_emissions_gwp_sar():
    obs = top_shelf.share_of_emissions(['CHN', 'USA'], year=2010, gwp='SAR')
    exp = 0.41475409836065574
    assert np.isclose(obs, exp)