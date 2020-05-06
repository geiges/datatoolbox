import datatoolbox as dt
import pandas as pd
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

df2 = dt.Datatable([[50,50,50,],
                   [40,40,40,]], 
                   columns = [2000, 2010, 2020],
                   index= ['DEU', 'IND'],
                   meta = metaDict)

df3 = dt.Datatable([[50,50],
                   [40,40]], 
                   columns = [2000, 2010],
                   index= ['DEU', 'IND'],
                    meta = metaDict2)

def test_addition():
    
    # adding integer
    exp = dt.Datatable([[30,40,50,],
                       [60,60,70,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    obs = df1 + 20
    
    assert (obs.values == exp.values).all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

    obs = 20 + df1
    
    assert (obs.values == exp.values).all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'
    
    # adding two datatables
    obs = df1 + df2
    exp = dt.Datatable([[60,70,80,],
                       [80,80,90,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    assert (obs.values == exp.values).all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

    # adding two datatables + conversion
    obs = df1 + df3
    exp = dt.Datatable([[10.05,  20.05, pd.np.nan],
                       [40.04,  40.04,  pd.np.nan]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    assert (obs.loc[:,[2000,2010]].values == exp.loc[:,[2000,2010]].values).all()
    assert obs.loc[:,2020].isnull().all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'
    
    obs = df3 + df1
    exp = dt.Datatable([[10050,  20050, pd.np.nan],
                       [40040,  40040,  pd.np.nan]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict2)
    
    assert (obs.loc[:,[2000,2010]].values == exp.loc[:,[2000,2010]].values).all()
    assert obs.loc[:,2020].isnull().all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'


def test_substraction():
    
    # adding integer
    exp = dt.Datatable([[-10, 0,10,],
                       [20,20,30,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    obs = df1 - 20
    
    assert (obs.values == exp.values).all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

    exp = dt.Datatable([[10, 0,-10,],
                       [-20,-20,-30,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    obs = 20 - df1
    
    assert (obs.values == exp.values).all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'
    
    # adding two datatables
    obs = df1 - df2
    exp = dt.Datatable([[-40,-30,-20,],
                       [0,0,10,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    assert (obs.values == exp.values).all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

    # adding two datatables + conversion
    obs = df1 - df3
    exp = dt.Datatable([[9.95,  19.95, pd.np.nan],
                       [39.96,  39.96,  pd.np.nan]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    assert (obs.loc[:,[2000,2010]].values == exp.loc[:,[2000,2010]].values).all()
    assert obs.loc[:,2020].isnull().all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'
    
    obs = df3 - df1
    exp = dt.Datatable([[-9950.0, -19950.0, pd.np.nan],
                       [-39960.0, -39960.0,  pd.np.nan]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict2)
    
    assert (obs.loc[:,[2000,2010]].values == exp.loc[:,[2000,2010]].values).all()
    assert obs.loc[:,2020].isnull().all()
    assert obs.meta['unit']   == exp.meta['unit']
    assert obs.meta['source'] == 'calculation'

#test_addition()
#test_substraction()