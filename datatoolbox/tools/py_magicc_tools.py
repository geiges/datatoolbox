#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 10:32:50 2019

@author: Andreas Geiges
"""

"""
PYMAGICC TOOLS
"""
#%%
import pymagicc as pym
import pandas as pd
import datatools as dt

#%%

#%% CONFIG for MAGICC 6
REQUIRED_GASES = {'HFC143a' : 'kt',
                'FossilCO2' : 'Mt',
                'HFC125' : 'kt',
                'BC' : 'Mt',
                'NOx' : 'MtN',
                'C6F14' : 'kt',
                'SF6' : 'kt',
                'CO' : 'Mt',
                'NMVOC' : 'Mt',
                'HFC23' : 'kt',
                'HFC134a' : 'kt',
                'HFC245fa' : 'kt',
                'CH4' : 'Mt',
                'OtherCO2' : 'Mt',
                'OC' : 'Mt',
                'NH3' : 'Mt',
                'N2O' : 'MtN2O-N',
                'HFC32' : 'kt',
                'HFC43-10' : 'kt',
                'HFC227ea' : 'kt',
                'C2F6' : 'kt',
                'SOx' : 'MtS',
                'CF4' : 'kt',}
PRIMAP_PATH = '/media/sf_Documents/primap_climatemodule/'

def writeMagic6ScenFile(table, filePath):
    def _create_column_header(headers):
        return pd.MultiIndex.from_arrays(
            arrays=[ headers["variables"], headers["todos"], headers["units"], headers["regions"]],
            names=("variable", "todo", "unit", "region"))
    efCopy = table.copy()
    
    
    ##% Magicc6 requires some chemical components in specific units
    #   which are converted in the following
    
    # ensure Mt as unit for the following component converions
#    for entity in ['NH3', 'FossilCO2', 'OtherCO2' ]:
#        efCopy.changeUnit(entity, 'Mt')
#    
#    #efCopy.convert(('N2O','N2O','MtN'), 14/28)
#    efCopy.convert(('NH3','NH3','MtN'), 14/17)
#    efCopy.convert(('FossilCO2', 'CO2', 'GtC'), 12/44/1000)
#    efCopy.convert(('OtherCO2', 'CO2', 'GtC'), 12/44/1000)
    #efCopy.convert(('Sulfur','Sulfur','MtS'), 32/64)
    #efCopy.convert(('NOx','NOx','MtN'), 14/44)
    
    
    headers = dict()
    headers['variables'] = list(efCopy.columns)
    headers['variables'] = pym.io.convert_magicc6_to_magicc7_variables(list(efCopy.columns))
    
    
    headers['units'] = list(efCopy.loc['Yrs',:])
    nCols = len(headers['units'])
    headers['regions'] = ['World']*nCols
    headers['todos'] = ['']*nCols
    
    columnIndex = _create_column_header(headers)
    
    #print(variables)
    ioData = pym.io.MAGICCData()
    #return efCopy
    efCopy = efCopy.iloc[1:,:].astype(float)
    ioData.df = pd.DataFrame(efCopy)
    ioData.df.index.name = "time"
    ioData.df.columns = columnIndex
    ioData.df = pd.DataFrame(ioData.df.T.stack(), columns=["value"]).reset_index()
    ioData.write(filepath=filePath, magicc_version=6)

def run_MAGGIC6(scenFile):
    scenario = scenFile.split('.')[0]
    import os 
    os.system('matlab -nodesktop -r "run_magicc6 ' + scenario + ' ' + PRIMAP_PATH + '"')
    
    outTemperature = dt.io.read_MAGICC6_MATLAB_bulkout(scenario + '/CSSENS_DAT_SURFACE_TEMP_BOXGLOBAL_MAGICC_INDIVIDUAL.OUT')
#    outTemperature = dt.io.read_MAGICC6_MATLAB_bulkout(scenario + '/CSSENS_DAT_SURFACE_TEMP_BOXGLOBAL_MAGICC_COMPARISON_MEAN.OUT')
    print('Median: ' + str(outTemperature.loc[2100,:].median()))
    print('p66: ' + str(outTemperature.loc[2100,:].quantile(.66)))
    print('p16: ' + str(outTemperature.loc[2100,:].quantile(.16)))
    print('max Median: ' + str(outTemperature.loc[:,:].median(axis=1).max()))
    return outTemperature


dt.greenhouse_gas_database
if __name__ == '__main__':
#    scenFile = 'WEO_2019_18.SCEN'
#    testData = pd.read_excel('/media/sf_Documents/data_files/IEA_trial_MAGICC.xlsx',index_col=0, sheet_name = 'IEA_SDS_18')
#    scenFile = 'WEO_2019_15b.SCEN'
#    testData = pd.read_excel('/media/sf_Documents/data_files/IEA_trial_MAGICC.xlsx',index_col=0, sheet_name = 'IEA_SDS_15_b')
    scenFile = 'WEO_2019_test.SCEN'
    
    for scen in ['IEA B2DS 4', 'IEA B2DS 2 rev', 'IEA B2DS 3']:
        scenFile = scen.replace(' ','_')+ '.SCEN'
        testData = pd.read_excel('/media/sf_Documents/data_files/Summary_of_IEA_B2DS_pathways_for_MAGICC.xlsx',index_col=0, sheet_name = scen)
        writeMagic6ScenFile(testData, scenFile)
        outTemperature = run_MAGGIC6(scenFile)
        
        import matplotlib.pylab as plt
        
        plt.plot(outTemperature.loc[outTemperature.index[::5],:].quantile(.66,axis=1))
        plt.plot(outTemperature.loc[outTemperature.index[::5],:].quantile(.5,axis=1))
        plt.plot(outTemperature.loc[outTemperature.index[::5],:].quantile(.14,axis=1))
        
        outDf = pd.DataFrame(columns = outTemperature.index[::5])
        outDf.loc['p84',:] = outTemperature.loc[outTemperature.index[::5],:].quantile(.84,axis=1)
        outDf.loc['p75',:] = outTemperature.loc[outTemperature.index[::5],:].quantile(.75,axis=1)
        outDf.loc['p66',:] = outTemperature.loc[outTemperature.index[::5],:].quantile(.66,axis=1)
        outDf.loc['p50',:] = outTemperature.loc[outTemperature.index[::5],:].quantile(.5,axis=1)
        outDf.loc['p36',:] = outTemperature.loc[outTemperature.index[::5],:].quantile(.36,axis=1)
        outDf.loc['p25',:] = outTemperature.loc[outTemperature.index[::5],:].quantile(.25,axis=1)
        outDf.loc['p16',:] = outTemperature.loc[outTemperature.index[::5],:].quantile(.16,axis=1)
        
        scenFile = scen.replace(' ','_')
        
        outDf.to_excel('MAGICC6_RUN_' + scenFile.split('.')[0] + '.xlsx')
