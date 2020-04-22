#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 09:05:54 2020


KAYA identity decomposition
According to :
Sun, J., Ang, B., 2000. Some properties of an exact energy decomposition model. Energy 25, 1177–1188.

Emisisons  =  (emisisons/energy) * (energy/GDP) * (GDP/population) * population
I : Emission intensity per energy
E : Efficiency of energy per GDP
A : Affluence of economy
P : Population
E = I * E * A * P


@author: Andreas Geiges (andreas.geiges@climateanalytics.org)
@reviewer: Gaurav Gandi gaurav.ganti@climateanalytics.org 

"""
#import datatools as dt
import numpy as np
import pandas as pd


colors = ['red', 'green', 'royalblue', 'orange']

#def determineRegion(country):
#    
#    for region in dt.mapping.regions.R5.listAll():
#        
#        if country in dt.mapping.regions.R5.membersOf(region):
#            return region
#
#determineRegion('CHN')
#%%

def _decomposition_to_additive(X, deltaX):
    
    for i in [1,0,2,3]:
        assert X[i].meta['unit'] == deltaX[i].meta['unit'] 
        
    addiditveFactor = \
              deltaX[0] * ( X[1]  * X[2] * X[3]) + \
              deltaX[0] * (1/2) * ( ( deltaX[1] * X[2]      * X[3]) + \
                                    ( X[1]      * deltaX[2] * X[3] ) + \
                                    ( X[1]      * X[2]      * deltaX[3]) )+ \
              deltaX[0] * (1/3) * ( deltaX[1] * deltaX[2] * X[3] + \
                                    deltaX[1] * X[2]      * deltaX[3] + \
                                    X[1]      * deltaX[2] * deltaX[3]) + \
              deltaX[0] * (1/4) * (deltaX[1] *deltaX[2] *deltaX[3]) 
              
    return addiditveFactor

def decomposition_four_factors(X):
    """
    params: 
        X_t0: value of four factors at first timestep t=0
        X_t1: value of four factors at latter timestep t=1
    """
    
    # decomosition works for only for factors
    assert len(X) == 4
#    assert len(X_t1) == 4
    

    
    # computation of differences
    deltaX = [x.yearlyChange() for x in X]
#    for x in X:
#        print(x.meta['unit'] + ' * ', end='')
#    
#    for x in deltaX:
#        print(x.meta['unit'] + ' * ', end='')
#
#    for x in [deltaX[i] for i in [1,0,2,3]]:
#        print(x.meta['unit'] + ' * ', end='')
    

    aX0 = _decomposition_to_additive(X, deltaX)
    aX1 = _decomposition_to_additive([X[i] for i in [1,0,2,3]], [deltaX[i] for i in [1,0,2,3]])
    aX2 = _decomposition_to_additive([X[i] for i in [2,0,1,3]], [deltaX[i] for i in [2,0,1,3]])
    aX3 = _decomposition_to_additive([X[i] for i in [3,0,1,2]], [deltaX[i] for i in [3,0,1,2]])
    
#    aX0.column = [x+1 for x in aX0.columns]
#    aX1.column = [x+1 for x in aX1.columns]
#    aX2.column = [x+1 for x in aX2.columns]
#    aX3.column = [x+1 for x in aX3.columns]
    #print('error: {}'.format(((aX0 + aX1 + aX2 + aX3) - deltaE).loc[:,2016]))
#    import copy
    return ( [aX0, aX1, aX2, aX3])
#    deltaX = [copy.copy(deltaX[i]) for i in [1,0,2,3]]
#    X = [copy.copy(X[i]) for i in [1,0,2,3]]
 
def kaya_decomposion(factors, totalAbs, plot=True, labels = None):
        
        AX = decomposition_four_factors(factors)
    
        if plot:
            kaya_visualization(AX, totalAbs, labels)

def kaya_visualization(AX, totalAbs, labels, relativeChange = False):
    
    import matplotlib.pylab as plt
    
    if len(AX) != len(labels):
        print('inconsitent input')
        
    
    years = AX[0].columns
    countries = AX[0].index
    
    
    
    for country in countries:
        #%%
        plt.figure(num=country)
        plt.clf()
    
        # positive values plotting
        bottom = np.zeros(len(years))
        total  = np.zeros(len(years))
        handles = list()
        for i,factorAbs in enumerate(AX):
        
            if relativeChange:
                factor = 100*(factorAbs / totalAbs)
            else:
                factor = factorAbs
            mask = factor.loc[country,:] >0
            
            h = plt.bar(years[mask], factor.loc[country,mask], bottom=bottom[mask],color=colors[i])
            bottom[mask] += factor.loc[country,mask]
            total[mask]  += factor.loc[country,mask]
            handles.append(h)
        plt.legend(labels)
       
        bottom = np.zeros(len(years))
        for i in range(3,-1,-1):
            if relativeChange:
                factor = 100* (AX[i] /totalAbs)
            else:
                factor = AX[i]
            mask = factor.loc[country,:] <0
            
            plt.bar(years[mask],  factor.loc[country,mask], bottom=bottom[mask],color=colors[i])
            bottom[mask] += factor.loc[country,mask]
            total[mask]  += factor.loc[country,mask]
        plt.plot(years, total, 'kd', markersize=4)
        plt.savefig(country + '_composition.png')
        
    
        
         #%%   
if __name__ == '__main__':
    #%%
    import datatools as dt
    import matplotlib.pylab as plt

    visCountries = ['CHN', 'USA', 'GBR']
    visYears        = list(range(1990,2018))


    populationData = dt.getTable('Population||historic|WDI_2019')#.convert('count')
#    populationData.meta['unit'] = populationData.meta['unit'] + ' / yr'
    gdpData = dt.getTable('GDP|current||historic|WDI_2019').convert('trUSD')
    gdpData.meta['unit'] = gdpData.meta['unit'] + ' / yr'
    energyData = dt.getTable('Final_Energy|Total||historic|IEA_WEB_2019')#.convert('MJ / yr')
    emissionData = dt.getTable('Emissions|Fuel|CO2|Total||historic|IEA_CO2_FUEL_2019')
    emissionData.meta['unit'] = emissionData.meta['unit'] + ' / yr'
    #emissionData = emissionData.convert('kg CO2 / yr')
    #%% computation of the four factors
    intensity = emissionData / energyData
    intensity.meta['entity'] = "emission intensity"
    
    efficiency = (energyData / gdpData)
    efficiency.meta['entity'] = "energy efficiency"
    
    affluence = (gdpData / populationData)
    affluence.meta['entity'] = "affluence (wealth)"
    
    #%% viusualization
    fig = plt.figure('comparison', figsize= [10,10])
    plt.clf()
    fig, ax = plt.subplots(2,2, num='comparison', figsize= [10,10])
    
    intensity.loc[visCountries,visYears].vis.plot(ax= ax[0,0])
    efficiency.loc[visCountries,visYears].vis.plot(ax= ax[0,1])
    affluence.loc[visCountries,visYears].vis.plot(ax= ax[1,0])
    populationData.loc[visCountries,visYears].vis.plot(ax= ax[1,1])
    plt.tight_layout()
    plt.savefig('factor_comparison.png')
    
    #%% test of emission computation
    deltaE = emissionData.yearlyChange().loc[visCountries,visYears]
    emissionsComputed = populationData * affluence * efficiency * intensity
    print('error: {}'.format((emissionsComputed - emissionData).max().max()))

    X = [populationData.loc[visCountries,visYears], affluence.loc[visCountries,visYears], efficiency.loc[visCountries,visYears], intensity.loc[visCountries,visYears]]
    
    AX = decomposition_four_factors(X)
    
    labels = ['Population','GDP/capita','Energy Effiency', 'Emission Intensity']
    
    kaya_visualization(AX, emissionData.loc[visCountries,visYears], labels, relativeChange=True)
