#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 17:25:52 2019

@author: Andreas Geiges
"""
import datatools as dt
import pandas as pd


def initializeSpatialMapping():
    #SETUP
    MAPPING_FILE_PATH = 'data/region_mappings.csv'
    CONTINENT_FILE_PATH = 'data/all.csv'
    
    print('Using file ' + MAPPING_FILE_PATH + ' to create initial mapping')
            
    mappingDf = pd.read_csv(MAPPING_FILE_PATH, index_col=0)
    continentDf = pd.read_csv(CONTINENT_FILE_PATH, index_col=0)
    continentDf = continentDf.set_index('alpha-3')
    
#    for co in mappingDf.index:
#        if co not in dt.geoMapping.countries:
#            print(co)
    mappingDf = mappingDf.loc[dt.mapp.countries.listAll()]
    # EU28
    dt.mapp.addNewRegion('EU28', mappingDf.eu28=='EU')
    
    for continent in continentDf.region.unique():
        if pd.isna(continent):
            continue
        
        dt.geoMapping.addNewRegion(continent, continentDf.region==continent)
    
    dt.geoMapping.commitMapping()
        
def initSourceTable():
    sourceDf = pd.DataFrame(columns = dt.config.SOURCE_META_FIELDS)
    
    sourceDf.to_csv(dt.config.SOURCE_FILE)
    dt.core.DB._gitAddFile(dt.config.SOURCE_FILE)
    dt.core.DB._gitCommit('added empty source file')
    

def initCATRegions():
    rawInput = {'R5OECD' : 'AIAALAANDASMATAATFAUSAUTBELBVTCANCCKCHECHICXRCYMDEUDNKESPFINFJIFRAFROGBRGGYGIBGRCGRLGUMIMNIOTIRLISLITAJEYJPNLIELUXMCONCLNFKNLDNORNZLPRTPYFSJMSLBSMRSPMSWETURUMIUSAVATVGBVIRVUTWSM',
                'R5MAF'  : 'AGOAREBDIBENBFABHRBWACAFCIVCMRCODCOGCOMCPVDJIDZAEGYERIESHETHGABGHAGINGMBGNBGNQIRNIRQISRJORKENKWTLBNLBRLBYLSOMARMDGMLIMOZMRTMUSMWIMYTNAMNERNGAOMNPSEQATREURWASAUSDNSENSHNSLESOMSSDSTPSWZSYRTCDTGOTUNTZAUGAYEMZAFZMBZWE',
                'R5REF'  : 'ALBARMAZEBGRBIHBLRCYPCZEESTGEOHRVHUNKAZKGZLTULVAMDAMKDMLTMNEPOLROURUSSRBSVKSVNTJKTKMUKRUZB',
                'R5ASIA' : 'AFGBGDBRNBTNCHNCOKFSMHKGHMDIDNINDKHMKIRKORLAOLKAMACMDVMHLMMRMNGMNPMYSNIUNPLNRUPAKPHLPLWPNGPRKSGPSYCTHATKLTLSTONTUVTWNVNMWLF',
                'R5LAM'  : 'ABWANTARGATGBESBHSBLMBLZBMUBOLBRABRBCHLCOLCRICUBCUWDMADOMECUFLKGLPGRDGTMGUFGUYHNDHTIJAMKNALCAMAFMEXMSRMTQNICPANPCNPERPRIPRYSGSSLVSURSXMTCATTOURYVCTVEN'}
    mappingToCountries = dict()
    for region in rawInput.keys():
        countryList = [rawInput[region][i:i+3] for i in range(0,len(rawInput[region]),3)]
        mappingToCountries[region] =  countryList
    dt.mapp.regions.createNewContext('CAT',mappingToCountries)
    dt.mapp.regions.save()
    
G20List= [dt.mapp.getSpatialID(x) for x in [ 'Argentina',
                          'Australia',
                          'Brazil',
                          'Canada',
                          'China',
                          'India',
                          'Indonesia',
                          'Japan',
                          'Mexico',
                          'Russian Federation',
                          'Saudi Arabia',
                          'South Africa',
                          'Korea, Republic of',
                          'Turkey',
                          'United Kingdom',
                          'United States']] + list(dt.mapp.regions.EU28.membersOf('EU28'))
politicallDict = {'G20': G20List}

CAT_countries = ['ARE', 'ARG', 'AUS', 'BRA', 'BTN', 'CAN', 'CHE', 'CHL', 'CHN', 'CRI', 'ETH', 'EU28', 'GMB', 'IDN', 'IND', 'JPN', 'KAZ', 'KOR', 'MEX', 'MAR', 'NOR', 'NPL', 'NZL', 'PER', 'PHL', 'RUS', 'SAU', 'SGP', 'TUR', 'UKR', 'USA', 'ZAF']
#CAT_countries.remove('EU28')
#CAT_countries = CAT_countries + list(dt.mapp.regions.EU28.membersOf('EU28'))
dt.mapp.regions.createNewContext('political',politicallDict)
dt.mapp.regions.save()

