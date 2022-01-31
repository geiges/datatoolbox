#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 10:16:36 2019

@author: andreas Geiges
"""

import os 
import re
#import pycountry
import numpy as np
import pandas as pd
from . import config as conf
#from hdx.location.country import Country
#from datatools import core


special_regions = list()   

if not os.path.exists(os.path.join(conf.PATH_TO_DATASHELF, 'mappings')):
    from .admin import create_empty_datashelf
    create_empty_datashelf(conf.PATH_TO_DATASHELF)



class RegionMapping:
    
    def __init__(self, mappingFolder = None):
        
        self.countries = [x for x in conf.COUNTRY_LIST]
        self.contextList   = list()
        self.validIDs  = list()
        #self.validIDs.extend(self.countries)
        
        if mappingFolder is None:
            self.grouping = pd.DataFrame([], index = self.countries)
            print('creating empty grouping table')
            
        else:
            fileList = os.listdir(mappingFolder)
            for file in fileList:
                if file.endswith(".csv"):
                    self.loadContext(mappingFolder, file)
                        
            self.grouping = pd.read_csv(conf.PATH_TO_REGION_FILE, index_col=0)
            for contextCol in self.grouping.columns:
                self.createNewContext(contextCol, self.grouping[contextCol])
            
#    
#
#    def exists(self, regionString):
#        for context in self.contextList:
#            if (self.grouping[context] == (regionString)).any():
#                return True, context
#        #nothing found    
#        return False, None
#
#    def allExist(self, iterable):
#        testSet = set(iterable)           
#        
#        for context in self.contextList:
#            if testSet.issubset(set(self.grouping[context])):
#                return True, context                   
#        #nothing found    
#        return False, None            

    def exists(self, regionString):
        return regionString in self.validIDs
    
    def allExist(self, iterable):
        testSet = set(iterable)
        return testSet.issubset(set(self.validIDs))
    
    def loadContext(self, folderPath, fileName):
        name = fileName.replace('mapping_','').replace('.csv','')
        mappingDataFrame =  pd.read_csv(os.path.join(folderPath, fileName), index_col=0)
        context = RegionContext(name, mappingDataFrame)
        
        # assure no regionID is duplicated
#        for regionID in context.listAll():
#            if regionID in self.valid_IDs:
#                raise(Exception(regionID + 'found as duplicate'))
        #add new regionIDs to valid IDs
        self.validIDs.extend(context.listAll())
        
        self.__setattr__(name, context)
        self.contextList.append(context)
        
    def createNewContext(self, name, mappingDict):
        context = RegionContext.fromDict(name, mappingDict, self.countries)
        self.validIDs.extend(context.listAll())
        self.__setattr__(name, context)
        self.contextList.append(context)

    def addRegionToContext(self, name, mappingDict):
        context = self.__getattribute__(name)
        context.addRegions(mappingDict)
        
        
    def save(self):
        for context in self.contextList:
            
            context.writeToCSV(conf.PATH_TO_MAPPING)    
            
    def listAll(self):
        return self.validIDs            

class RegionContext():
    
    def __init__(self, name, mappingDataFrame):
        self.name = name
        self.groupingDf = mappingDataFrame 
        
        self.keys = self.listAll
        self.__getitem__ = self.membersOf
    
    def __call__(self):
        print('Regions:')
        print()
        
    def regionOf(self, countryID):
        """ 
        Returns the membership in this context
        """
        return self.groupingDf.columns[self.groupingDf.loc[countryID]]
    
    def membersOf(self, regionName):
        return self.groupingDf.index[self.groupingDf[regionName]]
    
    def listAll(self):
        return self.groupingDf.columns
    
    def writeToCSV(self, folderPath):
        self.groupingDf.to_csv(os.path.join(folderPath, 'mapping_' + self.name + '.csv'))

    def addRegions(self, mappingDict):
        for spatialID in mappingDict.keys():
            idList = mappingDict[spatialID]
            idList = [id for id in idList if id in conf.COUNTRY_LIST]
            self.groupingDf.loc[:,spatialID] = False
            self.groupingDf.loc[idList,spatialID] = True
        
    @classmethod        
    def fromDict(cls, name, mappingDict, countries):
        if np.nan in mappingDict.keys():
            del mappingDict[np.nan]
        mappingDataFrame = pd.DataFrame(index=countries)
        
        for spatialID in mappingDict.keys():
            idList = mappingDict[spatialID]
            idList = [id for id in idList if id in countries]
            mappingDataFrame.loc[:,spatialID] = False
            mappingDataFrame.loc[idList,spatialID] = True
            
        return cls(name, mappingDataFrame)       

    def toDict(self):
        mappDict = dict()
        
        for key in self.listAll():
            mappDict[key] = list(self.membersOf(key))
        
        return mappDict
        
class CountryMapping():
    
    def  __init__(self, dataTable = None):
        
        self.countries = [x for x in conf.COUNTRY_LIST]
        self.contextList = list()
        self.nameColumns = list()
        self.numericColumns = list()
        
        if dataTable is None:
            self.codes = pd.DataFrame([], index = self.countries)
            print('creating empty grouping table')
            
        else:
            self.codes = pd.read_csv(conf.PATH_TO_COUNTRY_FILE, index_col=0)
            for contextCol in self.codes.columns:
                self.createNewContext(contextCol, self.codes[contextCol])
                if self.codes[contextCol].dtype == 'object':
                    self.nameColumns.append(contextCol)
                elif self.codes[contextCol].dtype == 'float64':
                    self.numericColumns.append(contextCol)
                
    def createNewContext(self, name, codeSeries):
        self.codes[name] = codeSeries
        self.__setattr__(name, CountryContext(name,self.codes))
        self.contextList.append(name)
        
    def save(self):
        self.codes.to_csv(conf.PATH_TO_COUNTRY_FILE)


                
#    def exists(self, regionString):
#        
#        if (self.codes.index == (regionString)).any():
#            return True, 'alpha3'
#            
#        for context in self.contextList:
#            print(regionString)
#            if (self.codes[context] == (regionString)).any():
#                return True, context
#        #nothing found
#        return False, None
    
    def exists(self, regionString):
        return regionString in self.countries
    
    def allExist(self, iterable):
        testSet = set(iterable)
        return testSet.issubset(set(self.countries))
              
    def listAll(self):
        return self.countries

class CountryContext():
    
    def __init__(self, name, codeSeries):
        self.name = name
        self.codesFromISO = codeSeries[name].reset_index().set_index(name)
        

    def __call__(self, country=None):
        if country is None:
            print(self.codesFromISO)
        else:
            return self.coCode(country)
    
    def coCode(self, country):
        """ 
        Returns the membership in this context
        """
        return self.codesFromISO.loc[country, 'index']
        
regions = RegionMapping(conf.PATH_TO_MAPPING)
countries = CountryMapping(conf.PATH_TO_MAPPING)
#groupings = Groupings()
        
            
def initializeCountriesFromData():
    from hdx.location.country import Country
    

     
    mappingDf = pd.read_csv(conf.MAPPING_FILE_PATH, index_col=0)   
    continentDf = pd.read_csv(conf.CONTINENT_FILE_PATH, index_col=0)
    continentDf = continentDf.set_index('alpha-3')
    
    countryNames = list()
    countryCodes = list()
    for code in mappingDf.index:
#        country = pycountry.countries.get(alpha_3=code)
        country = Country.get_country_info_from_iso3(code)
        if country is not None:
#            countryNames.append(country.name)
#            countryCodes.append(code)
            countryNames.append(country['#country+name+preferred'])
            countryCodes.append(code)
            
    mappingDf['name'] = pd.Series(data=countryNames, index=countryCodes)

    countryCodes = CountryMapping()
    countryCodes.createNewContext('alpha2', continentDf['alpha-2'])
    countryCodes.createNewContext('numISO', continentDf['country-code'])
    countryCodes.createNewContext('name', mappingDf['name'])
    countryCodes.createNewContext('IEA_Name', mappingDf['IEA_country'])
    return countryCodes


def mappingSeries2Dict(mappingSeries):
    outDict = dict()
    for spatialID in mappingSeries.unique():
        outDict[spatialID] = list(mappingSeries.index[(mappingSeries==spatialID)])
        
    return outDict
    
def initializeRegionsFromData():
    #%% SETUP

    if  not os.path.exists(conf.PATH_TO_MAPPING):
        os.makedirs(conf.PATH_TO_MAPPING)
    
 
    mappingDf = pd.read_csv(conf.MAPPING_FILE_PATH, index_col=0)
    continentDf = pd.read_csv(conf.CONTINENT_FILE_PATH, index_col=0)
    continentDf = continentDf.set_index('alpha-3')    
    #CONTINENTS
    
    regions = RegionMapping()
    
    continentDict = mappingSeries2Dict(continentDf['region'])
    continentDict['World'] = list(continentDf.index)
    regions.createNewContext('continent', continentDict)
    regions.createNewContext('AR5', mappingSeries2Dict(mappingDf['ar5']))
    regions.createNewContext('IEA', mappingSeries2Dict(mappingDf['IEA_region_short']))
    regions.createNewContext('IEA_long', mappingSeries2Dict(mappingDf['IEA_region_long']))
    regions.createNewContext('IAM_MESSAGE', mappingSeries2Dict(mappingDf['MESSAGE']))
    regions.createNewContext('EU28', mappingSeries2Dict(mappingDf['eu28']))
    return regions




def getMembersOfRegion(context, regionID):
    
    if context not in regions.contextList:
        raise Exception('Context ' + context + ' not defined')

    if regionID not in regions.__getattribute__(context).listAll():
        raise Exception('RegionID ' + regionID + ' not in context')            
            
    return regions.__getattribute__(context).membersOf(regionID)

def getValidSpatialIDs():
    return regions.validIDs + countries.countries + special_regions
    
def getSpatialID(descriptor, iso_type = 'alpha3'):
    """
    returns the spatial ID (ISO3) for a given desciptor that 
    can be string or in
    """
    if isinstance(descriptor, str) and not descriptor.isdigit():
        #string search
        for codeCol in countries.nameColumns:
            mask = countries.codes[codeCol]==descriptor
            if np.sum(mask) == 1:
                if iso_type =='alpha3':
                    return countries.codes.index[mask][0]
                elif iso_type  in ['alpha2', 'numISO'] :
                    return countries.codes.loc[mask,iso_type][0]
    else:
        #numeric search
        for codeCol in countries.numericColumns:
            mask = countries.codes[codeCol]==descriptor
            if np.sum(mask) == 1:
                if iso_type =='alpha3':
                    return countries.codes.index[mask][0]
                elif iso_type  in ['alpha2', 'numISO'] :
                    return countries.codes.loc[mask,iso_type][0]

def nameOfCountry(coISO):
    try:
        return countries.codes.loc[coISO,'name']
    except:
        return coISO

def add_new_special_regions(regionList):
    """
    This function allows to add special regions to the  list of valid regions 
    to allows for special exceptions if needed. 
    
    If needed, adding this new regions must be done after each new import of 
    datatoolbox since the changes are not permanent.
    """
    for region in regionList:
        if region not in special_regions:
            special_regions.append(region)
            
            
class IPCC_SR15(object):
    def region_mapping(self, 
                        org = 'native', 
                        dest='standard'):
        mapping = {
            'World' : 'World',
            'R5ROWO' : 'R5_ROWO',
            'R5ASIA' : 'R5_ASIA',
            'R5LAM' : 'R5_LAM',
            'R5MAF' : 'R5_MAF',
            'R5OECD90+EU' : 'R5_OECD',
            'R5REF' : 'R5_REF'
            }
        mapping = pd.DataFrame(mapping.items(), columns = ['native', 'standard'])
        return mapping.set_index(org).to_dict()[dest]
  
class ADVANCE(object):
    def region_mapping(self, 
                        org = 'native', 
                        dest='standard'):
        mapping = {'ASIA' :'R5_ASIA', 
                   'BRA' : 'BRA',
                   'CHN' :'CHN',
                   'EU28' : 'EU28', 
                   'IND' : 'IND',
                   'JPN' : 'JPN',
                   'LAM' : 'R5_LAM',
                   'MAF' : 'R5_MAF',
                   'OECD90+EU' : 'R5_OECD',
                   'REF' : 'R5_REF', 
                   'RUS' : 'RUS', 
                   'USA' : 'USA', 
                   'World' :  'World'}
        mapping = pd.DataFrame(mapping.items(), columns = ['native', 'standard'])
        return mapping.set_index(org).to_dict()[dest]
        
class ENGAGE(object):
    def region_mapping(self, 
                        org = 'native', 
                        dest='standard'):
        mapping = {
            'Asia (R5)':  'R5_ASIA',
            'China (R10)': 'R10_China',
            'European Union and Western Europe (R10)' : 'R10_EUROPE',
            'Latin America (R10)' : 'R10_LAM',
            'Latin America (R5)' : 'R5_LAM',
            'Middle East & Africa (R5)' : 'R5_MAF',
            'Middle East (R10)' : 'R10_MIDEAST',
            'North America (R10)' : 'R10_NAMERICA',
            'OECD & EU (R5)' : 'R10_EU_OECD',
            'Other (R10)' : 'R10_OTHER',
            'Other (R5)' :'R5_OTHER',
            'Other Asian countries (R10)' : 'R10_ASIA',
            'Pacific OECD (R10)' : 'R10_PACIFIC_OECD',
            'Reforming Economies (R10)': 'R10_REF',
            'Reforming Economies (R5)' : 'R5_REF',
            'South Asia (R10)' : 'R10_SOUTH_ASIA',
            'Sub-Saharan Africa (R10)' : 'R10_SUB_AFIRICA',
            'World' : 'World'}
        mapping = pd.DataFrame(mapping.items(), columns = ['native', 'standard'])
        return mapping.set_index(org).to_dict()[dest]

class NGFS(object):
     def region_mapping(self, 
                        org = 'native', 
                        dest='standard'):
         mapping= {
             'World' : 'World',
             'OECD90 and EU (and EU candidate) countries' : 'R5_OECD',
             'Countries from the Reforming Ecomonies of the Former Soviet Union' : 'R5_REF',
             'Asian countries except Japan' : 'R5_ASIA',
             'Countries of the Middle East and Africa' : 'R5_MID',
             'Latin American countries' : 'R5_LAM'}
    
         mapping = pd.DataFrame(mapping.items(), columns = ['native', 'standard'])
         return mapping.set_index(org).to_dict()[dest]
    

class CD_LINKS(object):
    
    def region_mapping(self, 
                        org = 'native', 
                        dest='standard'):
        mapping= {
             'World' : 'World',
             'OECD90 and EU (and EU candidate) countries' : 'R5_OECD',
             'Countries from Reforming Ecomonies of the Former Soviet Union' : 'R5_REF',
             'Asian countries except Japan' : 'R5_ASIA',
             'Countries of the Middle East and Africa' : 'R5_MAF',
             'Latin American countries' : 'R5_LAM',
             'Federative Republic of Brazil' : 'BRA',
             "People's Repulic of China" : 'CHN',
             'European Union (28 member countries)': 'EU28',
             'Republic of India' : 'IDN', 
             'State of Japan' : 'JPN',
             'Russian Federation' :'RUS', 
             'United States of America': 'USA'}
        
        mapping = pd.DataFrame(mapping.items(), columns = ['native', 'standard'])
        return mapping.set_index(org).to_dict()[dest]
class IPCC_AR5(object):
    
    def region_mapping(self, 
                        org = 'native', 
                        dest='standard'):
        
        mapping = {'ASIA': 'R5_ASIA',
                   'LAM': 'R5_LAM',
                   'MAF': 'R5_MAF',
                   'OECD90': 'R5_OECD90',
                   'REF': 'R5_REF',
                   'World': 'World'}
        mapping = pd.DataFrame(mapping.items(), columns = ['native', 'standard'])
        return mapping.set_index(org).to_dict()[dest]
    
    def model_mapping(self,
                      org = 'native' , 
                      dest='PIK'):
        mapping = {
            'native' : [
                'GCAM 2.0', 'GCAM 3.0', 'GCAM 3.1', 'IMAGE 2.4', 'IMAGE 2.4 EMF22', 
                'MERGE-ETL_2011', 'MERGE_AME', 'MERGE_EMF22', 'MERGE_EMF27', 'MESSAGE V.1', 
                'MESSAGE V.2', 'MESSAGE V.3', 'MESSAGE V.4', 'REMIND 1.1', 'REMIND 1.2', 
                'REMIND 1.3', 'REMIND 1.4', 'REMIND 1.5', 'WITCH_AME', 'WITCH_AMPERE', 
                'WITCH_EMF22', 'WITCH_EMF27', 'WITCH_LIMITS', 'WITCH_ROSE', 'WITCH_RECIPE'],
            'PIK' : [
                'GC20', 'GC30', 'GC31', 'IMG24', 'IMG24E22', 
                'MRGET11', 'MRGAME', 'MRGE22', 'MRGE27', 'MESV1', 
                'MESV2', 'MESV3', 'MESV4', 'REM11', 'REM12', 
                'REM13', 'REM14', 'REM15', 'WITAME', 'WITAMP',  
                'WITE22', 'WITE27', 'WITLIM', 'WITRSE', 'WITREC']
            }
        mapping = pd.DataFrame.from_dict(mapping)
        return mapping.set_index(org).to_dict()[dest]
    
    def variable_mapping(self,
                         org = 'native' , 
                         dest='PIK'):
    
        mapping = {
            'native' : [
                'Emissions|CO2', 'Emissions|CO2|Fossil Fuels and Industry', 
                'Emissions|CO2|Land Use', 'Emissions|Kyoto Gases',  
                'Emissions|CH4', 'Emissions|F-Gases', 'Emissions|N2O', 
                'Emissions|BC', 'Emissions|CO', 'Emissions|Sulfur', 'Emissions|NOx', 
                'Emissions|OC'
                'Population','GDP|MER', 'Policy Cost|Area under MAC Curve', 
                'Policy Cost|Additional Total Energy System Cost', 'Price|Carbon', 
                'Consumption', 'Policy Cost| Consumption Loss', 'Policy Cost|GDP Loss', 
                'Policy Cost|Other'],
            'PIK': [
                'CO2CAT0', 'CO2CATM0EL','CO2CAT5','KYOTOGHGCAT0','CH4CAT0','FGASESCAT0',
                'N2OCAT0CAT0', 'BCCAT0', 'COCAT0', 'SOXCAT0', 'NOXOCCAT0', 'POPDEMOGR',
                'GDPECO', 'AUMACCECO', 'ATESCOSTECO', 'CPRICEECO', 'CONSECO', 'CONSLOSSECO',
                'GDPLOSSECO','OCOSTSECO']}
        mapping = pd.DataFrame.from_dict(mapping)
        
        return mapping.set_index(org).to_dict()[dest]
        
    def scenario_mapping(self,
                         org = 'native', 
                         dest='PIK'):
        
        mapping = {'native' : ['EMF27-450-Conv', 'EMF27-450-EERE', 'EMF27-450-FullTech',
       'EMF27-450-LimBio', 'EMF27-450-LimSW', 'EMF27-450-LowEI',
       'EMF27-450-NoCCS', 'EMF27-450-NucOff', 'EMF27-550-Conv',
       'EMF27-550-EERE', 'EMF27-550-FullTech', 'EMF27-550-LimBio',
       'EMF27-550-LimSW', 'EMF27-550-LimTech', 'EMF27-550-LowEI',
       'EMF27-550-NoCCS', 'EMF27-550-NucOff', 'EMF27-Base-Conv',
       'EMF27-Base-EERE', 'EMF27-Base-FullTech', 'EMF27-Base-LimBio',
       'EMF27-Base-LimSW', 'EMF27-Base-LimTech', 'EMF27-Base-LowEI',
       'EMF27-Base-NucOff', 'EMF27-FP-EERE', 'EMF27-FP-FullTech',
       'EMF27-G8-EERE', 'EMF27-G8-FullTech', 'LIMITS-450', 'LIMITS-500',
       'LIMITS-Base', 'LIMITS-RefPol', 'LIMITS-RefPol-450',
       'LIMITS-RefPol-450-EE', 'LIMITS-RefPol-450-PC',
       'LIMITS-RefPol-500', 'LIMITS-RefPol2030-500', 'LIMITS-StrPol',
       'LIMITS-StrPol-450', 'LIMITS-StrPol-500', 'AME 2.6 W/m2 OS',
       'AME 3.7 W/m2 NTE', 'AME CO2 price $10 (5% p.a.)',
       'AME CO2 price $30 (5% p.a.)', 'AME CO2 price $50 (5% p.a.)',
       'AME Reference', 'AMPERE2-450-FullTech-HST',
       'AMPERE2-450-FullTech-LST', 'AMPERE2-450-FullTech-OPT',
       'AMPERE2-450-LowEI-HST', 'AMPERE2-450-LowEI-LST',
       'AMPERE2-450-LowEI-OPT', 'AMPERE2-450-NoCCS-HST',
       'AMPERE2-450-NoCCS-LST', 'AMPERE2-450-NoCCS-OPT',
       'AMPERE2-450-NucOff-HST', 'AMPERE2-450-NucOff-LST',
       'AMPERE2-450-NucOff-OPT', 'AMPERE2-550-FullTech-HST',
       'AMPERE2-550-FullTech-LST', 'AMPERE2-550-FullTech-OPT',
       'AMPERE2-Base-FullTech-OPT', 'AMPERE2-Base-LowEI-OPT',
       'AMPERE2-Base-NucOff-OPT', 'AMPERE3-450', 'AMPERE3-450P-EU',
       'AMPERE3-550', 'AMPERE3-Base', 'AMPERE3-CF450', 'AMPERE3-CF550',
       'AMPERE3-RefP-EUback', 'AMPERE3-RefPol', 'Carbon tax 0',
       'Carbon tax 100', 'Carbon tax 150', 'Carbon tax 200',
       'Carbon tax 250', 'Carbon tax 300', 'Carbon tax 350',
       'Carbon tax 400', 'Carbon tax 450', 'Carbon tax 50',
       'Carbon tax 500', 'TER2011', 'AMPERE2-450-Conv-HST',
       'AMPERE2-450-Conv-LST', 'AMPERE2-450-Conv-OPT',
       'AMPERE2-450-EERE-HST', 'AMPERE2-450-EERE-LST',
       'AMPERE2-450-EERE-OPT', 'AMPERE2-450-LimBio-HST',
       'AMPERE2-450-LimBio-LST', 'AMPERE2-450-LimBio-OPT',
       'AMPERE2-450-LimSW-HST', 'AMPERE2-450-LimSW-LST',
       'AMPERE2-450-LimSW-OPT', 'AMPERE2-550-Conv-OPT',
       'AMPERE2-550-EERE-OPT', 'AMPERE2-550-LimBio-OPT',
       'AMPERE2-550-LimSW-OPT', 'AMPERE2-550-LowEI-OPT',
       'AMPERE2-550-NoCCS-OPT', 'AMPERE2-550-NucOff-OPT',
       'AMPERE2-Base-Conv-OPT', 'AMPERE2-Base-EERE-OPT',
       'AMPERE2-Base-LimBio-OPT', 'AMPERE2-Base-LimSW-OPT',
       'AMPERE3-450P-CE', 'AMPERE3-Base-EUback', 'AMPERE3-CF450P-EU',
       'AMPERE3-RefP-CEback', 'ROSE 450 DEF', 'ROSE 450 FS Gr',
       'ROSE 450 HI Coal', 'ROSE 450 HI Fos', 'ROSE 450 HI Gas',
       'ROSE 450 HI Pop', 'ROSE 450 LO Fos', 'ROSE 450 LO Oil',
       'ROSE 450 SL Gr', 'ROSE 550 DEF', 'ROSE 550 FS Gr',
       'ROSE 550 HI Coal', 'ROSE 550 HI Fos', 'ROSE 550 HI Gas',
       'ROSE 550 HI Pop', 'ROSE 550 LO Fos', 'ROSE 550 LO Oil',
       'ROSE 550 SL Gr', 'ROSE BAU DEF', 'ROSE BAU FS Gr',
       'ROSE BAU HI Coal', 'ROSE BAU HI Fos', 'ROSE BAU HI Gas',
       'ROSE BAU HI Pop', 'ROSE BAU LO Fos', 'ROSE BAU LO Oil',
       'ROSE BAU SL Gr', 'ROSE WEAK-2020 DEF', 'ROSE WEAK-2030 DEF',
       'ROSE WEAK-POL DEF', 'Advanced Technology Scenario',
       'Reference Scenario', 'Level1', 'Level2', 'Level3', 'Level4',
       'REF', 'AMPERE3-550P-EU', 'POEM_ScenarioA', 'POEM_ScenarioA_CPH',
       'POEM_ScenarioB_CPH_early', 'POEM_baseline', 'EMF22 2.6 OS BECCS',
       'EMF22 3.7 NTE', 'EMF22 3.7 NTE w Delay', 'EMF22 3.7 OS',
       'EMF22 3.7 OS w Delay', 'EMF22 4.5 NTE', 'EMF22 4.5 NTE w Delay',
       'EMF22 Reference', 'AMPERE3-Base-EU', 'AMPERE3-RefP-CE',
       'AMPERE3-RefP-EU', 'EMF22 3.7 NTE Pessimistic Growth',
       'EMF22 3.7 NTE w Delay Pessimistic Growth',
       'EMF22 3.7 OS Pessimistic Growth',
       'EMF22 3.7 OS w Delay Pessimistic Growth',
       'EMF22 4.5 NTE Pessimistic Growth',
       'EMF22 4.5 NTE w Delay Pessimistic Growth',
       'EMF22 Reference Pessimistic Growth', 'EMF22 2.6 OS',
       'EMF22 2.6 OS No BECCS', 'EMF22 3.7 NTE No BECCS',
       'EMF22 3.7 OS No BECCS', 'EMF22 4.5 NTE No BECCS', 'RCP 8.5',
       'RCP 8.5_MIT_2.6W', 'RCP 8.5_MIT_4.5W', 'RCP 8.5_MIT_6W',
       'GEA Counterfactual', 'GEA Efficiency_450_Illustrative',
       'GEA Efficiency_450_adv.transp_full',
       'GEA Efficiency_450_adv.transp_limbe',
       'GEA Efficiency_450_adv.transp_limbe_limir',
       'GEA Efficiency_450_adv.transp_limir',
       'GEA Efficiency_450_adv.transp_nbecs',
       'GEA Efficiency_450_adv.transp_nbecs_nsink_limbe',
       'GEA Efficiency_450_adv.transp_noccs',
       'GEA Efficiency_450_adv.transp_noccs_nonuc',
       'GEA Efficiency_450_adv.transp_nsink',
       'GEA Efficiency_450_conv.transp_full',
       'GEA Efficiency_450_conv.transp_limbe',
       'GEA Efficiency_450_conv.transp_limbe_limir',
       'GEA Efficiency_450_conv.transp_limir',
       'GEA Efficiency_450_conv.transp_nbecs',
       'GEA Efficiency_450_conv.transp_nbecs_nsink_limbe',
       'GEA Efficiency_450_conv.transp_noccs',
       'GEA Efficiency_450_conv.transp_noccs_nonuc',
       'GEA Efficiency_450_conv.transp_nonuc',
       'GEA Efficiency_450_conv.transp_nsink', 'GEA Mix_450_Illustrative',
       'GEA Mix_450_adv.transp_full', 'GEA Mix_450_adv.transp_limbe',
       'GEA Mix_450_adv.transp_limir', 'GEA Mix_450_adv.transp_nbecs',
       'GEA Mix_450_adv.transp_noccs', 'GEA Mix_450_adv.transp_nonuc',
       'GEA Mix_450_adv.transp_nsink', 'GEA Mix_450_conv.transp_limbe',
       'GEA Mix_450_conv.transp_limir', 'GEA Mix_450_conv.transp_nbecs',
       'GEA Mix_450_conv.transp_nonuc', 'GEA Mix_450_conv.transp_nsink',
       'GEA Supply_450_Illustrative', 'GEA Supply_450_adv.transp_limbe',
       'GEA Supply_450_adv.transp_limir',
       'GEA Supply_450_adv.transp_nbecs',
       'GEA Supply_450_adv.transp_nonuc',
       'GEA Supply_450_adv.transp_nsink',
       'GEA Supply_450_conv.transp_full',
       'GEA Supply_450_conv.transp_nonuc', 'EMF22 2.6 NTE',
       'EMF22 2.6 OS Lo Tech', 'EMF22 2.6 OS w Delay',
       'EMF22 3.7 NTE Lo Tech', 'EMF22 3.7 OS Lo Tech',
       'EMF22 3.7 OS w Delay Lo Tech', 'EMF22 4.5 NTE Lo Tech',
       'EMF22 4.5 NTE w Delay Lo Tech', 'EMF27-450-LimTech', '400ppm',
       '400ppmbiomax', '400ppmbiomin', '400ppmccsmin', '400ppmnonuke',
       '450ppm', '550ppm', '550ppmbiomax', '550ppmbiomin', '550ppmnoccs',
       '550ppmnonuke', '550ppmnorenew', 'baseline', 'baselinebiomax',
       'baselinebiomin', '410CO2os-DEF', '450CO2-EUonly',
       '450CO2-IC+CHN+IND', '450CO2-IConly', '450CO2-delay2020',
       '450CO2-fixBIO', '450CO2-fixNUC', '450CO2-fixRET', '450CO2-noCCS',
       '450CO2-noCCSfixNUC', '450CO2nte-DEF', '450CO2os-DEF', 'Baseline',
       'ROSE 450 FS Gr SL Con', 'ROSE 550 FS Gr SL Con',
       'ROSE BAU FS Gr SL Con', 'ROSE BAU LO Oil HI Gas',
       'ROSE BAU SL Gr SL Con', 'ROSE WEAK-2020 FS Gr',
       'ROSE WEAK-2020 FS Gr SL Con', 'ROSE WEAK-2020 SL Gr',
       'ROSE WEAK-2030 FS Gr', 'ROSE WEAK-2030 FS Gr SL Con',
       'ROSE WEAK-2030 SL Gr', 'ROSE WEAK-POL FS Gr',
       'ROSE WEAK-POL FS Gr SL Con', 'ROSE WEAK-POL SL Gr']}
        
        
        strings_to_replace={
            'Reference': 'REF',
            'AMPERE': 'AMP',
            'FullTech': 'FT',
            'price $': 'P',
            '(5% p.a.)': '',
            'LimBio': 'LB',
            'LimSW': 'LSW',
            'LowEI': 'LEI',
            'NoCCS': 'NCCS',
            'NucOff': 'NOFF',
            'baseline': 'BL',
            'Baseline': 'BL',
            'Base': 'BL',
            'back': 'B',
            'No BECCS': 'NBCS',
            'Pessimistic Growth': 'PG',
            'w Delay': 'WD',
            'Counterfactual': 'CF',
            'Illustrative': 'ILL',
            'adv.transp': 'AT',
            'conv.transp': 'CT',
            'limbe': 'LB',
            'Efficiency': 'EFF',
            'Supply': 'SUP',
            'full': 'F',
            'limir': 'LI',
            'nbecs': 'NBCS',
            'noccs': 'NCCS',
            'nonuc': 'NNUC',
            'nsink': 'NS',
            'LIMITS': 'LIM',
            'RefPol': 'RP',
            'StrPol': 'SP',
            '2020': '20',
            '2030': '30',
            'W/m2': '',
            'Rose': 'RSE',
            'WEAK': 'WK',
            'POL': 'P',
            'LO Oil': 'LO',
            'HI Coal': 'HC',
            'HI Pop': 'HP',
            'HI Fos': 'HF',
            'LO Fos': 'LF',
            'HI Gas': 'HG',
            'POEM': 'PM',
            'Scenario': 'SC'}

        characters_to_remove = ' /()-. +_'
        pattern = "[" + characters_to_remove + "]"
        PIK_scenarios = list()
        for scen in mapping['native']:
            
            for old, new in strings_to_replace.items():
                scen = scen.replace(old, new)
                
            PIK_scenarios.append(re.sub(pattern, "", scen).upper())
            
        mapping['PIK'] = PIK_scenarios
        
        mapping = pd.DataFrame.from_dict(mapping)
         
        return mapping.set_index(org).to_dict()[dest]
        
if __name__ == '__main__':

    #loc = Mapping()        
    regions = initializeRegionsFromData()
    regions.save()
    
    countries = initializeCountriesFromData()
    countries.save()