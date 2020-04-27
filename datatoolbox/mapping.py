#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 10:16:36 2019

@author: andreas Geiges
"""

import os 
#import pycountry
import numpy as np
import pandas as pd
from . import config as conf
#from hdx.location.country import Country
#from datatools import core
   
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
        self.groupingDf.to_csv(folderPath + 'mapping_' + self.name + '.csv')

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
    return regions.validIDs + countries.countries
    
def getSpatialID(descriptor):
    """
    returns the spatial ID (ISO3) for a given desciptor that 
    can be string or in
    """
    if isinstance(descriptor, str) and not descriptor.isdigit():
        #string search
        for codeCol in countries.nameColumns:
            mask = countries.codes[codeCol]==descriptor
            if np.sum(mask) == 1:
                return countries.codes.index[mask][0]
    else:
        #numeric search
        for codeCol in countries.numericColumns:
            mask = countries.codes[codeCol]==descriptor
            if np.sum(mask) == 1:
                return countries.codes.index[mask][0]

def nameOfCountry(coISO):
    try:
        return countries.codes.loc[coISO,'name']
    except:
        return coISO
    
if __name__ == '__main__':

    #loc = Mapping()        
    regions = initializeRegionsFromData()
    regions.save()
    
    countries = initializeCountriesFromData()
    countries.save()