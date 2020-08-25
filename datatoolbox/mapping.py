#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 10:16:36 2019

@author: andreas Geiges
"""

import os 
import re
import numpy as np
import pandas as pd
from dataclasses import dataclass, field

from . import config as conf
#from hdx.location.country import Country
#from datatools import core


special_regions = list()   

if not os.path.exists(os.path.join(conf.PATH_TO_DATASHELF, 'mappings')):
    from .admin import create_empty_datashelf
    create_empty_datashelf(conf.PATH_TO_DATASHELF)




@dataclass
class RegionMapping:
    mapping: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(index=[], columns=[], dtype=str)
    )

    @classmethod
    def fromFile(cls, path=conf.MAPPING_FILE_PATH):
        mapping = pd.read_csv(path, index_col=0)
        return cls(mapping)

    @classmethod
    def fromMappingPath(cls, path=conf.PATH_TO_MAPPING):
        regions = cls()
        for fn in os.listdir(path):
            if fn.startswith("mapping_"):
                try:
                    context = RegionContext.fromMappingFile(os.path.join(path, fn))
                except (ValueError, IndexError):
                    print("Skipping", fn)
                regions.addContext(context)

        return regions

    def save(self, path=conf.MAPPING_FILE_PATH):
        self.mapping.to_csv(path)

    @property
    def countries(self):
        return list(self.mapping.index)

    @property
    def contextList(self):
        return list(self.mapping.columns)

    @property
    def validIDs(self):
        # regions and countries
        return self.countries + list(self.mapping.stack().dropna().unique())

    def listAll(self):
        return self.validIDs            
    
    def exists(self, regionString):
        return (not self.mapping.empty) and (regionString in self.mapping.values)

    def allExist(self, iterable):
        testSet = set(iterable)
        return testSet.issubset(set(self.mapping.values.ravel()))

    def addContext(self, context):
        self.mapping[context.name] = context.map

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as e:
            raise AttributeError(e.args[0])
    
    def __getitem__(self, name):
        return RegionContext(name, self.mapping[name])

    _re_pattern = re.compile('[a-zA-Z_][a-zA-Z0-9_]*')

    def __dir__(self):
        context_keys = []
        for k in self.mapping.columns:
            if isinstance(k, str):
                m = self._re_pattern.match(k)
                if m:
                    context_keys.append(m.string)

        obj_attrs = list(dir(RegionMapping))

        return context_keys + obj_attrs

@dataclass
class RegionContext:
    name: str
    map: pd.Series
    
    @classmethod        
    def fromDict(cls, name, mappingDict, countries=None):
        map = []
        for region, countries in mappingDict.items():
            map.append(pd.Series(region, index=countries))
        map = pd.concat(map).rename(name)
        if countries is not None:
            map = map.reindex(countries)
            
        return cls(name, map)
    
    @classmethod
    def fromMappingFile(cls, path, name=None):
        if name is None:
            m = re.match(r"mapping_(.+)\.csv", os.path.basename(path))
            if m is None:
                raise ValueError(
                    "If filename is not of the form mapping_<name>.csv,"
                    " a valid name must be provided"
                )
            name = m.group(1)
        
        map = pd.read_csv(path, index_col=0)
        if (map.sum(axis=1) > 1).any():
            raise ValueError(
                f"{path} contains countries which are in multiple regions "
                f"(Check for World regions)."
            )

        map = map.rename_axis(columns=name).stack().loc[lambda s: s].reset_index(level=name)[name]

        return cls(name, map)

    def regionOf(self, countryID):
        """ 
        Returns the membership in this context
        """
        return self.map.loc[countryID]
    
    def membersOf(self, regionName):
        return self.map.index[self.map == regionName]
    
    def listAll(self):
        return pd.Index(self.map).unique()


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

# MAPPING_FILE_PATH has been generated from 
regions = RegionMapping.fromFile(conf.MAPPING_FILE_PATH)

# Mostly backwards compatible:
#regions = RegionMapping.fromMappingPath(conf.PATH_TO_MAPPING)
countries = CountryMapping(conf.PATH_TO_MAPPING)
            
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


def getMembersOfRegion(context, regionID):
    return regions[context].membersOf(regionID)

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
    
if __name__ == '__main__':

    #loc = Mapping()        
    regions = RegionMapping.fromMappingPath()
    regions.save()
    
    countries = initializeCountriesFromData()
    countries.save()