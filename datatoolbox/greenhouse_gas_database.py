#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 31 10:28:44 2019

@author: and
"""
import re
import os
import pandas as pd
from fuzzywuzzy import process
import pickle

from . import config

DATA_COLUMNS = ['formula', 
                'GWP100_AR4', 
                'GWP100_AR5', 
                'GWP100', 
                'lifetime', 
                'MAGICC6_varname',
                'MAGICC6_unit',
                'MAGICC7_varname', 
                'description','source']

ID_CONDITION_FUNC = lambda x : re.sub(r"([_-])", "", x).lower()


class GreenhouseGasTable(object):
    
    
    def __init__(self):
        
        self.data = pd.read_csv(os.path.join(config.MODULE_DATA_PATH, config.GHG_GAS_TABLE_FILE), index_col=0)
        self.idData = GasDetector()
            
            
        #self._processNameColumn()
        
#    def _processNameColumn(self):
#        stringConditioningFunc = lambda x : re.sub(r"([_-])", "", x).lower()
#        
#        self.data['condName'] = self.data['name'].apply(stringConditioningFunc)
    def __str__(self):
        return self.data.__str__()
    
    def addEntry(self, dataDict):
        id_ = dataDict.pop('id')
        index = self.findEntryIdx(id_)
        print(index)
        if index is None:
            #print(id_)
            self.data.loc[id_] = None
            self.data.loc[id_] = dataDict
            
            submitDict = {ID_CONDITION_FUNC(id_): id_}
            if 'formula' in dataDict:
                submitDict[dataDict['formula']] = id_
            print(submitDict)    
            self.idData.addGas(submitDict)
            
            return id_
        else:
            print(id_ + ' found in database')
            print('found Entry: ' + str(self.getEntry(index)))
            #print(dataDict)
        
            return index
        
    def save(self):
        self.data.to_csv(config.MODULE_DATA_PATH + config.GHG_GAS_TABLE_FILE)
        self.idData.save()
        
#    def findComponent(self, searchString):
#        
#        res, score          = process.extract(searchString, self.data.baseComponent.values, limit=1)[0]
#        #res_alt, score_alt  = process.extract(searchString).lower(), self.data.condName.values, limit=1)[0]
#        if score > 95:
#            idx = np.where(self.data.baseComponent==res)[0][0]
#            return self.data.baseComponent[idx]
#        else :
#            #return None
#        
#            mask = np.asarray([strData in searchString for strData in self.data.baseComponent])
#            
#            candidates = self.data.baseComponent[mask]
#            #print(candidates)
#            #print(len(self.data.baseComponent[mask]))
#            if len(candidates) == 0:
#                
#                return ''
#            else:
#                return(candidates.values[candidates.map(lambda x: len(x)).values.argmax()])
    def searchGHG(self, searchStr):
        id_ = self.idData.findGas(searchStr)
        if id_ is None:
            return None
        return self.getEntry(id_)
    
    def getEntry(self, id_):
        try:
            return self.data.loc[id_]
        except:
            assumedID = self.idData.findGas(id_)
            print('Inferring ' + assumedID + ' from ' + id_)
            return self.data.loc[assumedID]

    def getEntries(self, idList):
        
        ids = [self.findEntryIdx(id_) for id_ in idList]
        return self.data.loc[ids]

    def findEntryIdx(self, searchString):
        
        return self.idData.findGas(searchString)
        
        
    def updateEntry(self, index, dataDict, overwrite = False):
        """
        Adding data to the table. If overwrite is true  existing data
        will be overwritten
        """
        
        index = self.findEntryIdx(index)
        for key in dataDict.keys():
            if overwrite or pd.isna(self.data.loc[index][key]):
                self.data.loc[index][key] = dataDict[key]
            


class GasDetector(object):
    FILENAME = 'GHG_alternative_naming.pkl'
    def __init__(self, clearData = True):
        
#        if not(os.path.isfile(DATA_STORAGE_LOCATION + self.FILENAME)) or clearData:
#            print(DATA_STORAGE_LOCATION + self.FILENAME + 'not  found - init empty id data dict')
#            self.data = dict()
#        else:
        fid = open(os.path.join(config.MODULE_DATA_PATH, config.GHG_NAMING_FILENAME),'rb')
        self.data = pickle.load(fid)
        fid.close()
            
    def findGas(self, string):
        """
        Returns the main identifier from other alternative names
        """
        if len(self.data) == 0:
            return None
        
        try:
            # direct id match
            return self.data[ID_CONDITION_FUNC(string)]

        except:
            key, score = process.extract(ID_CONDITION_FUNC(string), list(self.data.keys()), limit=1)[0]

            if score > 95:
                return self.data[key]
            else :
                return None
        

    def addGas(self, newDataDict):
        
        for key in newDataDict.keys():
            self.data[ID_CONDITION_FUNC(key)] = newDataDict[key]
        
        
    def save(self):
        fid = open(config.MODULE_DATA_PATH + self.FILENAME,'wb')
        pickle.dump(self.data, fid)
        fid.close()
        
if __name__ == '__main__':

    table = GreenhouseGasTable()
    
    testData = {'id': 'Carbon Dioxide', 'formula': 'CO2', 'GWP100_AR5' : 1}
    table.addEntry(testData)
    print(table)
    testData = {'id': 'CO2', 'formula': 'CO2', 'GWP100_AR5' : 2}
    table.addEntry(testData)
    
    #adding missing names
#    eqwConf.ar4_gwp['FossilCO2'] = eqwConf.ar4_gwp['CO2']
#    eqwConf.ar4_gwp['OtherCO2'] = eqwConf.ar4_gwp['CO2']
#    eqwConf.ar4_gwp['NMVOC'] = eqwConf.ar4_gwp['VOC']
#    
#    # adding nessessary names
#    eqwConf.ar5_gwp['FossilCO2'] = eqwConf.ar5_gwp['CO2']
#    eqwConf.ar5_gwp['OtherCO2']  = eqwConf.ar5_gwp['CO2']
#    eqwConf.ar5_gwp['NMVOC']     = eqwConf.ar5_gwp['VOC']
#    eqwConf.ar5_gwp['HFC143a']   = eqwConf.ar5_gwp['HFC143A']
#    eqwConf.ar5_gwp['HFC227ea']  = eqwConf.ar5_gwp['HFC227EA']
#    eqwConf.ar5_gwp['HFC245fa']  = eqwConf.ar5_gwp['HFC245FA']
#    eqwConf.ar5_gwp['HFC134a']   = eqwConf.ar5_gwp['HFC134A']
#    eqwConf.ar5_gwp['HFC43-10']  = 1650 # IIASA HFC excel sheet
#    
#    
#    
#    for gasMeta in MAGICC6_ENTITIES:
#        ar4_gwp = eqwConf.ar4_gwp[gasMeta[1]]
#        ar5_gwp = eqwConf.ar5_gwp[gasMeta[1]]
#        table.addEntry({'name': gasMeta[0],
#                   'baseComponent': gasMeta[1],
#                   'baseUnit': gasMeta[2],
#                   'GWP100_AR4': ar4_gwp,
#                   'GWP100_AR5': ar5_gwp, 
#                   'MAGICC6': True})
#    table._processNameColumn()
#    print(table.data)
#    x = table.findComponent('FossilCO2')
#    
#    table.findEntry('HFC-143')
#    table.save()
#    