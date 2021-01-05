#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Basic data structures that allow more efficient handling of year-country
based data sets. 

Datatables are based on pandas dataframes and enfore regions as index
and years as columns. It uses meta data and provides full unit handling
by any computations using datatables.

"""

import pandas as pd
import matplotlib.pylab as plt
import numpy as np
from copy import copy

from . import core
from . import config 
#from . import mapping as mapp
from . import util
#from . import io_tools
#from .tools import xarray
            
class Datatable(pd.DataFrame):
    """
    Datatable
    ^^^^^^^^^
    
    Datatable is derrived from pandas dataframe. 
    """
    _metadata = ['meta', 'ID']

    def __init__(self, *args, **kwargs):
        
        metaData = kwargs.pop('meta', {x:'' for x in config.REQUIRED_META_FIELDS})

        super(Datatable, self).__init__(*args, **kwargs)
#        print(metaData)
        if metaData['unit'] is None or pd.isna(metaData['unit']):
            metaData['unit'] = ''
#        metaData['variable'] = metaData['entity'] + metaData['category']
        self.__appendMetaData__(metaData)
        self.vis = Visualization(self)
        try:
            self.generateTableID()
        except:
            self.ID = None

    
        if config.AVAILABLE_XARRAY:
            self.to_xarray = self._to_xarray
            
        self.columns.name = 'time'
        self.index.name   = 'region'
    
    @classmethod
    def from_pyam(cls, idf, **kwargs):
        """
        Create a datatable from an iam dataframe.

        Parameters
        ----------
        cls : datatable class
            DESCRIPTION.
        idf : pyam dataframe
            dataframe that contrains the data that is used to create the datatable.
        **kwargs : TYPE
            DESCRIPTION.

        Returns
        -------
        datatatble : datatoolbox datatable
            Datatable with original unit and related meta data.

        """
        import pyam

        if kwargs:
            idf = idf.filter(**kwargs)

        assert len(idf.variables()) == 1, (
            f"Datatables cannot represent more than one variable, "
            f"but there are {', '.join(idf.variables())}"
        )

        def extract_unique_values(df, fields, ignore):
            meta = {}
            for fld in set(fields).difference(ignore):
                values = df[fld].unique()
                assert len(values) == 1, (
                    f"Datatables can only represent unique meta entries, "
                    f"but {fld} has {', '.join(values)}"
                )
                meta[fld] = values[0]
            return meta

        meta = {
            **extract_unique_values(idf.data, pyam.IAMC_IDX, ['region']),
            **extract_unique_values(idf.meta, idf.meta.columns, ['exclude'])
        }

        data = (
            idf.data.pivot_table(index=['region'], columns=idf.time_col)
            .value  # column name
            .rename_axis(columns=None)
        )

        return cls(data, meta=meta)

    @classmethod
    def from_excel(cls, filepath, sheetName = None):
        """
        Create a dataframe from a suitable excel file that is saved by datatoolbox.
        
        Parameters
        ----------
        cls : class
        
        filepath : str
            Path to the file.
        sheetName : str, optional
            Sheetn ame that is read in. The default is None.
     
        Returns
        -------
        datatable
            DESCRIPTION.
        """
        return read_excel(filepath,sheetNames= [sheetName()])
       
    def _to_xarray(self):
        
        return core.xr.DataArray(self.values, coords=[self.index, self.columns], dims=['space','time'], attrs=self.meta)
    

    
    @property
    def _constructor(self):
        return Datatable
    
    def __finalize__(self, other, method=None, **kwargs):
        """propagate metadata from other to self """
        # merge operation: using metadata of the left object
        if method == 'merge':
            for name in self._metadata:
                object.__setattr__(self, name, copy(getattr(other.left, name, None)))
        # concat operation: using metadata of the first object
        elif method == 'concat':
            for name in self._metadata:
                object.__setattr__(self, name, copy(getattr(other.objs[0], name, None)))
        else:
            for name in self._metadata:
                #print(other)
                object.__setattr__(self, name, copy(getattr(other, name, None)))
        return self    
    
   
    def __appendMetaData__(self, metaDict):    
        """
        Private function to append meta data.

        Parameters
        ----------
        metaDict : dict
            New meta data.


        """
        self.__setattr__('meta', metaDict.copy())

        #test if unit is recognized
        #print(self.meta['unit'])
        core.getUnit(self.meta['unit'])


    def copy(self, deep=True):
        """
        Make a copy of this Datatable object
        Parameters
        ----------
        deep : boolean, default True
            Make a deep copy, i.e. also copy data
        Returns
        -------
        copy : Datatable
        """
        # FIXME: this will likely be unnecessary in pandas >= 0.13
        data = self._data
        if deep:
            data = data.copy(deep=True)
        return Datatable(data).__finalize__(self) 

    def diff(self, 
             periods=1, 
             axis=0):
        """
        Compute the difference between different years in the datatable
        Equivalent do pandas diff but return datatable.
        
        Parameters
        ----------
        periods : int, optional
            DESCRIPTION. The default is 1.
        axis : int, optional
            DESCRIPTION. The default is 0.

        Returns
        -------
        out : TYPE
            DESCRIPTION.

        """
        out = super(Datatable, self).diff(periods=periods, axis=axis)
        out.meta['unit'] = self.meta['unit']
        
        return out
    
    def to_excel(self, fileName = None, sheetName = "Sheet0", writer = None, append=False):
        """
        Save datatable to excel.

        Parameters
        ----------
        fileName : str, optional
            Relative file path. If None is provide, a writer is expected. The default is None.
        sheetName : str, optional
            Sheet name that is read in. The default is "Sheet0".
        writer : pandas excel writer, optional
            Pandas writer that is used instead opening a new one. The default is None.
        append : bool, optional
            If true, data is appended to the writer. The default is False.

        Returns
        -------
        None.

        """

        if fileName is not None:
            if append:
                writer = pd.ExcelWriter(fileName, 
                                        engine='openpyxl', mode='a',
                                        datetime_format='mmm d yyyy hh:mm:ss',
                                        date_format='mmmm dd yyyy')  
            else:
                writer = pd.ExcelWriter(fileName,
                                        engine='xlsxwriter',
                                        datetime_format='mmm d yyyy hh:mm:ss',
                                        date_format='mmmm dd yyyy')  
            
        metaSeries= pd.Series(data=[''] + list(self.meta.values()) + [''],
                              index=['###META###'] + list(self.meta.keys()) + ['###DATA###'])
        
        metaSeries.to_excel(writer, sheet_name=sheetName, header=None, columns=None)
        super(Datatable, self).to_excel(writer, sheet_name= sheetName, startrow=len(metaSeries))
        
        if fileName is not None:
            writer.close()
        
        
                
    
    def to_csv(self, fileName=None):
        """
        Save the datatable to an annotated csv file.

        Parameters
        ----------
        fileName : str, optional
            Path to file. The default is None.

        Returns
        -------
        None.

        """
        if fileName is None:
            fileName = '|'.join([ self.meta[key] for key in config.ID_FIELDS]) + '.csv'
        else:
            assert fileName[-4:]  == '.csv'
        
        fid = open(fileName,'w', encoding='utf-8')
        fid.write(config.META_DECLARATION)
        
        for key, value in sorted(self.meta.items()):
#            if key == 'unit':
#                value = str(value.u)
            fid.write(key + ',' + str(value) + '\n')
        
        fid.write(config.DATA_DECLARATION)
        super(Datatable, self).to_csv(fid)
        fid.close()

    def to_pyam(self, **kwargs):
        """
        Conversion to pyam dataframe.

        Parameters
        ----------
        **kwargs : TYPE
            DESCRIPTION.

        Raises
        ------
        AssertionError
            DESCRIPTION.

        Returns
        -------
        idf : pyam dataframe
            DESCRIPTION.

        """
        from pyam import IamDataFrame

        meta = {
            **self.meta,
            **kwargs
        }

        try:
            idf = IamDataFrame(
                pd.DataFrame(self).rename_axis(index="region").reset_index(),
                model=meta.get('model', ''),
                scenario=meta["scenario"],
                variable=meta['variable'],
                unit=meta['unit']
            )
        except KeyError as exc:
            raise AssertionError(f"meta does not contain {exc.args[0]}")

        # Add model, scenario meta fields
        for field in ('pathway', 'source', 'source_name', 'source_year'):
            if field in meta:
                idf.set_meta(meta[field], field)
 
        return idf
       
    def to_IamDataFrame(self, **kwargs):
        """
        Function to sustain backwars compatibility
        Depreciated.

        """
        return self.to_pyam(**kwargs)
        
    def convert(self, newUnit, context=None):
        """
        Convert datatable to different unit and returns converted
        datatable.

        Parameters
        ----------
        newUnit : str
            New unit string in which the datatable should be converted.
        context : str, optional
            Optional context (e.g. GWPAR4). The default is None.

        Returns
        -------
        datatable
            Datatable converted in the new unit.

        """
        if self.meta['unit'] == newUnit:
            return self
        
        dfNew = self.copy()
#        oldUnit = core.getUnit(self.meta['unit'])
#        factor = (1* oldUnit).to(newUnit).m
        
        factor = core.conversionFactor(self.meta['unit'], newUnit, context)
        
        dfNew.loc[:] =self.values * factor
        dfNew.meta['unit'] = newUnit
        dfNew.meta['modified'] = core.getTimeString()
        return dfNew
        
    def aggregate_region(self, mapping):
        """ 
        This functions added the aggregates to the table according to the provided
        mapping.( See datatools.mapp.regions)
        
        Returns the result, but does not inplace add it.
        """
        from datatoolbox.tools.for_datatables import aggregate_region
        return aggregate_region(self, mapping)
    
    def interpolate(self, method="linear", add_missing_years=False):
        """
        Interpoltate missing data between year with the option to add
        missing years in the columns.

        Parameters
        ----------
        method : sting, optional
            Interpolation method. The default is "linear".
            - linear
        add_missing_years : bool, optional
            If true, missing years within the time value range are added to 
            the dataframe. The default is False.

        Returns
        -------
        datatable
            Interpoltated dataframe.

        """
        from datatoolbox.tools.for_datatables import interpolate
        
        if add_missing_years:
            for col in list(range(self.columns.min(),self.columns.max()+1)):
                if col not in self.columns:
                    self.loc[:,col] = np.nan
            self = self.loc[:,list(range(self.columns.min(),self.columns.max()+1))]
        return interpolate(self, method)
    
    def clean(self):
        """
        Clean up the dataframe to only recogniszed regions, years and numeric values. 
        Removed columns and rows with only nan values.

        Returns
        -------
        datatable
            DESCRIPTION.

        """
        return util.cleanDataTable(self)
    
    def filter(self, spaceIDs):
        """
        Filter dataframe based on a list of spatial IDs.

        Parameters
        ----------
        spaceIDs : iterable of str
            DESCRIPTION.

        Returns
        -------
        datatable
            DESCRIPTION.

        """
        mask = self.index.isin(spaceIDs)
        return self.iloc[mask,:]
    
    
    def yearlyChange(self,forward=True):
        """
        This methods returns the yearly change for all years (t1) that reported
        and and where the previous year (t0) is also reported
        
        Parameters
        ----------
        forward : bool 
            If true, the yearly change is computed in the forward direction, otherwise
            backwards.
            Default is forward.
        """

        #%%
        if forward:
            t0_years = self.columns[:-1]
            t1_years = self.columns[1:]
            index    = self.index
            t1_data  = self.iloc[:,1:].values
            t0_data  = self.iloc[:,:-1].values
            
            deltaData = Datatable(index=index, columns=t0_years, meta={key:self.meta[key] for key in config.REQUIRED_META_FIELDS})
            deltaData.meta['entity'] = 'delta_' + deltaData.meta['entity']
            deltaData.loc[:,:] = t1_data - t0_data
        else:
                
            t1_years = self.columns[1:]
            index    = self.index
            t1_data  = self.iloc[:,1:].values
            t0_data  = self.iloc[:,:-1].values
            
            deltaData = Datatable(index=index, columns=t1_years, meta={key:self.meta[key] for key in config.REQUIRED_META_FIELDS})
            deltaData.meta['entity'] = 'delta_' + deltaData.meta['entity']
            deltaData.loc[:,:] = t1_data - t0_data
        
        
        return deltaData
    
    #%%
    def generateTableID(self):
        """
        Generates the table ID based on the meta data of the table.

        Returns
        -------
        datatable
            DESCRIPTION.

        """
        # update meta data required for the ID
        self.meta =  core._update_meta(self.meta)
        self.ID   =  core._createDatabaseID(self.meta)
        self.meta['ID'] = self.ID
        return self.ID


    
    def source(self):
        """
        Return the source of the table 
        """
        return self.meta['source']

    def append(self, other, **kwargs):
        """
        Append data to the datatable
        

        Parameters
        ----------
        other : datatable
            New data that will be added to the datatable.
        **kwargs : TYPE
            Default pandas append arguments.

        Returns
        -------
        datatable

        """
        if isinstance(other,Datatable):
            
            if other.meta['entity'] != self.meta['entity']:
#                print(other.meta['entity'] )
#                print(self.meta['entity'])
                raise(BaseException('Physical entities do not match, please correct'))
            if other.meta['unit'] != self.meta['unit']:
                other = other.convert(self.meta['unit'])
        
        out =  super(Datatable, self).append(other, **kwargs)
        
        # only copy required keys
        out.meta = {key: value for key, value in self.meta.items() if key in config.REQUIRED_META_FIELDS}
        
        # overwrite scenario
        out.meta['scenario'] = 'computed: ' + self.meta['scenario'] + '+' + other.meta['scenario']
        return out

    
        
    def __add__(self, other):
        """
        Private function to add two dataframes. The added table is converted to
        the unit of first table.

        Parameters
        ----------
        other : datatble
            Data to add.

        Returns
        -------
        out : datatable
            DESCRIPTION.

        """
        if isinstance(other,Datatable):
            
            if self.meta['unit'] == other.meta['unit']:
                factor = 1
            else:
                factor = core.getUnit(other.meta['unit']).to(self.meta['unit']).m
            
            rhs = pd.DataFrame(other * factor)
            out = Datatable(super(Datatable, self.copy()).__add__(rhs))

            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        else:
            out = Datatable(super(Datatable, self).__add__(other))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        return out 

    __radd__ = __add__
    
    def __sub__(self, other):
        """
        Private function to subract two dataframes. The subracted table is converted to
        the unit of first table.

        Parameters
        ----------
        other : datatble
            Data to substract.

        Returns
        -------
        out : datatable
            DESCRIPTION.

        """
        if isinstance(other,Datatable):
            if self.meta['unit'] == other.meta['unit']:
                factor = 1
            else:
                factor = core.getUnit(other.meta['unit']).to(self.meta['unit']).m
            rhs = pd.DataFrame(other * factor)
            out = Datatable(super(Datatable, self).__sub__(rhs))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        else:
            out = Datatable(super(Datatable, self).__sub__(other))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        return out
    
    def __rsub__(self, other):
        """
        Equivalent to __sub__
        """
        if isinstance(other,Datatable):
            if self.meta['unit'] == other.meta['unit']:
                factor = 1
            else:
                factor = core.getUnit(other.meta['unit']).to(self.meta['unit']).m
            out = Datatable(super(Datatable, self).__rsub__(other * factor))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        else:
            out = Datatable(super(Datatable, self).__rsub__(other))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        return out
        
    def __mul__(self, other):
        if isinstance(other,Datatable):
            newUnit = (core.getUnit(self.meta['unit']) * core.getUnit(other.meta['unit']))
            out = Datatable(super(Datatable, self).__mul__(other))
            out.meta['unit'] = str(newUnit.u)
            out.meta['source'] = 'calculation'
            out.values[:] *= newUnit.m
        else:
            out = Datatable(super(Datatable, self).__mul__(other))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        return out    
    
    __rmul__ = __mul__

    def __truediv__(self, other):
        if isinstance(other,Datatable):
            newUnit = (core.getUnit(self.meta['unit']) / core.getUnit(other.meta['unit']))
            out = Datatable(super(Datatable, self).__truediv__(other))
            out.meta['unit'] = str(newUnit.u)
            out.meta['source'] = 'calculation'
            out.values[:] *= newUnit.m
        else:
            out = Datatable(super(Datatable, self).__truediv__(other))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        return out

#    __rtruediv__ = __truediv__
    def __rtruediv__(self, other):
        if isinstance(other,Datatable):
            newUnit = (core.getUnit(other.meta['unit']) / core.getUnit(self.meta['unit']))
            out = Datatable(super(Datatable, self).__rtruediv__(other))
            out.meta['unit'] = str(newUnit.u)
            out.meta['source'] = 'calculation'
            out.values[:] *= newUnit.m
        else:
            out = Datatable(super(Datatable, self).__rtruediv__(other))
            out.meta['unit'] = (core.getUnit(self.meta['unit'])**-1).u
            out.meta['source'] = 'calculation'
        return out
    
    def __repr__(self):
        outStr = """"""
        if 'ID' in self.meta.keys():
            outStr += '=== Datatable - ' + self.meta['ID'] + ' ===\n'
        else:
            outStr += '=== Datatable ===\n'
        for key in self.meta.keys():
            if self.meta[key] is not None:
                outStr += key + ': ' + str(self.meta[key]) + ' \n'
        outStr += super(Datatable, self).__repr__()
        return outStr
    
    def __str__(self):
        outStr = """"""
        if 'ID' in self.meta.keys():
            outStr += '=== Datatable - ' + self.meta['ID'] + ' ===\n'
        else:
            outStr += '=== Datatable ===\n'
        for key in self.meta.keys():
            if self.meta[key] is not None:
                outStr += key + ': ' + str(self.meta[key]) + ' \n'
        outStr += super(Datatable, self).__str__()
        return outStr
    
    def _repr_html_(self):
        outStr = """"""
        if 'ID' in self.meta.keys():
            outStr += '=== Datatable - ' + self.meta['ID'] + ' ===<br/>\n'
        else:
            outStr += '=== Datatable ===<br/>\n'
        for key in self.meta.keys():
            if self.meta[key] is not None:
                outStr += key + ': ' + str(self.meta[key]) + ' <br/>\n'
        outStr += super(Datatable, self)._repr_html_()
        return outStr
#%%
class TableSet(dict):
    """
    Class TableSet that is inherited from the dict class. It organized multiple 
    heterogeneous datatbles into one structure.
    """
    def __init__(self, IDList=None):
        """
        Create tableset from a given list of table IDs. All tables are loaded from
        the database.

        Parameters
        ----------
        IDList : list, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """
        super(dict, self).__init__()
        self.inventory = pd.DataFrame(columns = ['key']+ config.INVENTORY_FIELDS)
        
        if IDList is not None:
            for tableID in IDList:
                self.add(core.DB.getTable(tableID))
        
        
    @classmethod
    def from_list(cls,
                  tableList):
        """
        Create tableSet form list of datatables.

        Parameters
        ----------
        cls : TYPE
            DESCRIPTION.
        tableList : list
            List of datatables.

        Returns
        -------
        tableSet : tableset
            DESCRIPTION.

        """
        tableSet = cls()
        for table in tableList:
            tableSet.add(table)
    
        return tableSet
    
            
    def to_xarray(self, dimensions):
        """
        Convert tableset to and xarray with the given dimenions.
        Requires xarray installed

        Parameters
        ----------
        dimensions : list of str
            List of xarray dimensions.

        Returns
        -------
        xr.xarray
            DESCRIPTION.

        """
        if not config.AVAILABLE_XARRAY:
            raise(BaseException('module xarray not available'))
        return core.to_XDataArray(self, dimensions)
       
    def to_xset(self, dimensions = ['region', 'time']):
        """
        Convert table set to an xarray data set.

        Parameters
        ----------
        dimensions : list, optional
            DESCRIPTION. The default is ['region', 'time'].

        Returns
        -------
        xr.Dataset
            DESCRIPTION.

        """
        dimensions = ['region', 'time']
        if not config.AVAILABLE_XARRAY:
            raise(BaseException('module xarray not available'))
        return core.to_XDataSet(self, dimensions)
    
    def to_list(self):
        """
        Convert to list of tables

        Returns
        -------
        list
            List of datatables.

        """
        return [ self[key] for key in self.keys()]
    
    def __iter__(self):
        return iter(self.values())
    
    def add(self, datatables=None, tableID=None):
        """
        Add new tables to table set. Either datatables or table IDs should be given.

        Parameters
        ----------
        datatables : list of datatables, optional
            DESCRIPTION. The default is None.
        tableID : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """
        
        # assert only on parameter is None
        assert not ((datatables is None) and (tableID is None))
        
        if datatables is not None:
            
            if isinstance(datatables, list):
                self._add_list(datatables)
                
            elif isinstance(datatables, TableSet):
                self._add_TableSet(datatables)
                
            elif isinstance(datatables, Datatable):
                self._add_single_table(datatables)
                
            else:
                 print('Data type not recognized.')
            
        elif tableID is not None:
            self._add_tableID(tableID)
            
            
            
    def _add_list(self, tableList):
        for table in tableList:
            tableID = table.generateTableID()
            
            if tableID in self.keys():
                self._update(table, tableID)
            else:
                self.__setitem__(tableID, table)
    
    def _add_TableSet(self, tableSet):
        
        for tableID, table in tableSet.items():
            
            if tableID in self.keys():
                self._update(table, tableID)
            else:
                self.__setitem__(tableID, table)
    
    def _add_single_table(self, table):
        tableID = table.generateTableID()
        if tableID in self.keys():
            self._update(table, tableID)
        else:
            self.__setitem__(tableID, table)
        
    def _add_tableID(self, tableID):
        self[tableID] = None
        self.inventory.loc[tableID] = [None for x in config.ID_FIELDS]   
        
        
    def _update(self, table, tableKey):
        
        # make sure the data is compatible
#        print(table.meta)
#        print(self[tableKey].meta)
        assert table.meta == self[tableKey].meta
        
        # update data
        self[tableKey] = pd.concat([self[tableKey] , table])
            

    def remove(self, tableID):
        """
        Remove table form tableSet.

        Parameters
        ----------
        tableID : str
            TableID.

        Returns
        -------
        None.

        """
        del self[tableID]
        self.inventory.drop(tableID, inplace=True)
        
    
    def filter(self,**kwargs):
        """
        Filter tableSet based on the given table inventory columns. 
        (see config.INVENTORY_FIELDS)

        Parameters
        ----------
        **kwargs : dict-like
            Filter arguments as string that need to be contained in the fields..

        Returns
        -------
        newTableSet : tableSet
            DESCRIPTION.

        """
        inv = self.inventory.copy()
        for key in kwargs.keys():
            #table = table.loc[self.inventory[key] == kwargs[key]]
            mask = self.inventory[key].str.contains(kwargs[key], regex=False)
            mask[pd.isna(mask)] = False
            mask = mask.astype(bool)
            inv = inv.loc[mask].copy()
            
        newTableSet = TableSet()
        for key in inv.index:
            newTableSet[key] = self[key]
            
        return newTableSet

    def aggregate_to_region(self, mapping):
        """ 
        This functions added the aggregates to the output according to the provided
        mapping.( See datatools.mapp.regions)
        
        Returns the result, but does not inplace add it.
        """
        return util.aggregate_tableset_to_region(self, mapping)
        
        
    def __getitem__(self, key):
        item = super(TableSet, self).__getitem__(key)
        
        #load datatable if necessary
        if item is None:
            item = core.DB.getTable(key)
            self[key] = item
        
        return item

    def __setitem__(self, key, datatable):
        super(TableSet, self).__setitem__(key, datatable)
        
        if datatable.ID is None:
            try:
                datatable.generateTableID()
            except:
#                print('Could not generate ID, key used instead')
                datatable.ID = key
        self.inventory.loc[datatable.ID, "key"] = key
        self.inventory.loc[datatable.ID, config.INVENTORY_FIELDS] = [datatable.meta.get(x,None) for x in config.INVENTORY_FIELDS]
    

    
    def to_excel(self, 
                 fileName, 
                 append=False):
        """
        Sace TableSet as excel file with individual datatables in individual sheets.
    
        Parameters
        ----------
        fileName : str
            File path.
        append : bool, optional
            If true, try to append data. The default is False.
    
        Returns
        -------
        None.

        """

        if append:
            writer = pd.ExcelWriter(fileName, 
                                    engine='openpyxl', mode='a',
                                    datetime_format='mmm d yyyy hh:mm:ss',
                                    date_format='mmmm dd yyyy')  
        else:
            writer = pd.ExcelWriter(fileName,
                                    engine='xlsxwriter',
                                    datetime_format='mmm d yyyy hh:mm:ss',
                                    date_format='mmmm dd yyyy')  
        
        for i,eKey in enumerate(self.keys()):
            table = self[eKey].dropna(how='all', axis=1).dropna(how='all', axis=0)
            sheetName = str(i) + table.meta['ID'][:25]
#            print(sheetName)
            table.to_excel(writer=writer, sheetName = sheetName)
            
        writer.close()
        
    def create_country_dataframes(self, countryList=None, timeIdxList= None):
        
        # using first table to get country list
        if countryList is None:
            countryList = self[list(self.keys())[0]].index
        
        coTables = dict()
        
        for country in countryList:
            coTables[country] = pd.DataFrame([], columns= ['entity', 'unit', 'source'] +list(range(1500,2100)))
            
            for eKey in self.keys():
                table = self[eKey]
                if country in table.index:
                    coTables[country].loc[eKey,:] = table.loc[country]
                else:
                    coTables[country].loc[eKey,:] = np.nan
                coTables[country].loc[eKey,'source'] = table.meta['source']
                coTables[country].loc[eKey,'unit'] = table.meta['unit']
                                    
            coTables[country] = coTables[country].dropna(axis=1, how='all')
            
            if timeIdxList is not None:
                
                containedList = [x for x in timeIdxList if x in coTables[country].columns]
                coTables[country] = coTables[country][['source', 'unit'] + containedList]

            
        return coTables

    def variables(self):
        return list(self.inventory.variable.unique())

    def pathways(self):
        return list(self.inventory.pathway.unique())

    def entities(self):
        return list(self.inventory.entity.unique())

    def scenarios(self):
        return list(self.inventory.scenario.unique())
    
    def sources(self):
        return list(self.inventory.source.unique())

    def to_LongTable(self):
        tables = []

        for variable, df in self.items():
            if df.empty:
                continue
            
            try:
                df = df.assign(
                    region=df.index,
                    variable=df.meta['variable'],
                    unit=df.meta['unit'],
                    scenario=df.meta["scenario"],
                    model=df.meta.get("model", "")
                ).reset_index(drop=True)
            except KeyError as exc:
                raise AssertionError(f"meta of {variable} does not contain {exc.args[0]}")
 
            tables.append(df)

        long_df = pd.concat(tables, ignore_index=True, sort=False)
        
        # move id columns to the front
        id_cols = pd.Index(['variable', 'region', 'scenario', 'model', 'unit'])
        long_df = long_df[id_cols.union(long_df.columns)]
        long_df = pd.DataFrame(long_df)
        return long_df

    def to_pyam(self):
        
        import pyam
        long_table = self.to_LongTable()
        long_table.index.name = None
        idf = pyam.IamDataFrame(pd.DataFrame(long_table))

        meta = pd.DataFrame([df.meta for df in self.values()])
        if 'model' not in meta:
            meta['model'] = ""
        if 'scenario' not in meta:
            meta['scenario'] = ""
        meta = (
            meta[
                pd.Index(['model', 'scenario', 'pathway'])
                .append(meta.columns[meta.columns.str.startswith('source')])
            ]
            .set_index(['model', 'scenario'])
            .drop_duplicates()
        )

        idf.meta = meta
        idf.reset_exclude()

        return idf

    # Alias for backwards-compatibility
    to_IamDataFrame = to_pyam

    def plotAvailibility(self, regionList= None, years = None):
        
        avail= 0
        for table in self:
#            print(table.ID)
            table.meta['unit'] = ''
            temp = avail * table
            temp.values[~pd.isnull(temp.values)] = 1
            temp.values[pd.isnull(temp.values)] = 0
            
            avail = avail + temp
        avail = avail / len(self)
        avail = util.cleanDataTable(avail)
        if regionList is not None:
            regionList = avail.index.intersection(regionList)
            avail = avail.loc[regionList,:]
        if years is not None:
            years = avail.columns.intersection(years)
            avail = avail.loc[:,years]
        
        plt.pcolor(avail)
#        plt.clim([0,1])
        plt.colorbar()
        plt.yticks([x +.5 for x in range(len(avail.index))], avail.index)
        plt.xticks([x +.5 for x in range(len(avail.columns))], avail.columns, rotation=45)


class Visualization():
    """ 
    This class addes handy built-in visualizations to datatables
    """
    
    def __init__(self, df):
        self.df = df
    
    def availability(self):
        data = np.isnan(self.df.values)
        availableRegions = self.df.index[~np.isnan(self.df.values).any(axis=1)]
        print(availableRegions)
        plt.pcolormesh(data, cmap ='RdYlGn_r')
        self._formatTimeCol()
        self._formatSpaceCol()
        return availableRegions
        
    def _formatTimeCol(self):
        years = self.df.columns.values
        
        dt = int(len(years) / 10)+1
            
        xTickts = np.array(range(0, len(years), dt))
        plt.xticks(xTickts+.5, years[xTickts], rotation=45)
        print(xTickts)
        
    def _formatSpaceCol(self):
        locations = self.df.index.values
        
        #dt = int(len(locations) / 10)+1
        dt = 1    
        yTickts = np.array(range(0, len(locations), dt))
        plt.yticks(yTickts+.5, locations[yTickts])

    def plot(self, **kwargs):
        
        if 'ax' not in kwargs.keys():
            if 'ID' in self.df.meta.keys():
                fig = plt.figure(self.df.meta['ID'])
            else:
                fig = plt.figure('unkown')
            ax = fig.add_subplot(111)
            kwargs['ax'] = ax
        self.df.T.plot(**kwargs)
        #print(kwargs['ax'])
        #super(Datatable, self.T).plot(ax=ax)
        kwargs['ax'].set_title(self.df.meta['entity'])
        kwargs['ax'].set_ylabel(self.df.meta['unit'])

    def html_line(self, fileName=None, paletteName= "Category20",returnHandle = False):
        from bokeh.io import show
        from bokeh.plotting import figure
        from bokeh.resources import CDN
        from bokeh.models import ColumnDataSource
        from bokeh.embed import file_html
        from bokeh.embed import components
        from bokeh.palettes import all_palettes
        from bokeh.models import Legend
        tools_to_show = 'box_zoom,save,hover,reset'
        plot = figure(plot_height =600, plot_width = 900,
           toolbar_location='above', tools_to_show=tools_to_show,

        # "easy" tooltips in Bokeh 0.13.0 or newer
        tooltips=[("Name","$name"), ("Aux", "@$name")])
        #plot = figure()

        #source = ColumnDataSource(self)
        palette = all_palettes[paletteName][20]
        
        df = pd.DataFrame([],columns = ['year'])
        df['year'] = self.df.columns
        for spatID in self.df.index:
            df.loc[:,spatID] = self.df.loc[spatID].values
            df.loc[:,spatID + '_y'] = self.df.loc[spatID].values
        
        source = ColumnDataSource(df)
        legend_it = list()
        import datatoolbox as dt
        for spatID,color in zip(self.df.index, palette):
#            coName = mapp.countries.codes.name.loc[spatID]
            coName = dt.mapp.nameOfCountry(spatID)
            #plot.line(x=self.columns, y=self.loc[spatID], source=source, name=spatID)
            c = plot.line('year', spatID + '_y', source=source, name=spatID, line_width=2, line_color = color)
            legend_it.append((coName, [c]))
        plot.legend.click_policy='hide'
        legend = Legend(items=legend_it, location=(0, 0))
        legend.click_policy='hide'
        plot.add_layout(legend, 'right') 
        html = file_html(plot, CDN, "my plot")
        
        if returnHandle: 
            return plot
        
        if fileName is None:
            show(plot)
        else:
            with open(fileName, 'w') as f:
                f.write(html)


    def to_map(self, coList=None, year=None):
        #%%
        import matplotlib.pyplot as plt
        import cartopy.io.shapereader as shpreader
        import cartopy.crs as ccrs
        import matplotlib
        
        df = self.df
        if year is None:
            year = self.df.columns[-1]
        if coList is not None:
            
            df = df.loc[coList,year]
        cmap = matplotlib.cm.get_cmap('RdYlGn')

#        rgba = cmap(0.5)
        norm = matplotlib.colors.Normalize(vmin=df.loc[:,year].min(), vmax=df.loc[:,year].max())
        if 'ID' in list(df.meta.keys()):
            fig = plt.figure(figsize=[8,5], num = self.df.ID)
        else:
            fig = plt.figure(figsize=[8,5])
        ax = plt.axes(projection=ccrs.PlateCarree())
#        ax.add_feature(cartopy.feature.OCEAN)
        
        shpfilename = shpreader.natural_earth(resolution='110m',
                                              category='cultural',
                                              name='admin_0_countries')
        reader = shpreader.Reader(shpfilename)
        countries = reader.records()
        
        for country in countries:
            if country.attributes['ISO_A3_EH'] in df.index:
                ax.add_geometries(country.geometry, ccrs.PlateCarree(),
                                  color = cmap(norm(df.loc[country.attributes['ISO_A3_EH'],year])),
                                  label=country.attributes['ISO_A3_EH'],
                                  edgecolor='white'
                                  )
#            else:
#                ax.add_geometries(country.geometry, ccrs.PlateCarree(),
#                                  color = '#405484',
#                                  label=country.attributes['ISO_A3_EH'])
#        plt.title('Countries that accounted for 95% of coal emissions in 2016')
        
        ax2  = fig.add_axes([0.10,0.05,0.85,0.05])
#        norm = matplotlib.colors.Normalize(vmin=0,vmax=2)
        cb1  = matplotlib.colorbar.ColorbarBase(ax2,cmap=cmap,norm=norm,orientation='horizontal')
        cb1.set_label(self.df.meta['unit'])
        plt.title(self.df.meta['entity'])
        plt.show()
#        plt.colorbar()
#%%
    
    def html_scatter(self, fileName=None, paletteName= "Category20", returnHandle = False):
        from bokeh.io import show
        from bokeh.plotting import figure
        from bokeh.resources import CDN
        from bokeh.models import ColumnDataSource
        from bokeh.embed import file_html
        from bokeh.embed import components
        from bokeh.palettes import all_palettes
        from bokeh.models import Legend
        tools_to_show = 'box_zoom,save,hover,reset'
        plot = figure(plot_height =600, plot_width = 900,
           toolbar_location='above', tools=tools_to_show,
    
        # "easy" tooltips in Bokeh 0.13.0 or newer
        tooltips=[("Name","$name"), ("Aux", "@$name")])
        #plot = figure()
    
        #source = ColumnDataSource(self)
        palette = all_palettes[paletteName][20]
        
        df = pd.DataFrame([],columns = ['year'])
        df['year'] = self.df.columns
        
        for spatID in self.df.index:
            df.loc[:,spatID] = self.df.loc[spatID].values
            df.loc[:,spatID + '_y'] = self.df.loc[spatID].values
        
        source = ColumnDataSource(df)
        legend_it = list()
        import datatoolbox as  dt
        for spatID, color in zip(self.df.index, palette):
            coName = dt.mapp.countries.codes.name.loc[spatID]
            #plot.line(x=self.columns, y=self.loc[spatID], source=source, name=spatID)
            c = plot.circle('year', spatID + '_y', source=source, name=spatID, color = color)
            legend_it.append((coName, [c]))

        legend = Legend(items=legend_it, location=(0, 0))
        legend.click_policy='hide'
        plot.add_layout(legend, 'right')
            #p.circle(x, y, size=10, color='red', legend='circle')
        plot.legend.click_policy='hide'
        html = file_html(plot, CDN, "my plot")
        
        if returnHandle: 
            return plot
        if fileName is None:
            show(plot)
        else:
            with open(fileName, 'w') as f:
                f.write(html)
                
def read_csv(fileName):
    
    fid = open(fileName,'r', encoding='utf-8')
    
    assert (fid.readline()) == config.META_DECLARATION
    #print(nMetaData)
    
    meta = dict()
    while True:
        
        line = fid.readline()
        if line == config.DATA_DECLARATION:
            break
        dataTuple = line.replace('\n','').split(',')
        meta[dataTuple[0]] = dataTuple[1].strip()
        if "unit" not in meta.keys():
            meta["unit"] = ""
    df = Datatable(pd.read_csv(fid, index_col=0), meta=meta)
    df.columns = df.columns.map(int)

    fid.close()
    return df

def read_excel(fileName, sheetNames = None):
 
    
    if sheetNames is None:
        xlFile = pd.ExcelFile(fileName)
        sheetNames = xlFile.sheet_names
        xlFile.close()

    if len(sheetNames) > 1:
        out = TableSet()
        for sheet in sheetNames:
            fileContent = pd.read_excel(fileName, sheet_name=sheet, header=None)
            metaDict = dict()
            try:
                for idx in fileContent.index:
                    key, value = fileContent.loc[idx, [0,1]]
                    if key == '###DATA###':
                        break
                    
                    metaDict[key] = value
                columnIdx = idx +1
                dataTable = Datatable(data    = fileContent.loc[columnIdx+1:, 1:].astype(float).values, 
                                      index   = fileContent.loc[columnIdx+1:, 0], 
                                      columns = [int(x) for x in fileContent.loc[columnIdx, 1:]], 
                                      meta    = metaDict)
                dataTable.generateTableID()
                out.add(dataTable)
            except:
                print('Failed to read the sheet: {}'.format(sheet))
        
    else:
        sheet = sheetNames[0]
        fileContent = pd.read_excel(fileName, sheet_name=sheet, header=None)
        metaDict = dict()
        if True:
            for idx in fileContent.index:
                key, value = fileContent.loc[idx, [0,1]]
                if key == '###DATA###':
                    break
                
                metaDict[key] = value
            columnIdx = idx +1
            dataTable = Datatable(data    = fileContent.loc[columnIdx+1:, 1:].astype(float).values, 
                                  index   = fileContent.loc[columnIdx+1:, 0], 
                                  columns = [int(x) for x in fileContent.loc[columnIdx, 1:]], 
                                  meta    = metaDict)
            
            try:
                dataTable.generateTableID()
            except:
                print('Warning: Meta data incomplete, table ID not generated')
            out = dataTable
#        except:
#                print('Failed to read the sheet: {}'.format(sheet))
        
    return out
#%%
class MetaData(dict):
    
    def __init__(self):
        super(MetaData, self).__init__()
        self.update({x : '' for x in config.REQUIRED_META_FIELDS})
    
    
    def __setitem__(self, key, value):
        super(MetaData, self).__setitem__(key, value)
        super(MetaData, self).__setitem__('variable', '|'.join([self[key] for key in ['entity', 'category'] if key in self.keys()]))
        super(MetaData, self).__setitem__('pathway', '|'.join([self[key] for key in ['scenario', 'model'] if key in self.keys()]))
        super(MetaData, self).__setitem__('source', '_'.join([self[key] for key in ['institution', 'year'] if key in self.keys()]))
        
        
if __name__ == '__main__':
    meta = MetaData()
    meta['entity'] = 'Emissions|CO2'
    meta['institution'] = 'WDI'
    meta['year']  = '2020'
    #print(meta)
