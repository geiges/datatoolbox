#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 10:46:40 2019

@author: andreas geiges
"""

import pandas as pd
import matplotlib.pylab as plt
import numpy as np
from copy import copy

from . import core
from . import config 
from . import mapping as mapp
from . import util
#from . import io_tools
            
class Datatable(pd.DataFrame):
    
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

        #compatibililty with v1
#        if not 'model' in self.meta.keys():
#            self.meta['model'] = ''
    
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
        
        self.__setattr__('meta', metaDict.copy())

        #test if unit is recognized
        #print(self.meta['unit'])
        core.getUnit(self.meta['unit'])


    def copy(self, deep=True):
        """
        Make a copy of this ClimateFrame object
        Parameters
        ----------
        deep : boolean, default True
            Make a deep copy, i.e. also copy data
        Returns
        -------
        copy : ClimateFrame
        """
        # FIXME: this will likely be unnecessary in pandas >= 0.13
        data = self._data
        if deep:
            data = data.copy(deep=True)
        return Datatable(data).__finalize__(self) 

    
#with pd.ExcelWriter('the_file.xlsx', engine='openpyxl', mode='a') as writer: 
#     data_filtered.to_excel(writer) 

    def to_excel(self, fileName = None, sheetName = "Sheet0", writer = None, append=False):
        if writer is None:
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
        writer.close()
        
        
                
    
    def to_csv(self, fileName=None):
        
        if fileName is None:
            fileName = '|'.join([ self.meta[key] for key in config.ID_FIELDS]) + '.csv'
        else:
            assert fileName[-4:]  == '.csv'
        
        fid = open(fileName,'w')
        fid.write(config.META_DECLARATION)
        
        for key, value in sorted(self.meta.items()):
#            if key == 'unit':
#                value = str(value.u)
            fid.write(key + ',' + str(value) + '\n')
        
        fid.write(config.DATA_DECLARATION)
        super(Datatable, self).to_csv(fid)
        fid.close()
        
    def convert(self, newUnit, context=None):
        
        dfNew = self.copy()
#        oldUnit = core.getUnit(self.meta['unit'])
#        factor = (1* oldUnit).to(newUnit).m

        factor = core.conversionFactor(self.meta['unit'], newUnit, context)
        
        dfNew.loc[:] =self.values * factor
        dfNew.meta['unit'] = newUnit
        dfNew.meta['modified'] = core.getTimeString()
        return dfNew
        
    
    
    def interpolate(self, method, newSource):
        
        if method == 'linear':
            from scipy import interpolate
            import numpy as np
            xData = self.columns.values.astype(float)
            yData = self.values
            
            for row in yData:
                idxNan = np.isnan(row)
                interpolator = interpolate.interp1d(xData[~idxNan], row[~idxNan], kind='linear')
                row[idxNan] = interpolator(xData[idxNan])
        else:
            raise(NotImplementedError())
            
        dfNew = self.copy()
        dfNew.loc[:] = yData
        
        dfNew.meta['modified'] = core.getTimeString()
        dfNew.meta['source'] = newSource
        return dfNew

#    def filter(self, context, regionID):
#        """ 
#        valid filter aguments are:
#            "region"
#        """
#        members = mapp.getMembersOfRegion(context, regionID)
#        mask = self.index.isin(members)
#
#        return self.iloc[mask,:]
    
    def clean(self):

        return util.cleanDataTable(self)
    
    def filter(self, spaceIDs=None):
        mask = self.index.isin(spaceIDs)
        return self.iloc[mask,:]
    
    
    def yearlyChange(self,forward=True):
        """
        This methods returns the yearly change for all years (t1) that reported
        and and where the previous year (t0) is also reported
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
        # update meta data required for the ID
        self.ID =  core._createDatabaseID(self.meta)
        self.meta['ID'] = self.ID
        return self.ID
    
    def source(self):
        return self.meta['source']

    def append(self, other, **kwargs):

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
#    def append(self, other, **kwargs):
#        if isinstance(other,Datatable):
#            
#            if other.meta['entity'] != self.meta['entity']:
#                raise(BaseException('Physical entities do not match, please correct'))
#            if other.meta['unit'] != self.meta['unit']:
#                other = other.convert(self.meta['unit'])
#            
#            
#        
#        super(Datatable, self).append(self, other, **kwargs)
    
        
    def __add__(self, other):
        #print('other in' + str(other))
        if isinstance(other,Datatable):
            
            factor = core.getUnit(other.meta['unit']).to(self.meta['unit']).m
            #print('factor: ' + str(factor))
            #print('other' + str(other))
            #print(other * factor)
            out = Datatable(super(Datatable, self).__add__(other * factor))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        else:
            
            out = Datatable(super(Datatable, self).__add__(other))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        return out 

    __radd__ = __add__
    
    def __sub__(self, other):
        if isinstance(other,Datatable):
            factor = core.getUnit(other.meta['unit']).to(self.meta['unit']).m
            out = Datatable(super(Datatable, self).__sub__(other * factor))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        else:
            out = Datatable(super(Datatable, self).__sub__(other))
            out.meta['unit'] = self.meta['unit']
            out.meta['source'] = 'calculation'
        return out

    def __rsub__(self, other):
        if isinstance(other,Datatable):
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
            outStr += '=== Datatable - ' + self.meta['ID'] + ' ===\n'
        else:
            outStr += '=== Datatable ===\n'
        for key in self.meta.keys():
            if self.meta[key] is not None:
                outStr += key + ': ' + str(self.meta[key]) + ' \n'
        outStr += super(Datatable, self)._repr_html_()
        return outStr

class TableSet(dict):
    def __init__(self, IDList=None):
        super(dict, self).__init__()
        self.inventory = pd.DataFrame(columns = config.INVENTORY_FIELDS)
        
        if IDList is not None:
            for tableID in IDList:
                self.add(core.DB.getTable(tableID))
    
    def __iter__(self):
        return iter(self.values())
    
    def add(self, datatables=None, tableID=None):
        if isinstance(datatables, (list, TableSet)):
            for datatable in datatables:
                self._add(datatable, tableID)
        else:
            datatable = datatables
            self._add(datatable, tableID)
            
    def _add(self, datatable=None, tableID=None):
        if datatable is None:
            # adding only the ID, the full table is only loaded when necessary
            self[tableID] = None
            self.inventory.loc[tableID] = [None for x in config.ID_FIELDS]
        else:
            # loading the full table
            if datatable.ID is None:
                datatable.generateTableID()
            self[datatable.ID] = datatable
            self.inventory.loc[datatable.ID] = [datatable.meta.get(x,None) for x in config.INVENTORY_FIELDS]
    
    def remove(self, tableID):
        del self[tableID]
        self.inventory.drop(tableID, inplace=True)
        
    
    def filter(self, ):
        pass
    
    def __getitem__(self, key):
        item = super(TableSet, self).__getitem__(key)
        
        #load datatable if necessary
        if item is None:
            item = core.DB.getTable(key)
            self[key] = item
        
        return item
    
    def to_excel(self, fileName):
        writer = pd.ExcelWriter(fileName, 
            engine='openpyxl', 
            mode='a',
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

    def entities(self):
        return list(self.inventory.entity.unique())

    def scenarios(self):
        return list(self.inventory.scenario.unique())
    
    def sources(self):
        return list(self.inventory.source.unique())

    def to_LongTable(self):
        import copy
        #self = dt.getTables(res.index)
        tableList= list()
        minMax = (-np.inf, np.inf)
        for key in self.keys():
            table= copy.copy(self[key])
            #print(table.columns)
            
            oldColumns = list(table.columns)
            minMax = (max(minMax[0], min(oldColumns)), min(minMax[1],max(oldColumns)))
#            for field in config.ID_FIELDS:
#                table.loc[:,field] = table.meta[field]
            
            table.loc[:,'region'] = table.index
            table.loc[:,'unit']   = table.meta['unit']
            table.loc[:,'variable']   = table.meta['entity']
            
            scenModel = table.meta['scenario'].split('|')
            if len(scenModel) == 2:
                table.loc[:,'model'] = scenModel[1]
                table.loc[:,'scenario']   =scenModel[0]
            else:
                table.loc[:,'scenario']   = table.meta['scenario']
                table.loc[:,'model']      = ''
                
            tableNew = table.loc[:, ['variable', 'region', 'scenario',  'model', 'unit'] +  oldColumns]
            tableNew.index= range(len(tableNew.index))
            tableList.append(tableNew)
        #df = pd.DataFrame(columns = ['variable','region', 'model', 'unit'] + list(range(minMax[0], minMax[1])))
        
#        iaDf = pyam.IamDataFrame(tableList[0])
#        for table in tableList[1:]:
#            iaDf.append(table)
#        return iaDf
        fullDf = pd.DataFrame(tableList[0])
        for table in tableList[1:]:
            #print(table)
            fullDf = fullDf.append(pd.DataFrame(table))
        fullDf.index = range(len(fullDf))
        
        columns = list(fullDf.columns)
        [columns.remove(x) for x in ['variable', 'region', 'scenario',  'model', 'unit']]
        columns = ['variable', 'region', 'scenario',  'model', 'unit'] + columns 
        return fullDf.loc[:, columns]
    
    def to_IamDataFrame(self):
        #%%
        import pyam
        import copy
        #self = dt.getTables(res.index)
        tableList= list()
        minMax = (-np.inf, np.inf)
        for key in self.keys():
            table= copy.copy(self[key])
            #print(table.columns)
            
            oldColumns = list(table.columns)
            minMax = (max(minMax[0], min(oldColumns)), min(minMax[1],max(oldColumns)))
#            for field in config.ID_FIELDS:
#                table.loc[:,field] = table.meta[field]
            
            table.loc[:,'region'] = table.index
            table.loc[:,'unit']   = table.meta['unit']
            table.loc[:,'variable']   = table.meta['entity']
            
            scenModel = table.meta['scenario'].split('|')
            if len(scenModel) == 2:
                table.loc[:,'model'] = scenModel[1]
                table.loc[:,'scenario']   =scenModel[0]
            else:
                table.loc[:,'model'] = table.meta['scenario']
                table.loc[:,'scenario']   = ''
            tableNew = table.loc[:, ['variable','region', 'scenario',  'model', 'unit'] +  oldColumns]
            tableNew.index= range(len(tableNew.index))
            tableList.append(tableNew)
        #df = pd.DataFrame(columns = ['variable','region', 'model', 'unit'] + list(range(minMax[0], minMax[1])))
        
#        iaDf = pyam.IamDataFrame(tableList[0])
#        for table in tableList[1:]:
#            iaDf.append(table)
#        return iaDf
        iaDf = pyam.IamDataFrame(pd.DataFrame(tableList[0]))
        for table in tableList[1:]:
#            print(table)
            iaDf = iaDf.append(pd.DataFrame(table))
        return iaDf         

    def plotAvailibility(self, regionList= None, years = None):
        #%%
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
        #%%
    
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
        for spatID,color in zip(self.df.index, palette):
            coName = mapp.countries.codes.name.loc[spatID]
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
        for spatID, color in zip(self.df.index, palette):
            coName = mapp.countries.codes.name.loc[spatID]
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
    
    fid = open(fileName,'r')
    
    assert (fid.readline()) == config.META_DECLARATION
    #print(nMetaData)
    
    meta = dict()
    while True:
        
        line = fid.readline()
        if line == config.DATA_DECLARATION:
            break
        dataTuple = line.replace('\n','').split(',')
        meta[dataTuple[0]] = dataTuple[1].strip()

    df = Datatable(pd.read_csv(fid, index_col=0), meta=meta)
    df.columns = df.columns.map(int)

    fid.close()
    return df

def read_excel(fileName, sheetNames = None):
 
    out = TableSet()
    if sheetNames is None:
        xlFile = pd.ExcelFile(fileName)
        sheetNames = xlFile.sheet_names
        xlFile.close()

        
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