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
from . import config as conf
from . import mapping as mapp

            
class Datatable(pd.DataFrame):
    
    _metadata = ['meta', 'ID']

    def __init__(self, *args, **kwargs):
        
        metaData = kwargs.pop('meta', {x:None for x in conf.REQUIRED_META_FIELDS})
        super(Datatable, self).__init__(*args, **kwargs)
#        print(metaData)
        if metaData['unit'] is None or pd.isna(metaData['unit']):
            metaData['unit'] = ''
        self.__appendMetaData__(metaData)
        self.vis = Visualization(self)
        try:
            self.generateTableID()
        except:
            self.ID = None


#    def __getattr__(self, key):
#        if key == 'ID' and self.ID is None:
#            self.ID = self.generateTableID()
#        else:
#            return super(Datatable, self).__getitem__(key)
    
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
        #writer.close()
        
        
                
    
    def to_csv(self, fileName=None):
        
        if fileName is None:
            fileName = '|'.join([ self.meta[key] for key in conf.ID_FIELDS]) + '.csv'
        else:
            assert fileName[-4:]  == '.csv'
        
        fid = open(fileName,'w')
        fid.write(conf.META_DECLARATION)
        
        for key, value in sorted(self.meta.items()):
#            if key == 'unit':
#                value = str(value.u)
            fid.write(key + ',' + str(value) + '\n')
        
        fid.write(conf.DATA_DECLARATION)
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
            
            deltaData = Datatable(index=index, columns=t0_years, meta={key:self.meta[key] for key in conf.REQUIRED_META_FIELDS})
            deltaData.meta['entity'] = 'delta_' + deltaData.meta['entity']
            deltaData.loc[:,:] = t1_data - t0_data
        else:
                
            t1_years = self.columns[1:]
            index    = self.index
            t1_data  = self.iloc[:,1:].values
            t0_data  = self.iloc[:,:-1].values
            
            deltaData = Datatable(index=index, columns=t1_years, meta={key:self.meta[key] for key in conf.REQUIRED_META_FIELDS})
            deltaData.meta['entity'] = 'delta_' + deltaData.meta['entity']
            deltaData.loc[:,:] = t1_data - t0_data
        
        
        return deltaData
    
    #%%
    def generateTableID(self):
        metaDict = self.meta
        self.ID =  core._createDatabaseID(metaDict)
        self.meta['ID'] = self.ID
        return self.ID
    
    def source(self):
        return self.meta['source']

    
    
        
    def __add__(self, other):
        if isinstance(other,Datatable):
            
            factor = core.getUnit(other.meta['unit']).to(self.meta['unit']).m
            
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
#    def __repr__(self):
#        outStr = """"""
#        if 'ID' in self.meta.keys():
#            outStr += '=== Datatable - ' + self.meta['ID'] + ' ===\n'
#        else:
#            outStr += '=== Datatable ===\n'
#        for key in self.meta.keys():
#            if self.meta[key] is not None:
#                outStr += key + ': ' + str(self.meta[key]) + ' \n'
#        outStr += super(Datatable, self).__repr__()
#        return outStr
    
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
            kwargs['ax'] = ax[0]
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
    
    assert (fid.readline()) == conf.META_DECLARATION
    #print(nMetaData)
    
    meta = dict()
    while True:
        
        line = fid.readline()
        if line == conf.DATA_DECLARATION:
            break
        dataTuple = line.replace('\n','').split(',')
        meta[dataTuple[0]] = dataTuple[1].strip()

    #print(meta.keys())
    assert conf.REQUIRED_META_FIELDS.issubset(set(meta.keys()))

    df = Datatable(pd.read_csv(fid, index_col=0), meta=meta)
    df.columns = df.columns.map(int)

    fid.close()
    return df
    
#    repo.execute(["git", "add", "test.csv"])
#    repo.execute(["git", "commit", '-m' "first commit"])
#%%
if __name__ == '__main__':
    ## TESTING
    metaDict = {'source' : 'TEST',
                'entity' : 'values',
                'unit' : 'm'}
    metaDict2 = {'source' : 'TEST2',
                'entity' : 'area',
                'unit' : 'km'}    
    
    data = np.asarray([[1,2.2,3,4 ],
                       [2.3, np.nan, 3.4, np.nan],
                       [1.3, np.nan, np.nan, np.nan],
                       [np.nan, 3.4, 2.4, 3.2]])
#                       [np.nan,1900,np.nan,np.nan]])
    
    data2 = np.asarray([[1,2.2,3,4.5 ],
                   [2.3, np.nan, 3.4, np.nan],
                   [1.1, np.nan, np.nan, np.nan],
                   [np.nan, 3.3, 2.4, np.nan]])
    
    
    dt = Datatable(data, meta=metaDict, columns = [2010, 2012, 2013, 2015], index = ['ARG', 'DEU', 'FRA', 'GBR'])
    dt2 = Datatable(data2, meta=metaDict2, columns = [2009, 2012, 2013, 2015], index = ['ARG', 'DEU', 'FRA', 'GBR'])
    dt.vis.availablitity()
    data3 = dt / dt2
    data / data2
