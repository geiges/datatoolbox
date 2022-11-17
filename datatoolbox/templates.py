#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 11:02:57 2020

@author: ageiges
"""

#%% Datatoolbox


def xls_default_excel_reader_setup():
    """
    Retrun Template for reader setup dict
    """
    print(
        """
        setup = dict()
        setup['filePath']  = 'path/to/template/'
        setup['fileName']  = 'template.xlsx'
        setup['sheetName'] = 'templateSheet0'
        setup['timeIdxList']  = ('B1', 'C5')
        setup['spaceIdxList'] = ('A2', 'A20')
            """
    )


#%% Matplotlib


def plt_fancy_text_box():
    print(
        """
        plt.text(x=.6,y= 0.4
                 , s='text', color='k', 
        bbox={'facecolor':'white', 'alpha':0.8, 'pad':2})
        """
    )


def plt_legend():
    print(
        """
        plt.legend(handles,['first line only'], bbox_to_anchor=(.5,0), loc="lower center", bbox_transform=fig.transFigure, ncol=3, title = 'GHG-Emissions ')
        ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1.2, .5), loc='center right', ncol=1)
        """
    )


#%% OS
def os_list_of_subfolders():

    print(
        """
        **get subfolder from folder**
        folderList = [ name for name in os.listdir(mainFolder) if os.path.isdir(os.path.join(mainFolder, name)) ]
        """
    )


#%% Pandas


def pd_combine_columns():
    print(
        """
          df['combined'] = df[['Col1', 'Col2']].apply(lambda x: '_'.join(x.str.strip()), axis=1)
          """
    )


def pd_rempove_letters():
    print(
        """
          df['Columm_name'].apply(lambda x: re.sub("[^0123456789\.]", "", str(x))) 
          """
    )
