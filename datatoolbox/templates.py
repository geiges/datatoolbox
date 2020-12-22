#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 11:02:57 2020

@author: ageiges
"""

#%% Datatoolbox 

def default_excel_reader_setup():
    """ 
    Retrun Template for reader setup dict
    """
    print("""
        setup = dict()
        setup['filePath']  = 'path/to/template/'
        setup['fileName']  = 'template.xlsx'
        setup['sheetName'] = 'templateSheet0'
        setup['timeIdxList']  = ('B1', 'C5')
        setup['spaceIdxList'] = ('A2', 'A20')
            """) 
    

#%% Matplotlib



