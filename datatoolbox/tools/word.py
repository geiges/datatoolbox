#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 19 09:49:33 2021

@author: ageiges
"""

""" 
Optional toosl for the automatic creation of word documents
"""
import os
import matplotlib.pyplot as plt
import pandas as pd

import docx
from docx import Document
from docx.shared import Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_COLOR_INDEX

import numpy as np
from PIL import ImageColor, Image
from docx.shared import RGBColor
from docx.oxml.ns import nsdecls
from docx.oxml import parse_xml

#%% Defintions
alignments = {
    'left' : WD_ALIGN_PARAGRAPH.LEFT,
    'right' : WD_ALIGN_PARAGRAPH.RIGHT,
    'center' : WD_ALIGN_PARAGRAPH.CENTER,
    'block' : WD_ALIGN_PARAGRAPH.JUSTIFY,
    'justify' : WD_ALIGN_PARAGRAPH.JUSTIFY}
#%% Functions

def create_document():
    doc = Document()
    return doc


def add_heading(doc, string, level):
    doc.add_heading(string,level=level)
    
    
def add_table(pandas_table, 
              heading, 
              document,
              float_format = '{:2.1f}%'):
    
    add_paragraph(document)
    n_col = len(pandas_table.columns)+1
    table = document.add_table(1, n_col)
    cells = table.rows[0].cells
    a, b = cells[0], cells[-1]
    a.merge(b)
    cells[0].text = heading
    
    # populate header row --------
    heading_cells = table.add_row().cells
    for i, col in enumerate(pandas_table.columns):
        heading_cells[i+1].text = str(col)
    # heading_cells[1].0text = 'Oil'
    # heading_cells[2].text = 'Gas'
    # heading_cells[3].text = 'Nuclear'
    # heading_cells[4].text = 'Renewables'
    for ind in pandas_table.index:
        cells = table.add_row().cells
        cells[0].text = str(ind)
        for i, col in enumerate(pandas_table.columns):
            value = pandas_table.loc[ind, col]
            if isinstance(value, float):
                cells[i+1].text = float_format.format(value)
            else:
                cells[i+1].text = str(value)
    table.style = 'Light Grid Accent 1'


def add_to_paragraph(para, 
                     text, 
                     style =None,
                     highlight_color=False):

    if not text.endswith(' '):
        text = text + ' '
        

    run =  para.add_run(text)
    if style == 'italic':
       run.italic = True
    
    elif style == 'bold':
        run.bold = True
        
    if highlight_color==True:
        run.font.highlight_color=WD_COLOR_INDEX.YELLOW
        
    return run

def add_paragraph(document, 
                  scentences = None,
                  alignment='left'):
    """
    

    Parameters
    ----------
    alignment : TYPE, optional
        DESCRIPTION. The default is 'left'.
        [left , right, center, block, justify]

    Returns
    -------
    None.

    """
    
    
    para = document.add_paragraph('')
    
    if isinstance(scentences, str):
        #create iterable
        scentences = [scentences]
        
    if scentences is not None:
        for scentence in scentences:
            if not scentence.endswith(' '):
                scentence = scentence + ' '
            add_to_paragraph(para, scentence)

        
    para.alignment = alignments[alignment]
        
    return para

def add_figure(doc, 
               fig,
               path = None, 
               relative_width = 1., 
               crop= False):
    
    if path is None:
        path = 'temp.png'
    
    fig.savefig('temp.png', dpi=300)
    # sdf
    
    # width = Inches(6.96)
    # height =  Inches(6.96)*ratio
    
    if crop:
        _crop_png('temp.png')
        
    im = Image.open('temp.png')
    width = Inches(5.5) * relative_width
    ratio = im.size[1]/ im.size[0]
    height = width * ratio
    
    doc.add_picture(path, 
                    width,
                    height, 
                    )
    
    plt.close()
     
    os.remove(path)
    
   
def _crop_png(file):
    def bbox(im):
        a = np.array(im)[:,:,:3]  # keep RGB only
        m = np.any(a != [255, 255, 255], axis=2)
        coords = np.argwhere(m)
        y0, x0, y1, x1 = *np.min(coords, axis=0), *np.max(coords, axis=0)
        return (max(0,x0-5), max(0,y0-5), x1, y1)

    im = Image.open(file)
    print(bbox(im))  # (33, 12, 223, 80)
    im2 = im.crop(bbox(im))
    im2.save(file)
   
def set_cell_background(table, 
                        row,
                        col, 
                        color):
    
    if color.startswith('#'):
        color = color[1:]
    shading_elm_1 = parse_xml(r'<w:shd {} w:fill="{}"/>'.format(nsdecls('w'), color))
    table.rows[row].cells[col]._tc.get_or_add_tcPr().append(shading_elm_1)

    return table

def open_word_file(file_name):
    import datatoolbox as dt
    dt.utilities.open_file('test.docx')
    
    

#%% Test
if __name__ == '__main__':
    data = pd.DataFrame(index = [2018, 2017], columns =  ['Coal', 'Oil','Gas','Nuclear','Renewables'])
    data.loc[2018,: ] = [88.9,0.1, 0, 4.5, 6.6]
    data.loc[2017,: ] = [89.9,0.1, 0, 3.5, 6.6]
    #data.loc[2016,: ] = [85.9,0.1, 0, 3.5, 9.6]
        
    
    doc = create_document()
    
    add_heading(doc, 'Test chapter', level=1)
    add_paragraph(doc, 'This document is generated automatically by datatoolbox using the python package docx.')
    table = add_table(data, 'Power generation shares', doc)
    
    fig =plt.figure()
    plt.clf()
    plt.bar(list(range(len(data.columns))),data.loc[2018,:])
    plt.xticks(list(range(len(data.columns))), data.columns)
    
    add_figure(doc, fig, relative_width=.7,crop=True)
    doc.save('test.docx')
    
    open_word_file('test.docx')