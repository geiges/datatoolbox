#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 09:54:20 2020

@author: ageiges
"""
#%% Import
import pandas as pd

#%%Defintions
_dataFrameStyle = """
<style>
    tr:nth-child(even) {color: blue; }
    tr:nth-child(odd)  {background: #ededed;}
    thead th      { background: #cecccc; }
    tr:hover { background: silver;   cursor: pointer;}
    td, th {padding: 5px;}
    {border: 1px solid silver;}
    {border-collapse: collapse; }
    {font-size: 11pt;}
    {font-family: Arial;}
    {text-align: left;}
</style>
"""
# nice green #d2f4e5

#%% Functions
def _HTML_with_style(df, style=None, random_id=None):
    """
    Generates HTML code based on a dataframe using a given styple format
    """

    from IPython.display import HTML
    import numpy as np
    import re

    df_html = df.to_html(escape=False)

    if random_id is None:
        random_id = 'id%d' % np.random.choice(np.arange(1000000))

    if style is None:
        style = """
        <style>
            table#{random_id} {{color: blue}}
        </style>
        """.format(
            random_id=random_id
        )
    else:
        new_style = []
        s = re.sub(r'</?style>', '', style).strip()
        for line in s.split('\n'):
            line = line.strip()
            if not re.match(r'^table', line):
                line = re.sub(r'^', 'table ', line)
            new_style.append(line)
        new_style = ['<style>'] + new_style + ['</style>']

        style = re.sub(r'table(#\S+)?', 'table#%s' % random_id, '\n'.join(new_style))

    df_html = re.sub(r'<table', r'<table id=%s ' % random_id, df_html)

    #    html_string = '''
    #        <html>
    #          <head><title>HTML Pandas Dataframe with CSS</title></head>
    #          <link rel="stylesheet" type="text/css" href="df_style.css"/>
    #          <body>
    #          <h1>{heading}</h1>
    #            {table}
    #          </body>
    #        </html>.
    #        '''
    # df_html = html_string.format(table=df_html, heading=heading)
    return HTML(style + df_html)


def generate_html(dataframe, style=None):
    """
    Generates basic html representation of a dataframe
    """

    if style is None:
        style = _dataFrameStyle
    pd.set_option('colheader_justify', 'center')  # FOR TABLE <th>

    html = _HTML_with_style(dataframe, '<style>table {}</style>'.format(style))

    return html.data.replace('NaN', '')


def export_to_html(dataframe, fileName=None, heading=''):
    pd.set_option('colheader_justify', 'center')  # FOR TABLE <th>

    html = _HTML_with_style(dataframe, '<style>table {}</style>'.format(DataFrameStyle))

    html_string = '''
            <html>
              <head><title>HTML Pandas Dataframe with CSS</title></head>
              <link rel="stylesheet" type="text/css" href="df_style.css"/>
              <body>
              <h1>{heading}</h1>
                {table}
              </body>
            </html>
            '''
    df_html = html_string.format(table=html.data, heading=heading)

    if fileName:
        with open(fileName, 'w') as f:
            f.write(df_html)

    return df_html


#%% Classes
class HTML_BLOCK_FILE:
    MAIN_STYLE_HEADER = """
                <style type="text/css">
                .main-container {
                  max-width: 940px;
                  margin-left: auto;
                  margin-right: auto;
                  text-align: justify;
                }
                h1 {
                    margin-top: 40px;
                }
                code {
                  color: inherit;
                  background-color: rgba(0, 0, 0, 0.04);
                }
                img {
                  max-width:100%;
                  height: auto;
                }
                .tabbed-pane {
                  padding-top: 12px;
                }
                .html-widget {
                  margin-bottom: 20px;
                }
                button.code-folding-btn:focus {
                  outline: none;
                }
                </style>
                <div class="container-fluid main-container">
                """

    MAIN_STYLE_FOODER = """ </div>"""

    def __init__(self, fileName, title):

        self.fileName = fileName
        self.headerBlocks = list()
        self.bodyBlocks = list()
        self.fooderBlocks = list()
        self.title = title
        self.replaceDict = dict()

        self.headerBlocks.append(self.MAIN_STYLE_HEADER)
        self.fooderBlocks.append(self.MAIN_STYLE_FOODER)

    def _writeFile(self):
        buffer = self._createBuffer()
        buffer = self._doReplacement(buffer)
        with open(self.fileName, 'w') as f:
            f.write(buffer)

    def write(self):
        self._writeFile()

    def addToHeader(self, htmlCode):
        self.headerBlocks.append(htmlCode)

    def addToBody(self, htmlCode):
        self.bodyBlocks.append(htmlCode)

    def addToFooder(self, htmlCode):
        self.fooderBlocks.append(htmlCode)

    def appendHeading(self, heading, tierStr='h1'):
        self.bodyBlocks.append(
            """<{tier}>{heading}</{tier}>""".format(heading=heading, tier=tierStr)
        )

    def addRepacement(self, old, new):
        self.replaceDict[old] = new

    def _doReplacement(self, buffer):
        for key, value in self.replaceDict.items():
            buffer = buffer.replace(key, value)
        return buffer

    def _createBuffer(self):
        #%% header
        buffer = """<html>
                <head><title>{title}</title></head>
                <link rel="stylesheet" type="text/css" href="df_style.css"/>
                """.format(
            title=self.title
        )

        for block in self.headerBlocks:
            buffer += block

        # body
        buffer += """<body>"""

        for block in self.bodyBlocks:
            buffer += block

        buffer += """</body>"""

        # fooder
        for block in self.fooderBlocks:
            buffer += block
        buffer += """</html>"""
        return buffer


class HTML_File:
    def __init__(self, fileName):

        self.fileName = fileName
        self.buffer = """"""

        self.mainStyle_header = """
                <style type="text/css">
                .main-container {
                  max-width: 940px;
                  margin-left: auto;
                  margin-right: auto;
                  text-align: justify;
                }
                
                code {
                  color: inherit;
                  background-color: rgba(0, 0, 0, 0.04);
                }
                img {
                  max-width:100%;
                  height: auto;
                }
                .tabbed-pane {
                  padding-top: 12px;
                }
                .html-widget {
                  margin-bottom: 20px;
                }
                button.code-folding-btn:focus {
                  outline: none;
                }
                </style>
                <div class="container-fluid main-container">
                """

        self.mainStyle_fooder = """ </div>"""

        self._appendHeader()

    def _appendHeader(self):
        self.buffer += (
            """
        <html>
        """
            + self.mainStyle_header
            + """
        <head><title>HTML Pandas Dataframe with CSS</title></head>
          <link rel="stylesheet" type="text/css" href="df_style.css"/>
          <body>
        """
        )

    def _appendFooder(self):
        self.buffer += (
            self.mainStyle_fooder
            + """
        </body>
        </html>.
        """
        )

    def _writeFile(self):
        with open(self.fileName, 'w') as f:
            f.write(self.buffer)

    def appendHeading(self, heading, tierStr='h1'):
        self.buffer += """<{tier}>{heading}</{tier}>""".format(
            heading=heading, tier=tierStr
        )

    def appendTable(self, htmlTable):
        self.buffer += htmlTable

    def appendText(self, htmlText):
        self.buffer += htmlText

    def close(self):
        self._appendFooder()
        self._writeFile()
