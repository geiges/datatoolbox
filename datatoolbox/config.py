#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  1 14:15:56 2019

@author: andreas geiges
"""
import os
import platform
OS = platform.system() #'win32' , linux, #Darwin


if OS == 'Darwin':
    import matplotlib
    matplotlib.use("TkAgg")
#%% Personal setup
if not os.path.isfile(os.path.dirname(__file__) + '/settings/personal.py'):
    from .tools.install_support import create_initial_config
    modulePath =  os.path.dirname(__file__) + '/'
    CRUNCHER, PATH_TO_DATASHELF, DB_READ_ONLY, DEBUG = create_initial_config(modulePath)

else:
    from .settings.personal import CRUNCHER, PATH_TO_DATASHELF, DB_READ_ONLY, DEBUG
  

#%% general setup


REQUIRED_META_FIELDS = {'entity',
                        'scenario',
                        'institution',
                        'year',
                        'unit'}

OPTIONAL_META_FIELDS = ['category',
                       'model']

ID_FIELDS = ['variable',
             'pathway',
             'source']

INVENTORY_FIELDS = ['variable',
                    'entity',
                    'category', 
                    'pathway',
                    'scenario',
                    'model',
                    'source',
                    'institution',
                    'year',
                    'unit'
                    ]


META_DECLARATION = '### META ###\n'
DATA_DECLARATION = '### DATA ###\n'

MODULE_PATH = os.path.dirname(__file__)
MODULE_DATA_PATH =  os.path.join(MODULE_PATH, 'data')

MAPPING_FILE_PATH = os.path.join(MODULE_DATA_PATH, 'region_mappings.csv')
CONTINENT_FILE_PATH = os.path.join(MODULE_DATA_PATH, 'all.csv')
PATH_TO_MAPPING = os.path.join(PATH_TO_DATASHELF, 'mappings')
PATH_TO_COUNTRY_FILE = os.path.join(PATH_TO_MAPPING, 'country_codes.csv')
PATH_TO_REGION_FILE =  os.path.join(PATH_TO_MAPPING, 'regions.csv')
PATH_PINT_DEFINITIONS = os.path.join(MODULE_PATH, 'pint_definitions.txt')

SOURCE_FILE   =  os.path.join(PATH_TO_DATASHELF, 'sources.csv')


SOURCE_META_FIELDS = ['SOURCE_ID',
                      'collected_by',
                      'date',
                      'source_url',
                      'licence',
                      'git_commit_hash']

SOURCE_SUB_FOLDERS = ['tables',
                      'raw_data'
                      ]

GHG_GAS_TABLE_FILE = 'GHG_properties_2019_CA.csv'
GHG_NAMING_FILENAME = 'GHG_alternative_naming.pkl'


COUNTRY_LIST = ['AFG', 'ALA', 'ALB', 'DZA', 'ASM', 'AND', 'AGO', 'AIA', 'ATA', 'ATG', 'ARG', 'ARM', 'ABW', 
                'AUS', 'AUT', 'AZE', 'BHS', 'BHR', 'BGD', 'BRB', 'BLR', 'BEL', 'BLZ', 'BEN', 'BMU', 'BTN', 
                'BOL', 'BIH', 'BES', 'BWA', 'BVT', 'BRA', 'IOT', 'BRN', 'BGR', 'BFA', 'BDI', 'KHM', 'CMR', 
                'CAN', 'CPV', 'CYM', 'CAF', 'TCD', 'CHL', 'CHN', 'CXR', 'CCK', 'COL', 'COM', 'COG', 'COD', 
                'COK', 'CRI', 'CIV', 'HRV', 'CUB', 'CUW', 'CYP', 'CZE', 'DNK', 'DJI', 'DMA', 'DOM', 'ECU', 
                'EGY', 'SLV', 'GNQ', 'ERI', 'EST', 'ETH', 'FLK', 'FRO', 'FJI', 'FIN', 'FRA', 'GUF', 'PYF', 
                'ATF', 'GAB', 'GMB', 'GEO', 'DEU', 'GHA', 'GIB', 'GRC', 'GRL', 'GRD', 'GLP', 'GUM', 'GTM', 
                'GGY', 'GIN', 'GNB', 'GUY', 'HTI', 'HMD', 'VAT', 'HND', 'HKG', 'HUN', 'ISL', 'IND', 'IDN', 
                'IRN', 'IRQ', 'IRL', 'IMN', 'ISR', 'ITA', 'JAM', 'JPN', 'JEY', 'JOR', 'KAZ', 'KEN', 'KIR', 
                'PRK', 'KOR', 'KWT', 'KGZ', 'LAO', 'LVA', 'LBN', 'LSO', 'LBR', 'LBY', 'LIE', 'LTU', 'LUX', 
                'MAC', 'MKD', 'MDG', 'MWI', 'MYS', 'MDV', 'MLI', 'MLT', 'MHL', 'MTQ', 'MRT', 'MUS', 'MYT', 
                'MEX', 'FSM', 'MDA', 'MCO', 'MNG', 'MNE', 'MSR', 'MAR', 'MOZ', 'MMR', 'NAM', 'NRU', 'NPL', 
                'NLD', 'NCL', 'NZL', 'NIC', 'NER', 'NGA', 'NIU', 'NFK', 'NOR', 'OMN', 'PAK', 'PLW', 'PSE', 
                'PAN', 'PNG', 'PRY', 'PER', 'PHL', 'PCN', 'POL', 'PRT', 'PRI', 'QAT', 'REU', 'ROU', 'RUS', 
                'RWA', 'BLM', 'SHN', 'KNA', 'LCA', 'MAF', 'SPM', 'VCT', 'WSM', 'SMR', 'STP', 'SAU', 'SEN', 
                'SRB', 'SYC', 'SLE', 'SGP', 'SXM', 'SVK', 'SVN', 'SLB', 'SOM', 'ZAF', 'SGS', 'ESP', 'LKA', 
                'SSD', 'SDN', 'SUR', 'SJM', 'SWZ', 'SWE', 'CHE', 'SYR', 'TWN', 'TJK', 'TZA', 'THA', 'TLS', 
                'TGO', 'TKL', 'TON', 'TTO', 'TUN', 'TUR', 'TKM', 'TCA', 'TUV', 'UGA', 'UKR', 'ARE', 'GBR', 
                'USA', 'UMI', 'URY', 'UZB', 'VUT', 'VEN', 'VNM', 'VGB', 'WLF', 'ESH', 'YEM', 'ZMB', 'ZWE', 
                'MNP', 'VIR']

logTables = False

DATASHELF_REMOTE  = 'git@gitlab.com:climateanalytics/datashelf/'

#### FUNCTION TESTS ########
if __name__ == '__main__':
    pass