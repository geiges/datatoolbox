#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 09:42:44 2019

@author: ageiges
"""

####### SETS 
import pandas as pd
from . import config

class Regions():
    
    def __init__(self):
        # 2020
        self.CAT_COUNTRIES_2020 = set(['ARE', 'ARG', 'AUS', 'BRA', 'BTN', 'CAN', 'CHE', 'CHL', 'CHN', 'CRI',
                                   'EU28','ETH', 'GMB', 'IDN', 'IND', 'JPN', 'KAZ', 'KOR', 'MAR', 'MEX', 'NOR',
                                   'NPL', 'NZL', 'PER', 'PHL', 'RUS', 'SAU', 'SGP', 'TUR', 'UKR', 'USA',
                                   'ZAF'])
        
        self.CAT_COUNTRIES = set(['ARE', 'ARG', 'AUS', 'BRA', 'BTN', 'CAN', 'CHE', 'CHL', 'CHN', 'COL', 'CRI', 'DEU',
                                  'EU27','ETH', 'GBR', 'GMB', 'IDN', 'IND', 'IRN', 'JPN', 'KAZ', 'KEN', 'KOR', 'MAR', 
                                  'MEX', 'NGA', 'NOR', 'NPL', 'NZL', 'PER', 'PHL', 'RUS', 'SAU', 'SGP', 'TUR', 'THA', 
                                  'UKR', 'USA', 'VNM', 'ZAF'])
        
        self.AnnexI = set(['AUS', 'AUT', 'BLR', 'BEL', 'BGR', 'CAN', 'HRV', 'CYP', 
                       'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 
                       'ISL', 'IRL', 'ITA', 'JPN', 'LVA', 'LIE', 'LTU', 'LUX', 
                       'MLT', 'MCO', 'NLD', 'NZL', 'NOR', 'POL', 'PRT', 'ROU', 
                       'RUS', 'SVK', 'SVN', 'ESP', 'SWE', 'CHE', 'TUR', 'UKR', 
                       'GBR', 'USA'])
        
        self.G20 = set(['ARG', 'AUS', 'AUT', 'BEL', 'BRA', 'BGR', 'CAN', 'CHN', 'HRV', 'CYP',
                   'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IND', 'IDN',
                   'IRL', 'ITA', 'JPN', 'KOR', 'LVA', 'LIE', 'LTU', 'LUX', 'MLT', 'MEX',
                   'NLD', 'POL', 'PRT', 'ROU', 'RUS', 'SAU', 'SVK', 'SVN', 'ZAF', 'ESP',
                   'SWE', 'TUR', 'GBR', 'USA'])
        
        self.EU28 = set(['AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN',
                   'FRA', 'GBR', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LIE', 'LTU', 'LUX',
                   'LVA', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE'])

    
        self.LDCS = set(["AGO", "BEN", "BFA", "BDI", "CAF", "TCD", "COM", "COD", "DJI", "ERI",
                  "ETH", "GMB", "GIN", "GNB", "LSO", "LBR", "MDG", "MWI", "MLI", "MRT",
                  "MOZ", "NER", "RWA", "STP", "SEN", "SLE", "SOM", "SSD", "SDN", "TZA",
                  "TGO", "UGA", "ZMB",
                  # Asia
                  "AFG", "BGD", "BTN", "KHM", "TLS", "LAO", "MMR", "NPL", "YEM",
                  # Oceania
                  "KIR", "SLB", "TUV", "VUT",
                  # The Americas
                  "HTI"])
    
        self.SIDS = set(['GUY', 'BHS', 'BLZ', 'BRB', 'ATG', 'HTI', 'JAM', 'TTO', 'SUR', 
                     'LCA', 'VCT', 'GRD', 'DMA', 'KNA', 'MSR', 'KIR', 'MHL', 'TUV',
                     'NRU', 'NIU', 'NCL', 'SLB', 'WSM', 'GUM', 'PLW', 'FJI', 'PYF',
                     'FSM', 'ASM', 'COK', 'PNG', 'VUT', 'TON', 'MNP'])
    
        self.AR5 = set(['R5ASIA', 'R5LAM', 'R5MAF', 'R5OECD90+EU', 'R5REF', 'World'])
class Scenarios():
    
    def __init__(self):
        
        #%% 1.5 compatible
        dict_ = dict()
        dict_['models'] = ['AIM_CGE_2.0',
                                         'AIM_CGE_2.0',
                                         'AIM_CGE_2.1',
                                         'AIM_CGE_2.1',
                                         'IMAGE_3.0.1',
                                         'IMAGE_3.0.1',
                                         'MESSAGE-GLOBIOM_1.0',
                                         'MESSAGE-GLOBIOM_1.0',
                                         'MESSAGE-GLOBIOM_1.0',
                                         'POLES_EMF33',
                                         'POLES_EMF33',
                                         'POLES_EMF33',
                                         'POLES_EMF33',
                                         'POLES_EMF33',
                                         'POLES_EMF33',
                                         'POLES_EMF33',
                                         'WITCH-GLOBIOM_4.4',
                                         'WITCH-GLOBIOM_4.4',
                                         'MESSAGEix-GLOBIOM_1.0']

        dict_['scenarios'] = ['SSP1-19',
                                             'SSP2-19',
                                             'TERL_15D_LowCarbonTransportPolicy',
                                             'TERL_15D_NoTransportPolicy',
                                             'IMA15-LiStCh',
                                             'SSP1-19',
                                             'ADVANCE_2020_1.5C-2100',
                                             'SSP1-19',
                                             'SSP2-19',
                                             'EMF33_1.5C_cost100',
                                             'EMF33_1.5C_limbio',
                                             'EMF33_1.5C_nofuel',
                                             'EMF33_WB2C_limbio',
                                             'EMF33_WB2C_nobeccs',
                                             'EMF33_WB2C_nofuel',
                                             'EMF33_WB2C_none',
                                             'CD-LINKS_NPi2020_1000',
                                             'CD-LINKS_NPi2020_400',
                                             'LowEnergyDemand']
        self.compatible_15_sustainable = pd.DataFrame(dict_)

        #%% 2.0 compatible
        dict_ = dict()
        
        dict_['models'] = ['AIM_CGE_2.0', 
                           'AIM_CGE_2.0', 
                           'AIM_CGE_2.0', 
                           'AIM_CGE_2.0', 
                           'AIM_CGE_2.0', 
                           'AIM_CGE_2.0',
                           'AIM_CGE_2.1', 
                           'AIM_CGE_2.1', 
                           'AIM_CGE_2.1', 
                           'AIM_CGE_2.1', 
                           'AIM_CGE_2.1', 
                           'IMAGE_3.0.1', 
                           'IMAGE_3.0.1', 
                           'IMAGE_3.0.1', 
                           'IMAGE_3.0.1', 
                           'IMAGE_3.0.1', 
                           'IMAGE_3.0.1', 
                           'IMAGE_3.0.1', 
                           'MESSAGE_V.3', 
                           'MESSAGE_V.3', 
                           'MESSAGE_V.3', 
                           'MESSAGE-GLOBIOM_1.0',
                           'MESSAGE-GLOBIOM_1.0', 
                           'MESSAGE-GLOBIOM_1.0', 
                           'MESSAGE-GLOBIOM_1.0', 
                           'MESSAGE-GLOBIOM_1.0', 
                           'MESSAGE-GLOBIOM_1.0', 
                           'MESSAGE-GLOBIOM_1.0', 
                           'MESSAGE-GLOBIOM_1.0', 
                           'MESSAGE-GLOBIOM_1.0', 
                           'MESSAGE-GLOBIOM_1.0', 
                           'MESSAGEix-GLOBIOM_1.0', 
                           'MESSAGEix-GLOBIOM_1.0', 
                           'POLES_ADVANCE', 
                           'POLES_ADVANCE', 
                           'POLES_EMF33', 
                           'POLES_EMF33', 
                           'POLES_EMF33', 
                           'POLES_EMF33', 
                           'POLES_EMF33', 
                           'REMIND_1.7', 
                           'REMIND-MAgPIE_1.7-3.0',
                           'REMIND-MAgPIE_1.7-3.0', 
                           'REMIND-MAgPIE_1.7-3.0', 
                           'REMIND-MAgPIE_1.7-3.0', 
                           'REMIND-MAgPIE_1.7-3.0', 
                           'REMIND-MAgPIE_1.7-3.0', 
                           'REMIND-MAgPIE_1.7-3.0', 
                           'REMIND-MAgPIE_1.7-3.0', 
                           'REMIND-MAgPIE_1.7-3.0', 
                           'REMIND-MAgPIE_1.7-3.0', 
                           'WITCH-GLOBIOM_4.2', 
                           'WITCH-GLOBIOM_4.2', 
                           'WITCH-GLOBIOM_4.2', 
                           'WITCH-GLOBIOM_4.4']
        dict_['scenarios'] = ['ADVANCE_2020_WB2C', 
                              'ADVANCE_2030_Price1.5C', 
                              'ADVANCE_2030_WB2C', 
                              'SSP1-26', 
                              'SSP2-26', 
                              'SSP4-26', 
                              'CD-LINKS_NPi2020_1000', 
                              'EMF33_WB2C_cost100', 
                              'EMF33_WB2C_full', 
                              'TERL_2D_LowCarbonTransportPolicy', 
                              'TERL_2D_NoTransportPolicy', 
                              'ADVANCE_2020_WB2C', 
                              'ADVANCE_2030_WB2C', 
                              'CD-LINKS_NPi2020_1000', 
                              'IMA15-LoNCO2',
                              'SSP1-26', 
                              'SSP2-26', 
                              'SSP4-26', 
                              'GEA_Eff_1p5C', 
                              'GEA_Eff_1p5C_Delay2020', 
                              'GEA_Eff_AdvNCO2_1p5C',
                              'ADVANCE_2020_WB2C', 
                              'ADVANCE_2030_Price1.5C',
                              'ADVANCE_2030_WB2C', 
                              'EMF33_Med2C_nobeccs', 
                              'EMF33_Med2C_none', 
                              'EMF33_WB2C_cost100', 
                              'EMF33_WB2C_full', 
                              'EMF33_WB2C_limbio', 
                              'EMF33_WB2C_nofuel', 
                              'EMF33_tax_hi_full', 
                              'CD-LINKS_NPi2020_1000', 
                              'CD-LINKS_NPi2020_400',
                              'ADVANCE_2020_Med2C', 
                              'ADVANCE_2030_Med2C', 
                              'EMF33_Med2C_cost100', 
                              'EMF33_Med2C_limbio', 
                              'EMF33_Med2C_nobeccs', 
                              'EMF33_Med2C_nofuel', 
                              'EMF33_Med2C_none', 
                              'ADVANCE_2020_WB2C', 
                              'CD-LINKS_NPi2020_1000', 
                              'EMF33_WB2C_nobeccs', 
                              'EMF33_WB2C_none', 
                              'PEP_2C_full_eff', 
                              'PEP_2C_full_netzero', 
                              'PEP_2C_red_NDC', 
                              'PEP_2C_red_goodpractice', 
                              'PEP_2C_red_netzero', 
                              'SMP_2C_Def', 
                              'SMP_2C_early',
                              'ADVANCE_2020_WB2C', 
                              'ADVANCE_2030_Price1.5C', 
                              'ADVANCE_2030_WB2C', 
                              'CD-LINKS_NPi2020_1600']
        
        self.compatible_20_sustainable = pd.DataFrame(dict_)
        
        
        dict_ = {'models': ['AIM_CGE_2.0',
              'AIM_CGE_2.0',
              'AIM_CGE_2.0',
              'AIM_CGE_2.1',
              'AIM_CGE_2.1',
              'AIM_CGE_2.1',
              'C-ROADS-5.005',
              'C-ROADS-5.005',
              'C-ROADS-5.005',
              'GCAM_4.2',
              'IMAGE_3.0.1',
              'IMAGE_3.0.1',
              'IMAGE_3.0.1',
              'IMAGE_3.0.1',
              'IMAGE_3.0.1',
              'IMAGE_3.0.1',
              'IMAGE_3.0.1',
              'MERGE-ETL_6.0',
              'MESSAGE-GLOBIOM_1.0',
              'MESSAGE-GLOBIOM_1.0',
              'MESSAGE-GLOBIOM_1.0',
              'MESSAGE-GLOBIOM_1.0',
              'MESSAGE-GLOBIOM_1.0',
              'MESSAGEix-GLOBIOM_1.0',
              'POLES_ADVANCE',
              'POLES_EMF33',
              'POLES_EMF33',
              'POLES_EMF33',
              'POLES_EMF33',
              'POLES_EMF33',
              'POLES_EMF33',
              'POLES_EMF33',
              'POLES_EMF33',
              'POLES_EMF33',
              'POLES_EMF33',
              'REMIND_1.5',
              'REMIND_1.5',
              'REMIND_1.5',
              'REMIND_1.5',
              'REMIND_1.7',
              'REMIND_1.7',
              'REMIND-MAgPIE_1.7-3.0',
              'REMIND-MAgPIE_1.7-3.0',
              'REMIND-MAgPIE_1.7-3.0',
              'REMIND-MAgPIE_1.7-3.0',
              'REMIND-MAgPIE_1.7-3.0',
              'REMIND-MAgPIE_1.7-3.0',
              'REMIND-MAgPIE_1.7-3.0',
              'WITCH-GLOBIOM_3.1',
              'WITCH-GLOBIOM_3.1',
              'WITCH-GLOBIOM_4.2',
              'WITCH-GLOBIOM_4.4',
              'WITCH-GLOBIOM_4.4'],
        'scenarios': ['ADVANCE_2020_1.5C-2100',
              'SSP1-19',
              'SSP2-19',
              'CD-LINKS_NPi2020_400',
              'TERL_15D_LowCarbonTransportPolicy',
              'TERL_15D_NoTransportPolicy',
              'Ratchet-1.5-limCDR-noOS',
              'Ratchet-1.5-noCDR',
              'Ratchet-1.5-noCDR-noOS',
              'SSP1-19',
              'IMA15-AGInt',
              'IMA15-Def',
              'IMA15-Eff',
              'IMA15-LiStCh',
              'IMA15-Pop',
              'IMA15-TOT',
              'SSP1-19',
              'DAC15_50',
              'ADVANCE_2020_1.5C-2100',
              'EMF33_1.5C_cost100',
              'EMF33_1.5C_full',
              'SSP1-19',
              'SSP2-19',
              'LowEnergyDemand',
              'ADVANCE_2020_1.5C-2100',
              'EMF33_1.5C_cost100',
              'EMF33_1.5C_full',
              'EMF33_1.5C_limbio',
              'EMF33_1.5C_nofuel',
              'EMF33_WB2C_cost100',
              'EMF33_WB2C_full',
              'EMF33_WB2C_limbio',
              'EMF33_WB2C_nobeccs',
              'EMF33_WB2C_nofuel',
              'EMF33_WB2C_none',
              'EMC_Def_100$',
              'EMC_LimSW_100$',
              'EMC_NucPO_100$',
              'EMC_lowEI_100$',
              'CEMICS-1.5-CDR12',
              'CEMICS-1.5-CDR8',
              'PEP_1p5C_red_eff',
              'SMP_1p5C_Def',
              'SMP_1p5C_Sust',
              'SMP_1p5C_early',
              'SMP_1p5C_lifesty',
              'SMP_1p5C_regul',
              'SMP_2C_Sust',
              'SSP1-19',
              'SSP4-19',
              'ADVANCE_2020_1.5C-2100',
              'CD-LINKS_NPi2020_1000',
              'CD-LINKS_NPi2020_400']}

        self.compatible_15_unfiltered = pd.DataFrame(dict_)
#        df = pd.read_excel(config.MODULE_DATA_PATH + '/compatible1_5_unfiltered.xlsx')
#        df.loc[:,'ref'] = df.loc[:,['Scenario','Model']].apply(lambda x: '|'.join(x.str.replace(' ','_').str.replace('/','_')),axis=1)
#        dict_ = dict()
#            
#        dict_['models'] = list(df.loc[:,'Model'].apply(lambda x: x.replace(' ','_').replace('/','_')))
#        dict_['scenarios'] = list(df.loc[:,'Scenario'].apply(lambda x: x.replace(' ','_').replace('/','_')))
#        self.compatible_15_unfiltered = pd.DataFrame(dict_)
        
#%% INSTANCES
SCENARIOS = Scenarios()
REGIONS   = Regions()
