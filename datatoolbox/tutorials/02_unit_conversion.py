#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Andreas Geiges

TUTORIAL:       
      
"""

import datatoolbox as dt
import pandas as pd
import matplotlib.pyplot as plt

#%% simple conversion factory

# conversion form kt oil equivalent to Exajoule
dt.conversionFactor('ktoe', 'EJ')

# conversion of compont units 
dt.conversionFactor('ktoe / yr', 'EJ / d')


# conversion of tables
finalEnergyCommercial = dt.getTable('Final_Energy|Comercial_and_public|Total||historic|IEA_WEB_2019')
print("Final energy Commercial buildings")
print("Germany: {:2.2f} {}".format(finalEnergyCommercial.loc['DEU',2015],finalEnergyCommercial.meta['unit']))

finalEnergyCommercial = finalEnergyCommercial.convert('TWh / d')
print("Germany: {:2.2f} {}".format(finalEnergyCommercial.loc['DEU',2015],finalEnergyCommercial.meta['unit']))


finalEnergyResidential = dt.getTable('Final_Energy|Residential|Total||historic|IEA_WEB_2019')
print("Final energy Residential buildings")
print("Germany: {:2.2f} {}".format(finalEnergyResidential.loc['DEU',2015],finalEnergyResidential.meta['unit']))


#%% computing with tables
# unit of first table is used to convert tables for calculations 
# missmatch of units does not lead to errors

totalFinalEnergy = finalEnergyCommercial + finalEnergyResidential
print("Final energy: Buildings total")
print("Germany: {:2.2f} {}".format(totalFinalEnergy.loc['DEU',2015],totalFinalEnergy.meta['unit']))

#%% example of impossible conversions