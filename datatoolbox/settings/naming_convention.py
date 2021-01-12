#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Naming convention for entities used in for datatoolbox
"""

#%% Emissions
emission_entities = set([
    'Emissions|KYOTOGHG_AR4', # EM|KYO_AR4
    'Emissions|KYOTOGHG_AR5', # EM|KYO_AR4
    'Emissions|BC',
    'Emissions|CO2',
    'Emissions|CH4',
    'Emissions|NH3',
    'Emissions|N2O',
    'Emissions|NF3',
    'Emissions|NOx',
    'Emissions|HFCs',
    'Emissions|OC',
    'Emissions|SF6',
    'Emissions|PFCs',
    'Emissions|VOC',])

#%% Energy (production if not otherwise stated)
energy_entities = set([
    'Primary_Energy',           # PE
    'Secondary_Energy',         # SE
    'Final_Energy',             # FE 
    'Electric_Energy',          # ELCT 
    'Electric_Energy|Capacity', # ELCT_CAP
    'Heat_Energy',              # HEAT
    'Heat_Energy|Capacity',])   # HEAT_CAP

#%% Economic enitites
economic_entities = set([
    'GDP|PPP|constant',
    'GDP|PPP|current',
    'GDP|MER',
    'Investment',   # INV 
    'Subsidies',    
    'Price',
    'Capital_Costs'
    'Exports',      # EXP
    'Imports',      # IMP
    'Value_Added',  # VAL_ADD 
    'Value_Lost',   # VA_LO 
    'Population'])  # POP

#%% Other entities
other_entities = set([
    'Area',
    'Count',
    'Global_Mean_Temperature' # GMT
    'Climate_Radiative_Forcing']) # CRF)

entities = set.union(emission_entities, 
                     energy_entities,
                     economic_entities,
                     other_entities)

# What to do with those? Like pre-fixes?
# Share
# Intensity
# Price
# Concentration
# Production
# Demand
# Storage
# Losses
# Total (implied?)
# Emissions
