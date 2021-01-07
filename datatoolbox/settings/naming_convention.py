#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Naming convention for entities used in for datatoolbox
"""

#%% Emissions
emission_entities = set([
    'Emissions|KYOTOGHG_AR4',
    'Emissions|KYOTOGHG_AR5',
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
    'Primary_Energy',
    'Secondary_Energy',
    'Final_Energy',
    'Electric_Energy', 
    'Electric_Energy|Capacity',
    'Heat_Energy', 
    'Heat_Energy|Capacity',])

#%% Economic enitites
economic_entities = set([
    'GDP|PPP|constant',
    'GDP|PPP|current',
    'GDP|MER',
    'Investment',
    'Subsidies',
    'Price',
    'Exports',
    'Imports',
    'Value_Added',
    'Value_Lost',
    'Population',
    'Demand',
    'Production'])

#%% Other entities
other_entities = set([
    'Area',
    'Count',
    'GMT'
    'Climate_radiative_forcing'])

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

