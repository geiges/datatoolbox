#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Naming convention for entities used in for datatoolbox
"""

#%% Emissions
emission_entities = set(
    [
        'Emissions|Kyoto Gases (AR4-GWP100)',
        'Emissions|Kyoto Gases (AR5-GWP100)',
        'Emissions|Kyoto Gases (SAR-GWP100)',
        'Emissions|Kyoto Gases',
        'Emissions|KYOTOGHG_SAR',
        'Emissions|KYOTOGHG_AR4',  # EM|KYO_AR4
        'Emissions|KYOTOGHG_AR5',  # EM|KYO_AR4
        'Emissions|BC',
        'Emissions|CO2',
        'Emissions|CH4',
        'Emissions|C2F6',
        'Emissions|C6F14',
        'Emissions|CF4',
        'Emissions|NH3',
        'Emissions|N2O',
        'Emissions|NF3',
        'Emissions|NOx',
        'Emissions|HFCs',
        'Emissions|OC',
        'Emissions|SF6',
        'Emissions|PFCs',
        'Emissions|VOC',
        'Emissions|CO',
        'Emissions|Sulfur',
        'Emissions|F-Gases',
        'Emissions|HFC',
        'Emissions|PFC',
    ]
)

#%% Energy (production if not otherwise stated)
energy_entities = set(
    [
        'Cumulative Capacity',
        'Capacity Additions',
        'Capacity',
        'Capacity|Electricity|Storage Capacity',
        'Capital Cost',
        'Carbon Sequestration',
        'Primary Energy',  # PE
        'Secondary Energy',  # SE
        'Secondary Energy|Electricity',  # SE
        'Secondary Energy|Heat',  # SE
        'Final Energy',  # FE
    ]
)  # ELCT_CAP

#%% Economic enitites
economic_entities = set(
    [
        'Discount rate',
        'GDP|PPP|constant',
        'GDP|PPP|current',
        'GDP|MER',
        'GDP|PPP',
        'Investment',  # INV
        'Subsidies',
        'Price',
        'Capital Costs',
        'Policy Cost',
        'Exports',  # EXP
        'Imports',  # IMP
        'Value Added',  # VAL_ADD
        'Value Lost',  # VA_LO
        'Population',
        'Trade',
    ]
)  # POP

#%% Other entities
other_entities = set(
    [
        'Agricultural Production',
        'Area',
        'Count',
        'GMT' 'Climate_Radiative_Forcing',  # Global Mean Temperature
        'Food Demand',
        'Fertilizer Use',
        'Land Cover',
        'Water Withdrawal',
        'Water Consumption',
        'Forestry Demand',
        'Forestry Production',
        'Yield',
    ]
)  # CRF)

#%% warming assessment
warming_entities = [
    'AR5 climate diagnostics|Concentration',
    'AR5 climate diagnostics|Forcing',
    'AR5 climate diagnostics|Temperature|Exceedance Probability',
    'AR5 climate diagnostics|Temperature',
    'Concentration',
]
entities = set.union(
    emission_entities,
    energy_entities,
    economic_entities,
    other_entities,
    warming_entities,
)

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
