#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 18 15:48:29 2021

@author: ageiges
"""
# import datatoolbox as dt

from pymagicc.definitions import (
    MAGICC7_TO_MAGICC6_VARIABLES_MAPPING,
    convert_magicc7_to_openscm_variables,
    MAGICC7_EMISSIONS_UNITS,
)

ar4_gwp = {
    'CO2': 1,
    'CH4': 25,
    'N2O': 298,
    'CFC11': 4750,
    'CFC12': 10900,
    'CFC13': 14400,
    'CFC113': 6130,
    'CFC114': 10000,
    'CFC115': 7370,
    'HALON1301': 7140,
    'HALON1211': 1890,
    'HALON2402': 1640,
    'CCL4': 1400,
    'CH3BR': 5,
    'CH3CCL3': 146,
    'HCFC22': 1810,
    'HCFC123': 77,
    'HCFC124': 609,
    'HCFC141b': 725,
    'HCFC142b': 2310,
    'HCFC225ca': 122,
    'HCFC225cb': 595,
    'HFC23': 14800,
    'HFC32': 675,
    'HFC41': 92,
    'HFC125': 3500,
    'HFC134a': 1430,
    'HFC134': 1100,
    'HFC143a': 4470,
    'HFC143': 353,
    'HFC152': 53,
    'HFC152a': 124,
    'HFC161': 4,
    'HFC227ea': 3220,
    'HFC227a': 3220,
    'HFC236cb': 1210,
    'HFC236ea': 1330,
    'HFC236fa': 9810,
    'HFC245ca': 693,
    'HFC245fa': 1030,
    'HFC365mfc': 794,
    'HFC43-10': 1640,
    'SF6': 22800,
    'NF3': 17200,
    'CF4': 7390,
    'C2F6': 12200,
    'C3F8': 8830,
    'CC3F6': 9200,
    'CC4F8': 10300,
    'C4F10': 8860,
    'C5F12': 9160,
    'C6F14': 9300,
    'C7F16': 9300,
    'C10F18': 7190,
}


def read_MAGICC6_scen_file(filepath):
    #%%
    import datatoolbox as dt
    from pymagicc.io import MAGICCData, read_cfg_file, NoReaderWriterError

    mdata = MAGICCData()
    mdata.read(filepath)
    mdata.df

    tset = dt.TableSet()
    for variable in mdata.df.variable.unique():
        idx = mdata.df.variable.index[mdata.df.variable == variable]
        data = mdata.df.loc[idx, :]
        years = data.time
        values = data.value
        unit = data.unit[data.index[0]]
        if len(data.region.unique()) == 1:
            region = data.region[data.index[0]]
        meta = {'variable': variable, 'unit': unit}
        edata = dt.Datatable([list(values)], index=[region], columns=years, meta=meta)
        if ('CO2' in variable) and ("t C / yr" in unit):
            edata = edata.convert('Mt CO2 / yr')
            # print(edata)
        if ('N2O' in variable) and ("t N2ON / yr" in unit):
            edata = edata.convert('Mt N2O / yr')
            # print(edata)

        tset[variable] = edata
    return tset


#%%
def convert_to_CO2eq(tset):
    #%%

    for key in tset.keys():
        table = tset[key]

        unit = table.meta['unit']
        meta = table.meta.copy()
        for unit_to_test in ar4_gwp.keys():

            if unit_to_test in unit:
                print(f"{table.meta['variable']} -> COeq: {ar4_gwp[unit_to_test]}")
                table = table * ar4_gwp[unit_to_test]

                table.meta = meta
                table.meta['unit'] = table.meta['unit'].replace(unit_to_test, 'CO2eq')
                break
        else:
            print(f"Could not convert: {table.meta['variable']} -> COeq: 0")
            table = table * 0
            table.meta = meta
            table.meta['unit'] = table.meta['unit'].replace(unit_to_test, 'CO2eq')
            # table.meta['variable'] = table.meta['unit'].replace(unit_to_test, 'CO2eq')
            # print(f'Could not convert {table.meta["variable"]}')

        if 'kt' in table.meta['unit']:
            table = table.convert(table.meta['unit'].replace('kt', 'Mt'))
        tset[key] = table
    return tset


if __name__ == '__main__':
    #%%
    filepath = '/media/sf_Documents/python/cat_global_aggregation_2.0/output/211017_21_10_COP_0/magicc_results/MAGICCresults_CPPHIGH_CATFINAL_211017/QuantileCalculation/results/EQW_SPLIT_CPPHIGH_CATFINAL_211017.SCEN'
    dset = read_MAGICC6_scen_file(filepath)
    dset = convert_to_CO2eq(dset)
    df, meta = dset.to_compact_long_format()

    df.loc[:, df.columns[3:]].sum().plot()

    filepath = '/media/sf_Documents/python/cat_global_aggregation_2.0/output/211017_21_10_COP_0/magicc_results/MAGICCresults_CPPLOW_CATFINAL_211017/QuantileCalculation/results/EQW_SPLIT_CPPLOW_CATFINAL_211017.SCEN'
    dset = read_MAGICC6_scen_file(filepath)
    dset = convert_to_CO2eq(dset)
    df, meta = dset.to_compact_long_format()

    df.loc[:, df.columns[3:]].sum().plot()
