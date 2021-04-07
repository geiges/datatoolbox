
from datatoolbox import getTable

def share_of_emissions(isos, year='latest', gwp='AR4'):
    """
    Return the pint unit for a given unit string. Compared to the original
    pint this functions replaces special characters $ € and % by a string 
    reprentation.

    Parameters
    ----------
    isos : list
        ISO-3 values to use in the numerator for the emissions share
        calculation
    year : int, optional
        the year to calculate the emissions share, default is the latest
        available year in historical data
    gwp : str, optional
        the GWP value to use (per PRIMAP variable names KYOTOGHG{gwp}),
        default is 'AR4'

    Returns
    -------
    share : float
        the share (in fraction) of emissions in the group of countries
    """
    gwp_supported = ['AR4', 'SAR']
    if gwp not in gwp_supported:
        raise ValueError(f'gwp arg only supports {gwp_supported}, {gwp} provided')
    gwp = '' if gwp == 'SAR' else gwp

    tbl = f'Emissions|KYOTOGHG{gwp}|IPCM0EL__Historic|country_reported__PRIMAP_2019'
    hist = getTable(tbl)
    year = hist.columns[-1] if year == 'latest' else year
    share = hist.loc[isos, year].sum() / hist.loc['EARTH', year]
    return share

