# Datatoolbox - First steps



## Software installation

#### Os software

sudo apt install git

#### Python packages

pip install pint==0.9
pip install pyam-iamc==0.3.0
pip install pycountry
pip install datatoolbox --upgrade



## Set up Datatoolbox to connect to a database

Python

`import datatoolbox as dt`
`dt.admin.create_empty_datashelf('/Users/andreasgeiges/dataself') `

`dt.admin.change_personal_config()This will set up a local database structure on you computer that is used by datatoolbox.`


Code and data access via gitlab
- Create account and apply to https://gitlab.com/climateanalytics
- set up ssh key access (https://docs.gitlab.com/ee/ssh/)


Work folder with all computations that is shared among all people that help.
- clone code repository: git clone git@gitlab.com:climateanalytics/b2g_2020.git


Available datasets on gitlab:
https://gitlab.com/climateanalytics/datashelf

#### Python

Import of remote source (using git in the background)

`import datatoolbox as dt`
`dt.core.DB.importSourceFromRemote('WDI_2020')`
`dt.core.DB.importSourceFromRemote('PRIMAP_2019')`
After the import, the datasets will be available on your computer



Working with datatoolbox

#%% dt.find usage

##### Finds datatables that contain the search string

`dt.find(variable ='', entity='', category='', scenario ='', model='' source='')`

#%% The underlying inventory
`inventory = dt.find()` #returns the full inventory
`print(inventory.head())`

##### Data inventory strucutures in for categories : ['variabale', entity', 'category', 'scenario', 'model', source']
`print(inventory.columns)`

##### Each ID is unique is contructed as the joined string of the categories above using '|'
##### Each entry is representing a datatable with the index beeing the ID
`print(inventory.index[0:10])`

`#%% List all data sources`
`sources = list(dt.find().source.unique())`
`sources.sort()`
`print(sources)`

`#%% List all scenarios within a source`
`res = dt.find(source='PRIMAP_2019')`
`print(res.scenario.unique())`

`#%% List all variables within a source`
`print(res.entity.unique())`

`#%% List all Emissions|KYOTOGHG data tables` 
`res = dt.find(entity = "Emissions|KYOTOGHG", source='PRIMAP_2019')`
`print(res.entity.unique())`

`#%% dt.findExact`

# dt.find returns mutltiple matching datatables


##### dt.find returns mutltiple matching datatables

`res = dt.find(entity = "Emissions|CO2", source='IAMC')`
`print(res.entity.unique())`

##### Finds datatables that match the search string exactly

`res = dt.findExact(entity = "Emissions|CO2", source='IAMC15_2019_R2')`
`print(res.entity.unique())`

### Data Access

##### Tables are accessed by their ID, given by the inventory index returning a Datatable
`table = dt.getTable(inventory.index[100])`

##### or given by the result dataframe
`table = dt.getTable(res.index[100])`

##### Multiple tables can be loaded at once and returned in a tableSet (being a dictionary+)
`tables = dt.getTables(res.index[:10])`

`#%% Datatable`

`print(type(table))`

### A Datatable is a pandas Dataframe with additonal restrictions and functionalities
- columns are integer years
- the index only consists of valied region identifiers
- the data is numeric
- each table as meta data attached with some required varariables (see config.py)
- each tables only consists of one variable with the same unit which 
- allows for easy unit conversion (see unit_conversion.py as tutorial)
- each table is stored as a csv file each inf the dedicated source folder

`print(dt.core.DB._getTableFilePath(table.ID))`

`#%% DataSet`

`print(type(tables))`

# A TableSet is a dictionary with minor additional functionalities

# interface to pyam
iamDataFrame = tables.to_IamDataFrame()

# interface to excel
tables.to_excel('output_test.xlsx')