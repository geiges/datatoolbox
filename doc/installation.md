

## Installation

#### Software installation outside python
```bash
sudo apt install git
```
#### Datatoolbox python dependencies

See enviroment file.


## Set up datatoolbox and connect to a database



First time you import datatoolbox, you need to set up the local database and link it to datatoolbox. Otherwise, a simple SANDBOX data structure is loaded for playing around.

### 1) Create local empty database folder

```
import datatoolbox as dt
dt.admin.create_empty_datashelf('/your/path/on/local/hard_disk')
```

This created an empty database that can be linked to datatoolbox. This link is one by creating you personal setting including you name and the same path to the database folder on you hard disk without quotation marks.

```
dt.admin.change_personal_config()
```

![](figures/config_input.png)

### 2) Set up remote Access

Datatoolbox allows to automatically integrate new data sets by using git + ssh connections. However, this requires having git installed on you system and ssh connection properly set up. The following example shows the outlines the required steps to access gitlab via ssh:

- Create account and apply to https://gitlab.com/climateanalytics
- set up ssh key access (https://docs.gitlab.com/ee/ssh/)

Available datasets on gitlab:
https://gitlab.com/climateanalytics/datashelf

### 3) Import remote sources

Import of remote source (using git in the background)

```
import datatoolbox as dt
dt.core.DB.importSourceFromRemote('WDI_2020')
dt.core.DB.importSourceFromRemote('PRIMAP_2019')
```

After the import, the datasets will be available in your local database and can be accessed by functions of datatoolbox.
