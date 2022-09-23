# Airtable

This tap pulls raw data from a [AirTable](https://airtable.com/api) database.


## Supported Features
| Feature Name                                                                            | Supported | Comment                                 |
| ------------                                                                            |-------|-----------------------------------------|
| [Full Import](https://docs.y42.com/docs/features#full-import)                           | ✅      |                                         |
| [Partial Import](https://docs.y42.com/docs/features#partial-import)                     | ❌    | No incremental import supported         |
| [Start Date Selection](https://docs.y42.com/docs/features#start-date-selection)         | ❌    |                                         |
| [Import Empty Tables](https://docs.y42.com/docs/features#import-empty-table)            | ❌    | Empty streams will not generate a table |
| [Custom Data](https://docs.y42.com/docs/features#custom-data)                           | ✅     | Dynamic schemas                         |
| [Retroactive Updating](https://docs.y42.com/docs/features#retroactive-updating)         | ❌     | No historical data                      |
| [Dynamic Column Selection](https://docs.y42.com/docs/features#dynamic-column-selection) | ✅    | Select optional, non mandatory columns  |      



## Connector

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

### Supported Streams

  - Schema generation is dynamic, the streams could not be known before execution
  - Synch is done as FULL TABLE only

### Workflow

There is no predefined schema for this integration. 
The integration will dynamically load the tables & columns which are defined within your specific system.
For this source you can use full imports. Every time the source syncs, it will fully get all your data.

> Quotas 

The API is limited to  5 requests per second, per base. 
If you exceed this rate, you will receive a 429 status code and will need to wait 30 seconds before subsequent requests will succeed.
This limit is the same across all pricing tiers and increased rate limits are not currently available. 

---

## Quick Start - Install

```bash
python3 -m venv ~/.virtualenvs/tap-airtable
source ~/.virtualenvs/tap-airtable/bin/activate
pip install -e .
```

### Create the configuration file


| Configuration Key   | required | Description                                                                                              |
|---------------------|----------|----------------------------------------------------------------------------------------------------------|
| metadata_url        | - | Airtable metadata URL, at the time of the update: "https://api.airtable.com/v2/meta/"                    |
| records_url         | - | Airtable content URL, at the time of the update: "https://api.airtable.com/v0/"                          |
| token               | yes | Airtable Token                                                                                           |
| base_id             | - | Airtable base ID to export                                                                               |
| selected_by_default | - | Default for every table in the base. If set to true, all of the tables in the schema will be syncronized |
| remove_emojis       | - | Filter out emojis from the scyncronization                                                               |


#### Configuration file example


```json
{
    "metadata_url":"https://api.airtable.com/v2/meta/",
    "records_url":"https://api.airtable.com/v0/",
    "token":"airtable_token",
    "base_id": "airtable_base_id",
    "selected_by_default": true,
    "remove_emojis": false
}
```


### Discovery mode

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$ tap-airtable --config config.json --discover

```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

The `selected-by-default` fields is used to enable the sync of the tables. If set to 'true', all of the tables will be 
selected in the `properties.json` 



## Target project (Example: target-postgres) 

### Clone target-postgres project

```shell
 git clone https://github.com/datamill-co/target-postgres
 cd target-postgres
```

### To install dependencies on target project run the commands

```shell
 python3 -m venv ~/.virtualenvs/target-postgres
 source ~/.virtualenvs/target-postgres/bin/activate
 pip install target-postgres
```

### To run full tap and target action run for a particular Base

Complete the config.json 

```
{
    "metadata_url":"https://api.airtable.com/v2/meta/",
    "records_url":"https://api.airtable.com/v0/",
    "token":"airtable-api-key",
    "base_id": "base-id",
    "selected_by_default": true
}
```

From the home directory of the project 

```shell
tap-airtable -c config.json --properties properties.json | ~/.virtualenvs/target-postgres/bin/target-postgres 
```
---

# How To Setup
## Resources


### Overview
 
**Authentication**: App Credentials     
**Settings**: None     
**Schema type**: Dynamic  
**Update Type**: Full import     


### Authorization and Access

To authorize, an API Key/ Token should be generated first. 
For reference check [here](https://support.airtable.com/docs/how-do-i-get-my-api-key#:~:text=On%20your%20account%20overview%20page,the%20Regenerate%20API%20key%20option).

