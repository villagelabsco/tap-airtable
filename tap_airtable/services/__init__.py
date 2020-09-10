from tap_airtable.airtable_utils import JsonUtils, Relations
import requests
import singer
import urllib.parse
from singer.catalog import Catalog, CatalogEntry


class Airtable(object):
    metadata_url = "https://api.airtable.com/v2/meta/"
    records_url = "https://api.airtable.com/v0/"
    token = None
    selected_by_default = False
    remove_emojis = False

    @classmethod
    def run_discovery(cls, args):
        cls.__apply_config(args.config)
        if "base_id" in args.config:
            base_id = args.config['base_id']
            entries = cls.discover_base(base_id)
            return Catalog(entries).dump()

        bases = cls.__get_base_ids()
        entries = []

        for base in bases:
            entries.extend(cls.discover_base(base["id"], base["name"]))
        return Catalog(entries).dump()


    @classmethod
    def __apply_config(cls, config):
        if "metadata_url" in config:
            cls.metadata_url = config["metadata_url"]
        if "records_url" in config:
            cls.records_url = config["records_url"]
        if "selected_by_default" in config:
            cls.selected_by_default = config["selected_by_default"]
        if "remove_emojis" in config:
            cls.remove_emojis = config["remove_emojis"]
        cls.token = config["token"]

    @classmethod
    def __get_base_ids(cls):
        response = requests.get(cls.metadata_url, headers=cls.__get_auth_header())
        bases = []
        for base in response.json()["bases"]:
            bases.append({
                "id": base["id"],
                "name": base["name"]
            })
        return bases


    @classmethod
    def __get_auth_header(cls):
        return {'Authorization': 'Bearer {}'.format(cls.token)}

    @classmethod
    def discover_base(cls, base_id, base_name=None):
        headers = cls.__get_auth_header()
        response = requests.get(url=cls.metadata_url + base_id, headers=headers)
        response.raise_for_status()
        entries = []

        for table in response.json()["tables"]:
            columns = {}
            table_name = table["name"]
            base = {"selected": cls.selected_by_default,
                    "name": table_name,
                    "properties": columns,
                    "base_id": base_id}

            columns["id"] = {"type": ["null", "string"], 'key': True}

            for field in table["fields"]:
                if not field["name"] == "Id":
                    columns[field["name"]] = {"type": ["null", "string"]}


            entry = CatalogEntry(
                tap_stream_id=table["id"],
                database=base_name or base_id,
                table=table_name,
                stream=table_name,
                metadata=base)
            entries.append(entry)


        return entries

    @classmethod
    def run_sync(cls, config, properties):
        cls.__apply_config(config)

        streams = properties['streams']

        for stream in streams:
            base_id = stream["metadata"]["base_id"]
            table = stream['table_name'].replace('/', '')
            table = table.replace(' ', '')
            table = table.replace('{', '')
            table = table.replace('}', '')

            table = urllib.parse.quote_plus(table)

            schema = stream['metadata']

            if table != 'relations' and schema['selected']:
                response = Airtable.get_response(base_id, schema["name"])

                if response.json().get('records'):
                    records = JsonUtils.match_record_with_keys(schema,
                                                               response.json().get('records'),
                                                               cls.remove_emojis)

                    singer.write_schema(table, schema, 'id')
                    singer.write_records(table, records)

                    offset = response.json().get("offset")

                    while offset:
                        response = Airtable.get_response(base_id, schema["name"], offset)
                        if response.json().get('records'):
                            records = JsonUtils.match_record_with_keys(schema,
                                                                       response.json().get('records'),
                                                                       cls.remove_emojis)

                        singer.write_records(table, records)
                        offset = response.json().get("offset")

        relations_table = {"name": "relations",
                           "properties": {"id": {"type": ["null", "string"]},
                                          "relation1": {"type": ["null", "string"]},
                                          "relation2": {"type": ["null", "string"]}}}

        singer.write_schema('relations', relations_table, 'id')
        singer.write_records('relations', Relations.get_records())

    @classmethod
    def get_response(cls, base_id, table, offset=None):
        table = urllib.parse.quote(table)
        uri = cls.records_url + base_id + '/' + table

        if offset:
            uri += '?offset={}'.format(offset)

        response = requests.get(uri, headers=cls.__get_auth_header())
        response.raise_for_status()
        return response
