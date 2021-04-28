import singer
import logging
from tap_airtable.services import Airtable


REQUIRED_CONFIG_KEYS = [
    'token',
]


def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    try:
        if args.discover:
            Airtable.run_discovery(args)
        elif args.properties:
            Airtable.run_sync(args.config, args.properties)
    except Exception as e:
        singer.logger.log_error(str(e))
        logging.exception(e)
        exit(1)


if __name__ == "__main__":
    main()
