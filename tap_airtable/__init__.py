import backoff
import singer
import logging
from tap_airtable.services import Airtable
from requests.exceptions import HTTPError


REQUIRED_CONFIG_KEYS = [
    'token',
]


class CustomException(Exception):
    pass


@backoff.on_exception(backoff.expo,
                      CustomException,
                      max_tries=3)
def operate(main_args):
    try:
        if main_args.discover:
            logging.info("Discovery started")
            Airtable.run_discovery(main_args)
        elif main_args.properties:
            logging.info("Import started")
            Airtable.run_sync(main_args.config, main_args.properties)
    except HTTPError as e:
        if e.response.status_code == 401:
            main_args.config = Airtable.refresh_token(main_args.config)
            raise CustomException()
        else:
            singer.logger.log_error(str(e))
            logging.exception(e)
            exit(-1)
    except Exception as e:
        singer.logger.log_error(str(e))
        logging.exception(e)
        exit(1)


def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    operate(args)


if __name__ == "__main__":
    main()
