#!/usr/bin/env python3

__copyright__ = """

MIT License

Copyright (c) 2019 Dumitru Cotet

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""
__license__ = "MIT"

import json
import os
import argparse
import datetime
import logging
import requests


CONFIG_FILE = 'configf.json'
log = logging.getLogger('phabry')

def configure_logging(data_dir):
    global log
    log.setLevel(logging.DEBUG)
    log_name = os.path.join(data_dir, 'phabry.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    file_handler = logging.FileHandler(log_name)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    return log

def parse_arguments():
    """parses and sets up the command line argument system above
    with config file parsing."""
    global CONFIG_FILE

    # Parse any conf_file specification
    # add_help=False so it doesn't parse -h and print help.
    conf_parser = argparse.ArgumentParser(
        description=__doc__, # printed with -h/--help
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # Turn off help, to print all options in response to -h
        add_help=False
        )
    conf_parser.add_argument("-c", dest='conf_file', default=CONFIG_FILE,
                             help="Specify config file", metavar="FILE")
    args, remaining_argv = conf_parser.parse_known_args()
    defaults = {}
    if args.conf_file:
        try:
            with open(args.conf_file) as f:
                config = json.load(f)
                defaults.update({k.lower(): v for k, v in config.items()})
        except FileNotFoundError:
            print('Config file not found: ' + args.conf_file)
        except json.decoder.JSONDecodeError:
            print('Config file parsing failed. Please format it as json object')

    # Parse the rest of arguments
    parser = argparse.ArgumentParser(parents=[conf_parser])
    parser.set_defaults(**defaults)
    parser.add_argument('--name', help='REQUIRED Directory name for the Phabricator source')
    parser.add_argument('--url', help='REQUIRED Phabricator api URL')
    parser.add_argument('--token', help='REQUIRED Phabricator api token')
    parser.add_argument('--basedir', default='./phabry_data/',
                        help='Base directory name')
    parser.add_argument('--start', help='Start date dd-mm-yyyy')
    parser.add_argument('--end', help='End date dd-mm-yyyy')
    args = parser.parse_args(remaining_argv)
    if None in (args.name, args.url, args.token):
        parser.print_help()
        print("phabry.py: error: the following arguments are required:", end='')
        if args.name is None:
            print(" --name", end='')
        if args.url is None:
            print(" --url", end='')
        if args.token is None:
            print(" --token", end='')
        exit()
    if args.start:
        start_date = datetime.datetime.strptime(args.start, '%d-%m-%Y')
        args.start = int(start_date.replace(tzinfo=datetime.timezone.utc).timestamp())
    else:
        args.start = None

    if args.end:
        end_date = datetime.datetime.strptime(args.end, '%d-%m-%Y')
        args.end = int(end_date.replace(tzinfo=datetime.timezone.utc).timestamp())
    else:
        args.end = None

    return args


class Phabry(object):
    def __init__(self, name, url, token, from_date, to_date, basedir='./phabry_data/'):
        self.name = name
        self.url = url
        self.token = token
        self.directory = os.path.join(basedir, name)
        self.from_date = from_date
        self.to_date = to_date
        os.makedirs(os.path.join(self.directory, 'revisions'), exist_ok=True)
        os.makedirs(os.path.join(self.directory, 'transactions'), exist_ok=True)
        configure_logging(self.directory)

    @staticmethod
    def handle_exception(exception, object_type):
        if isinstance(exception, requests.exceptions.RequestException):
            if exception.response is not None:
                log.error('%s failed with http status %i',
                          object_type, exception.response.status_code)
            elif exception.errno is not None:
                log.error('%s failed with error: %s - %s',
                          object_type, exception.errno, exception.strerror)
            else:
                log.error('%s failed with error: %s', object_type, exception)
        elif isinstance(exception, json.JSONDecodeError):
            log.error('Reading JSON for %s failed', object_type)
        elif isinstance(exception, Exception):
            log.error('Unknown error occurred for %s: %s', object_type, exception)

    def get_revisions(self, after_cursor, order, limit=100):
        data = {'api.token': self.token,
                'attachments[subscribers]': 1,
                'attachments[reviewers]': 1,
                'attachments[projects]': 1,
                'order': order,
                'after': after_cursor,
                'limit': limit
                }
        if self.from_date is not None:
            data['constraints[createdStart]'] = self.from_date
        if self.to_date is not None:
            data['constraints[createdEnd]'] = self.to_date
        response = requests.post(self.url + 'differential.revision.search', data=data)
        response.raise_for_status()
        revisions = json.loads(response.text)
        if revisions["error_code"] is not None:
            raise requests.exceptions.RequestException(revisions["error_code"],
                                                       revisions["error_info"])
        if revisions['result']['cursor']['after'] is None:
            after_cursor = False
        else:
            after_cursor = revisions['result']['cursor']['after']
        return (revisions, after_cursor)

    def get_transactions(self, revision_phid, after_cursor):
        data = {'api.token': self.token,
                'objectIdentifier': revision_phid,
                'after': after_cursor
                }
        response = requests.get(self.url + 'transaction.search', data=data)
        response.raise_for_status()
        transactions = json.loads(response.text)
        if transactions["error_code"] is not None:
            raise requests.exceptions.RequestException(transactions["error_code"],
                                                       transactions["error_info"])
        if transactions['result']['cursor']['after'] is None:
            after_cursor = False
        else:
            after_cursor = transactions['result']['cursor']['after']
        return (transactions, after_cursor)


    def run(self):
        after_cursor = ''
        first_rev_id = 0
        print('Writing revisions to ' + self.directory)
        try:
            (last_revision, unused_after_cursor) = self.get_revisions('', 'newest', 1)
            last_rev_id = last_revision['result']['data'][0]['id']
        except Exception as exception:
            print("Getting the latest revision failed. Cannot continue further.")
            raise exception
        while after_cursor is not False:
            try:
                (revisions, after_cursor) = self.get_revisions(after_cursor, 'oldest')
                current_first_rev_id = revisions['result']['data'][0]['id']
                current_last_rev_id = revisions['result']['data'][-1]['id']
                if first_rev_id == 0:
                    first_rev_id = current_first_rev_id
                print('Revisions', str(current_first_rev_id) + '-' + str(current_last_rev_id),
                      'from', str(last_rev_id), '(' +
                      str((current_first_rev_id - first_rev_id) * 100 //
                          (last_rev_id - first_rev_id)) +
                      '%) ...', end='\r')
                file_name = str(first_rev_id) + '-' + \
                            str(last_rev_id) + '.json'
                with open(os.path.join(self.directory, 'revisions', file_name), 'w') as json_file:
                    json.dump(revisions, json_file, indent=2)
            except Exception as exception:
                after_cursor = '0' if after_cursor == '' else after_cursor
                print("Getting the revisions after {}. Cannot continue further.".format(after_cursor))
                raise exception

            for rev in revisions['result']['data']:
                after_cursor_transactions = ''
                file_count = 0
                while after_cursor_transactions is not False:
                    try:
                        (transactions, after_cursor_transactions) = \
                            self.get_transactions(rev['phid'], after_cursor_transactions)
                        file_name = str(rev['id']) + '_' + str(file_count) + '.json'
                        with open(os.path.join(self.directory, 'transactions', file_name),
                                  'w') as json_file:
                            json.dump(transactions, json_file, indent=2)
                        if after_cursor_transactions is not False:
                            file_count += 1
                    except Exception as exception:
                        Phabry.handle_exception(exception, 'transactions of revision '
                                                + str(rev['id']))
                        after_cursor_transactions = False

if __name__ == '__main__':
    arguments = parse_arguments()
    phabry = Phabry(arguments.name, arguments.url, arguments.token, arguments.start,
                    arguments.end, arguments.basedir)
    phabry.run()
