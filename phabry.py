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


CONFIG_FILE = 'config.json'
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
                defaults.update(config)
        except FileNotFoundError:
            print('Config file not found: ' + args.conf_file)

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
        exit()
    if args.start:
        args.start = datetime.datetime.strptime(args.start, '%d-%m-%Y')
    else:
        args.start = None

    if args.end:
        args.end = datetime.datetime.strptime(args.end, '%d-%m-%Y')
    else:
        args.end = None
    
    return args


class Phabry(object):
    def __init__(self, name, url, token, from_date, to_date,
                 directory='./phabry_data/'):
        self.name = name
        self.url = url
        self.token = token
        self.directory = os.path.join(directory, name)
        self.from_date = str(int(from_date.replace(tzinfo=datetime.timezone.utc).timestamp())) if \
                        from_date is not None else None
        self.to_date = str(int(to_date.replace(tzinfo=datetime.timezone.utc).timestamp())) if \
                        to_date is not None else None
        os.makedirs(self.directory, exist_ok=True)

    @staticmethod
    def handle_exception(exception, change_type):
        if isinstance(exception, requests.exceptions.RequestException):
            if exception.response is not None:
                log.error('%s failed with http status %i',
                          change_type, exception.response.status_code)
            else:
                log.error('%s failed with error: %s', change_type, exception)
        elif isinstance(exception, json.JSONDecodeError):
            log.error('Reading JSON for %s failed', change_type)
        elif isinstance(exception, Exception):
            log.error('Unknown error occurred for %s: %s', change_type, exception)

    def get_revisions(self, next_page, order, limit=100):
        data = {'api.token': self.token,
                'attachments[subscribers]': 1,
                'attachments[reviewers]': 1,
                'attachments[projects]': 1,
                'order': order,
                'after': next_page,
                'limit': limit
                }
        if self.from_date is not None:
            data['constraints[createdStart]'] = self.from_date
        if self.to_date is not None:
            data['constraints[createdEnd]'] = self.to_date
        response = requests.post(self.url + 'differential.revision.search', data=data)
        response.raise_for_status()

        revisions = json.loads(response.text)
        if revisions['result']['cursor']['after'] is None:
            next_page = False
        else:
            next_page = revisions['result']['cursor']['after']
        return (revisions, next_page)

    def get_transactions(self, revision, next_page):
        data = {'api.token': self.token,
                'objectIdentifier': revision['phid'],
                'after': next_page
                }
        response = requests.get(self.url + 'transaction.search', data=data)
        response.raise_for_status()

        transactions = json.loads(response.text)
        if transactions['result']['cursor']['after'] is None:
            next_page = False
        else:
            next_page = transactions['result']['cursor']['after']
        return (transactions, next_page)


    def run(self):
        next_page = ''
        print('Writing revisions to ' + self.directory)
        try:
            (last_revision, unused_next_page) = self.get_revisions('', 'newest', 1)
        except Exception as exception:
            Phabry.handle_exception(exception, 'newest revision')
        os.makedirs(os.path.join(self.directory, 'revisions'), exist_ok=True)
        os.makedirs(os.path.join(self.directory, 'transactions'), exist_ok=True)
        while next_page is not False:
            try:
                current_page = next_page
                (revisions, next_page) = self.get_revisions(next_page, 'oldest')
                print('Revisions ' + str(revisions['result']['data'][0]['id']) + '-' +
                        str(revisions['result']['data'][-1]['id']) + ' from ' +
                        str(last_revision['result']['data'][0]['id']) + ' (' +
                        str(int(revisions['result']['data'][0]['id']) * 100 //
                        int(last_revision['result']['data'][0]['id'])) +
                        '%) ...', end='\r')
                file_name = str(revisions['result']['data'][0]['id']) + '-' + \
                            str(revisions['result']['data'][-1]['id']) + '.json'
                with open(os.path.join(self.directory, 'revisions', file_name), 'w') as json_file:
                    json.dump(revisions, json_file, indent=2)
            except Exception as exception:
                print("Getting revisions from " + str(current_page) +
                      ' failed. Cannot continue further.')
                Phabry.handle_exception(exception, 'revisions IDs ' + str(current_page))
                
            for rev in revisions['result']['data']:
                next_page_transactions = ''
                while next_page_transactions is not False:
                    try:
                        (transactions, next_page_transactions) = self.get_transactions(rev, next_page_transactions)
                        file_name = str(rev['id']) + '.json'
                        with open(os.path.join(self.directory, 'transactions', file_name), 'w') as json_file:
                            json.dump(transactions, json_file, indent=2)
                    except Exception as exception:
                        Phabry.handle_exception(exception, 'transactions of revision '
                                                + str(rev['id']))
                        next_page_transactions = False

if __name__ == '__main__':
    arguments = parse_arguments()

    os.makedirs(arguments.basedir, exist_ok=True)

    phabry = Phabry(arguments.name, arguments.url, arguments.token, arguments.start,
                    arguments.end, arguments.basedir)
    configure_logging(phabry.directory)

    phabry.run()
