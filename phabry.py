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

import requests
import json
import os
import argparse
import datetime
import glob
import tqdm
import logging
import configparser

CONFIG_FILE='config.json'
log = logging.getLogger('phabry')

def config_logging(data_dir):
    global log
    log.setLevel(logging.DEBUG)
    log_name = os.path.join(data_dir, 'phabry-crawl.log')
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
    # We make this parser with add_help=False so that
    # it doesn't parse -h and print help.
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
    arguments = parser.parse_args(remaining_argv)
    if None in (arguments.name, arguments.url, arguments.token):
        parser.print_help()
        exit()
    today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    if arguments.start:
        arguments.start = datetime.datetime.strptime(arguments.start, '%d-%m-%Y')
    else:
        arguments.start = datetime.datetime(2009, 1, 1)

    if arguments.end:
        arguments.end = datetime.datetime.strptime(arguments.end, '%d-%m-%Y')
    else:
        arguments.end = today
    
    return arguments


class Phabry(object):
    def __init__(self, name, url, token, from_date, to_date,
                 directory='./phabry_data/'):
        self.name = name
        self.url = url
        self.token = token
        self.directory = os.path.join(directory, name)
        self.from_date = str(int(from_date.replace(tzinfo=datetime.timezone.utc).timestamp()))
        self.to_date = str(int(to_date.replace(tzinfo=datetime.timezone.utc).timestamp()))
        os.makedirs(self.directory, exist_ok=True)

    @staticmethod
    def handle_exception(exception, change_type):
        if isinstance(exception, requests.exceptions.RequestException):
            if exception.response is not None:
                log.error('%s failed with http status %i' % (
                    change_type, exception.response.status_code))
            else:
                log.error('%s failed with error: %s' % (change_type,
                                                            exception))
        elif isinstance(exception, json.JSONDecodeError):
            log.error(
                'Reading JSON for %s failed' % (change_type))
        elif isinstance(exception, Exception):
            log.error('Unknown error occurred for %s: %s' % (change_type,
                                                             exception))

    def get_revisions(self, next_page, order, limit=100):
        revisions = []

        data = {'api.token': self.token,
                'constraints[createdStart]': self.from_date,
                'constraints[createdEnd]': self.to_date,
                'attachments[subscribers]': 1,
                'attachments[reviewers]': 1,
                'attachments[projects]': 1,
                'order': order,
                'after': next_page,
                'limit': limit
                }
        response = requests.post(self.url + 'differential.revision.search', data = data)
        response.raise_for_status()

        revisions_subset = json.loads(response.text)
        revisions += revisions_subset['result']['data']
        if revisions_subset['result']['cursor']['after'] is None:
            next_page = False
        else:
            next_page = revisions_subset['result']['cursor']['after']
        return (revisions, next_page)

    def get_details(self, revision):
        data = {'api.token': self.token,
                'objectIdentifier': revision['phid']
                }
        response = requests.get(self.url + 'transaction.search', data = data)
        response.raise_for_status()
        revision['activity'] = json.loads(response.text)['result']['data']
        return revision


    def run(self):
        next_page = ''
        print('Writing revisions to ' + self.directory)
        try:
            last_revision = self.get_revisions(next_page, 'newest', 1)
        except Exception as exception:
            Phabry.handle_exception(exception, 'newest revision')

        while next_page is not False:
            try:
                (revisions, next_page) = self.get_revisions(next_page, 'oldest')
                print('Revisions ' + str(revisions[0]['id']) + '-' + str(revisions[-1]['id']) + ' from ' +
                        str(last_revision[0][0]['id']) + ' (' +
                        str(int(revisions[0]['id']) * 100 // int(last_revision[0][0]['id'])) +
                        '%) ...', end='\r')
            except Exception as exception:
                Phabry.handle_exception(exception, 'revisions IDs ' + str(next_page))
            if revisions:
                os.makedirs(os.path.join(self.directory, 'revisions'), exist_ok=True)
            for index, rev in enumerate(revisions):
                try:
                    revisions[index] = self.get_details(rev)
                except Exception as exception:
                    Phabry.handle_exception(exception, 'revision ' + str(rev['id']))
            file_name = str(revisions[0]['id']) + '-' + str(revisions[-1]['id']) + '.json'
            try:
                with open(os.path.join(self.directory, 'revisions', file_name), 'w') as json_file:
                    json.dump(revisions, json_file, indent=2)
            except Exception as exception:
                Phabry.handle_exception(exception, 'revision IDs' + str(next_page))

if __name__ == '__main__':
    arguments = parse_arguments()

    os.makedirs(arguments.basedir, exist_ok=True)

    phabry = Phabry(arguments.name, arguments.url, arguments.token, arguments.start, arguments.end, arguments.basedir)
    config_logging(phabry.directory)

    phabry.run()
