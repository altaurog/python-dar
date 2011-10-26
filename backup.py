#!/usr/bin/env python
# requires python 2.6 - 3.x
from __future__ import print_function

import os
import subprocess
import socket

from datetime import datetime

try:
    from shlex import quote
except ImportError:
    from pipes import quote

"""
TODO:
    incremental archives 
    automate archive test
    add archives to dar_manager db
    test restore procedure
    call par2 on archives
    upload selected archives (home dirs) to amazon s3
    backup remote systems, both linux and windows
    build dar 2.4.2 and use that

    monthly/weekly TOH, start on first Sat of month:

        S S M T W T F
        -------------
        0 3 2 5 4 7 6 
        1 3 2 5 4 7 6 
        1 3 2 5 4 7 6 
        1 3 2 5 4 7 6 
       (1 3 2 5 4 7 6)

    from Preston, W. Curtis.  Unix Backup & Recovery.  O'Reilly, 1999.  p.41
    [modified to start on Saturday]
"""

def get_timestamp():
    # timezone = pytz.timezone('Asia/Jerusalem') # don't waste time on this now
    now = datetime.now()
    fmt = '%Y%m%d_%H%M'
    return now.strftime(fmt)

def get_dcf():
    confdir = os.path.join(os.path.dirname(__file__), 'config')
    dcf_dict = {}
    for name in os.listdir(confdir):
        if name.endswith('.dcf'):
            fullpath = os.path.join(confdir, name)
            if os.path.isfile(fullpath):
                dcf_dict[name[:-4]] = fullpath

    return dcf_dict

def checkpath(*args):
    path = os.path.join(*args)
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def quote_command(cmd, *args):
    return cmd + ' '.join(map(quote, args))

class BackupSession(object):
    def __init__(self, path):
        self.sid = get_timestamp()
        self.path = checkpath(path, self.sid)
        logfile = os.path.join(self.path, 'log')
        self.log = open(logfile, 'w')

    def flushlog(self):
        self.log.flush()
        os.fsync(self.log.fileno())

    def print_f(self, template, *args, **kwargs):
        print(template.format(*args, **kwargs), file=self.log)
        self.flushlog()

    def backup(self, name, reference=None):
        host = socket.gethostname()
        path = checkpath(self.path, host, name)

        basename = '-'.join((host, name, self.sid))
        catalog = 'catalog-' + basename

        args = [
            'dar',

            # create an archive
            '--create', os.path.join(path, basename),

            # create a isolated catalog on the fly
            '--on-fly-isolate', os.path.join(path, catalog),
        ]

        dcf = get_dcf()
        for b in ('c-base', 'encryptionkey', 'compression', name):
            args.extend(['--batch', dcf[b],])

        if reference:
            # Reference previous backup archive/catalog
            args.extend(['--ref', reference])

        self.log_execution(args)

    def log_execution(self, args):
            self.print_f('executing command:\n' + quote_command(*args))
            return_code = subprocess.call(args,
                                        stdout = self.log,
                                        stderr = subprocess.STDOUT)
            self.print_f("{0} returned: {1:d}\n", args[0], return_code)

if __name__ == '__main__':
    session = BackupSession('/backup/dar')
    session.backup('home')
    session.backup('system')

