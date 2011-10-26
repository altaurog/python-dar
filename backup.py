#!/usr/bin/env python
# requires python 2.6 - 3.x
from __future__ import print_function

import csv
import os
import subprocess
import socket
import sys

from datetime import datetime
from optparse import OptionParser
from warnings import warn

try:
    from shlex import quote # python 3
except ImportError:
    from pipes import quote # python 2

"""
TODO:
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

class SliceSet(str):
    def first_slice(self):
        return self + '.1.dar'

    def exists(self):
        return os.path.exists(self.first_slice())

class LocalHost(object):
    def __init__(self, config):
        self.config = config
        self.level = config.level
        self.level_manager = LevelManager(config.archive_dir)

    def backup(self, name, reference=None):
        host = socket.gethostname()
        path = checkpath(self.config.path, host, name)
        fullpath = lambda f: os.path.join(path, f)

        basename = '-'.join((host, name, self.config.sid))
        archive = SliceSet(fullpath(basename))
        catalog = SliceSet(fullpath('catalog-' + basename))

        args = [ 'dar', '--create', archive, '--on-fly-isolate', catalog, ]

        dcf = get_dcf()
        for b in ('c-base', 'encryptionkey', 'compression', name):
            args.extend(['--batch', dcf[b],])

        if self.level is not None:
            if reference is not None:
                warn("Backup level and reference specified; ignoring reference")
            reference = self.level_manager.get_ref_for_level(self.level, host, name)
            print("level: {0!r}\nref: {1!r}\n".format(self.level, reference))

        if reference:
            # Reference previous backup archive/catalog
            args.extend(['--ref', reference])

        log_execution(args, self.config.dry_run)

        if self.config.dry_run:
            return
        
        if self.level is not None and catalog.exists():
            self.level_manager.add(self.level, host, name, catalog)
            self.level_manager.dump()
        elif self.config.dry_run:
            warn("Catalog {0} not generated!", catalog.first_slice())

        if not archive.exists():
            raise RuntimeError("Archive not created")

        args = [ 'dar', '--test', archive, '--batch', dcf['encryptionkey'], ]
        if 0 != log_execution(args):
            raise RuntimeError("Archive failed integrity test")

def quote_command(cmd, *args):
    return ' ' + ' '.join([cmd] + map(quote, args))

class LevelsDialect(csv.excel):
    delimiter = ' '
    doublequote = False
    escapechar = '\\'
    skipinitialspace = True

class LevelManager(object):
    dialect = LevelsDialect()

    def __init__(self, path):
        self.dbfile = os.path.join(path, 'levels.db')
        print_f("writing {0}", self.dbfile)
        self.levels = {}
        if os.path.exists(self.dbfile):
            self.load()

    def hn_dict(self, host, name):
        return self.levels.setdefault((host, name), {})

    def get_ref_for_level(self, level, host, name):
        hn_dict = self.hn_dict(host, name)
        for i in xrange(level, 0, -1):
            try:
                return hn_dict[i - 1]
            except KeyError:
                pass

    def add(self, level, *args):
        hn_dict = self._add(level, *args)
        for entry in filter(lambda L: L > level, hn_dict.keys()):
            del hn_dict[entry]

    def load(self):
        with open(self.dbfile) as f:
            for record in csv.reader(f, self.dialect):
                self._add(*record)

    def dump(self):
        with open(self.dbfile, "w") as f:
            writer = csv.writer(f, self.dialect)
            for record in self:
                writer.writerow(record)

    def __iter__(self):
        for hn, hn_dict in self.levels.iteritems():
            host, name = hn
            for level in sorted(hn_dict.keys()):
                yield level, host, name, hn_dict[level]

    def _add(self, level, host, name, catalog):
        hn_dict = self.hn_dict(host, name)
        hn_dict[int(level)] = catalog
        return hn_dict

class BackupConfig(object):
    def __init__(self, args=None):
        self.parse_args(args)
        self.sid  = self.get_timestamp()
        self.path = checkpath(self.archive_dir, self.sid)
        self.set_logfile()

    def parse_args(self, args=None):
        parser = OptionParser()
        parser.add_option("-l", "--level", type="int", help="backup level")
        parser.add_option("-d", "--archive-dir", help="base directory in which archives are to be saved")
        parser.add_option("-n", "--dry-run", action="store_true", default=False,
                            help="don't actually do anything")
        parser.add_option("-F", "--time-format", default="%Y%m%d_%H%M",
                            help="format passed to strftime to determine archive dir and name")
        self.options, self.args = parser.parse_args(args)

    def get_timestamp(self):
        # timezone = pytz.timezone('Asia/Jerusalem') # don't waste time on this now
        now = datetime.now()
        return now.strftime(self.time_format)

    def set_logfile(self):
        logpath = os.path.join(self.path, 'log')
        if sys.stdout.isatty:
            # http://stackoverflow.com/questions/616645/#answer-651718
            # requires posix system (we'd need cygwin for dar anyway)
            sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
            tee = subprocess.Popen(["tee", logpath], stdin=subprocess.PIPE)
            os.dup2(tee.stdin.fileno(), sys.stdout.fileno())
            os.dup2(tee.stdin.fileno(), sys.stderr.fileno())
        else:
            sys.stdout = open(logpath, 'w', 0)

    def __getattr__(self, attr_name):
        return getattr(self.options, attr_name)

def print_f(template, *args, **kwargs):
    print(template.format(*args, **kwargs))

def log_execution(args, dry_run=False):
    print_f('executing command:\n' + quote_command(*args))
    if not dry_run:
        return_code = subprocess.call(args, stderr = subprocess.STDOUT)
        print_f("{0} returned: {1:d}\n", args[0], return_code)
        return return_code

def main():
    config = BackupConfig()
    session = LocalHost(config)
    for name in config.args:
        session.backup(name)
    

if __name__ == '__main__':
    sys.exit(main())


