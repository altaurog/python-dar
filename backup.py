#!/usr/bin/env python
import os
import socket

from datetime import datetime

from iterpipes import call, cmd


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
    don't use iterpipes

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

def make_command(command, args=[], logfile=None):
    args = [command] + args
    template = ' '.join(['{}'] * len(args))
    if logfile:
        template += ' > ' + logfile
    return cmd(template, *args)

def get_dcf():
    confdir = os.path.join(os.path.dirname(__file__), 'config')
    dcf_dict = {}
    for name in os.listdir(confdir):
        if name.endswith('.dcf'):
            fullpath = os.path.join(confdir, name)
            if os.path.isfile(fullpath):
                dcf_dict[name[:-4]] = fullpath

    return dcf_dict

def backup(batch, reference=None):
    timestamp_str = get_timestamp()
    kwargs = dict(ts=timestamp_str, b=batch, h=socket.gethostname())
    path = '/backup/dar/{ts}/{h}-{b}/'.format(**kwargs)
    if not os.path.exists(path):
        os.makedirs(path)

    basename = '{h}-{b}-{ts}'.format(**kwargs)
    catalog = '{0}-catalog'.format(basename)
    logfile = '{0}-log'.format(basename)

    args = [
        # create an archive
        '--create', os.path.join(path, basename),

        # create a isolated catalog on the fly
        '--on-fly-isolate', os.path.join(path, catalog),
    ]

    dcf = get_dcf()
    for b in ('c-base', 'encryptionkey', 'compression', batch):
        args.extend(['--batch', dcf[b],])

    if reference:
        # Reference previous backup archive/catalog
        args.extend(['--ref', reference])

    call(make_command('dar', args, logfile=os.path.join(path, logfile)))


if __name__ == '__main__':
    backup('home')
    backup('system')

