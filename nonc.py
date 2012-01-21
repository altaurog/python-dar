import calendar
import csv
import json
import os
import sys
import SocketServer
import subprocess

from datetime import date, datetime
from glob     import glob
from optparse import OptionParser
from socket   import socket, AF_INET, SOCK_STREAM, gethostname

class Subprocess(object):
    def __init__(self, *args, **kwargs):
        self.proc = subprocess.Popen(*args, **kwargs)
        self.name = args[0][0]

    def communicate(self):
        self.out, self.err = self.proc.communicate()
        if self.err:
            print(self.err)
        if self.out:
            print(self.out)
        self.ret = self.proc.returncode
        if self.ret:
            print('{0} returned {1}'.format(self.name, self.ret))

COMMAND_LENGTH = 6
command_format = lambda c: '{{0:{0}}}'.format(COMMAND_LENGTH).format(c)[:COMMAND_LENGTH]
def send_message(sock, command, data=None):
    sock.send(command_format(command))
    json_data = json.dumps(data)
    sock.send('{0:8d}'.format(len(json_data)))
    sock.send(json)

def receive_message(sock):
    command = sock.recv(5)
    data_len = int(sock.recv(8))
    data = json.loads(sock.recv(data_len))
    return command, data

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

    def slices(self):
        return glob(self + '.*.dar')

class LevelsDialect(csv.excel):
    delimiter = ' '
    doublequote = False
    escapechar = '\\'
    skipinitialspace = True

class LevelManager(object):
    dialect = LevelsDialect()

    def __init__(self, path):
        self.dbfile = os.path.join(path, 'levels.db')
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
        hn_dict[int(level)] = SliceSet(catalog)
        return hn_dict

def tower_of_hannoi(today=None):
    """
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
    today = today or date.today()
    if today.weekday() == calendar.SATURDAY and today.day < 8:
        return 0

    levels_dict = {
        calendar.SATURDAY   : 1,
        calendar.SUNDAY     : 3,
        calendar.MONDAY     : 2,
        calendar.TUESDAY    : 5,
        calendar.WEDNESDAY  : 4,
        calendar.THURSDAY   : 7,
        calendar.FRIDAY     : 6,
    }

    return levels_dict[today.weekday()]

class DarHandler(SocketServer.BaseRequestHandler):
    def receive_message(self):
        return receive_message(self.request)

    def send_message(self, *args, **kwargs):
        send_message(self.request, *args, **kwargs)

    def negotiate_additional_connection(self, count):
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('',0))
        addr, port = s.getsockname()
        self.request.send('{0:5d}'.format(port))
        s.listen(count)
        return s

    def handle(self):
        command, data = self.receive_message()
        s = self.negotiate_additional_connection(3)

        if command == 'backup':
            archive, reference = self.config.get_names(data)
            self.send_message('ok', dict(reference=reference))

            xform_input = s.accept()[0].makefile('rb',0)
            xf_proc = Subprocess(
                    ['dar_xform', '-Q'] + self.config.xf_args() + ['-', archive],
                    stdin = xform_input,
                    stdout = subprocess.PIPE,
                    stderr = subprocess.PIPE,
            )

            if reference:
                slave_output = s.accept()[0].makefile('wb',0)
                slave_input = s.accept()[0].makefile('rb',0)
                slave_proc = Subprocess(
                        ['dar_slave', '-Q', reference],
                        stdin = slave_input,
                        stdout = slave_output,
                        stderr = subprocess.PIPE,
                )
                slave_proc.communicate()

            xf_proc.communicate()

            s.close()
            print('{0} {1} {2[level]} {2[host]} {2[name]} {3}'.format(
                self.client_address[0], command, data, archive)
            )
        else:
            self.send_message('error')
            print('{0} {1} error'.format(self.client_address[0], command))

def server(config):
    address = config.server_address
    class Handler(DarHandler):
        config = config
    server = SocketServer.TCPServer(address, Handler)
    print('listening')
    server.serve_forever()

class ConnectionManager(object):
    def __init__(self, sock):
        self.port = int(sock.recv(5))
        self.connections = []

    def newfile(self, *args, **kwargs):
        s = socket(AF_INET, SOCK_STREAM)
        s.connect(('localhost', self.port))
        self.connections.append(s)
        return s.makefile(*args, **kwargs)

    def close_all(self):
        for s in self.connections:
            s.close()

def client(config):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(config.server_address)
    send_message(sock, 'backup', config.data_dict)
    command, data = receive_message(sock)
    if command != 'ok':
        return
    cm = ConnectionManager(sock)
    processes = []

    dar_output = cm.newfile('wb',0)
    slave_output = None

    if data['reference']:
        slave_output = cm.newfile('rb',0)
        slave_input = cm.newfile('wb', 0)

        try:
            os.unlink('toslave')
        except:
            pass
        os.mkfifo('toslave')

        processes.append(Subprocess(
            ['cat', 'toslave'], stdout=slave_input,
            stderr=subprocess.PIPE, stdin=subprocess.PIPE,
        ))

    processes = [Subprocess(
        ['dar', '-Q', '-c', '-'] + config.dar_args(data),
        stdin = slave_output or subprocess.PIPE,
        stderr = subprocess.PIPE,
        stdout = dar_output,
    )] + processes

    for p in processes:
        p.communicate()

    cm.close_all()

    sock.close()
    print('socket closed, exiting')

class Config(object):
    def __init__(self, args=None):
        self.parse_args(args)
        self.server_address = (self.host, self.port)

class ServerConfig(Config):
    def __init__(self, args=None):
        super(ServerConfig, self).__init__(args)
        self.sid = self.get_timestamp()
        self.level_manager = LevelManager(self.archive_dir)

    def parse_args(self, args=None):
        parser = OptionParser()
        parser.add_option("-d", "--archive-dir", help="base directory in which archives are to be saved")
        parser.add_option("-s", "--slice_size", help="slice size")
        parser.add_option("-h", "--host", help="host name", default='')
        parser.add_option("-p", "--port", help="port", type='int', default=41200)
        parser.add_option("-f", "--time-format", default="%Y%m%d_%H%M",
                            help="strftime format used to determine archive path")
        parser.add_option("--no-par2", action="store_true", default=False,
                            help="Don't create par2 redundancy files")
        parser.add_option("--dump-config", action="store_true")
        self.options, self.args = parser.parse_args(args)

    def get_timestamp(self):
        # timezone = pytz.timezone('Asia/Jerusalem') # don't waste time on this now
        now = datetime.now()
        return now.strftime(self.time_format)

    def get_names(self, data):
        archive = checkpath(self.archive_dir, self.sid, data['host'], data['name'],)
        reference = self.level_manager.get_ref_for_level(**data)
        if reference:
            reference = SliceSet(reference)
        return SliceSet(archive), reference

    def xf_args(self):
        return ['--slice', self.slice_size]

    def __getattr__(self, attr_name):
        return getattr(self.options, attr_name)

class ClientConfig(object):
    def parse_args(self, args=None):
        parser = OptionParser()
        parser.add_option("-h", "--host", help="host name", default='')
        parser.add_option("-p", "--port", help="port", type='int', default=41200)
        parser.add_option("-n", "--name", help="backup set name")
        parser.add_option("-l", "--level", type="int", help="backup level")
        parser.add_option("--toh", dest='level', store_value="toh",
                            help="use monthly/weekly tower of hannoi scheme")
        parser.add_option("--dump-config", action="store_true")
        self.options, self.args = parser.parse_args(args)

    def __init__(self, *args):
        super(ClientConfig, self).__init__(*args)
        self.data_dict = dict(host = gethostname(),
                            level = self.options.level,
                            name = self.options.name,)

    def dar_args(self, data):
        args = []
        dcf = get_dcf()
        for b in ('c-base', 'compression', self.options.name):
            args.extend(['--batch', dcf[b],])

        for a in data.get('dar_args', []):
            args.append(a)

        if data.get('reference'):
            args.extend(['-A','-','-o','toslave'])

        return args

if __name__ == "__main__":
    if sys.argv[1].startswith('serv'):
        server(ServerConfig())
    else:
        client(ClientConfig())

