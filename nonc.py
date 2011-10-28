from socket import socket, AF_INET, SOCK_STREAM
import os
import sys
import SocketServer
import subprocess

class DarHandler(SocketServer.BaseRequestHandler):
    def negotiate_additional_connection(self, count):
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('',0))
        addr, port = s.getsockname()
        self.request.send('{0:5d}'.format(port))
        s.listen(count)
        return s

    def handle(self):
        data = self.request.recv(5)
        if data == 'xform':
            s = self.negotiate_additional_connection(3)

            xform_input = s.accept()[0].makefile('rb',0)
            xf_proc = subprocess.Popen(
                    ['dar_xform', '-Q', '-s', '10k', '-', 'archives/diffbackup',],
                    stdin = xform_input,
                    stdout = subprocess.PIPE,
                    stderr = subprocess.PIPE,
            )

            slave_output = s.accept()[0].makefile('wb',0)
            slave_input = s.accept()[0].makefile('rb',0)
            slave_proc = subprocess.Popen(
                    ['dar_slave', '-Q', 'archives/remotehost'],
                    stdin = slave_input,
                    stdout = slave_output,
                    stderr = subprocess.PIPE,
            )

            print slave_proc.communicate()[1],
            print 'dar_slave returned {0}'.format(slave_proc.returncode)

            print '\n'.join(xf_proc.communicate()),
            print 'dar_xform returned {0}'.format(xf_proc.returncode)

            # self.request.send('OKBYE') This doesn't work
            s.close()
        else:
            return_code = 'bad request'
            self.request.send(str(return_code))
        print('exiting handler')

server_address = ('localhost', 18010)
def server():
    server = SocketServer.TCPServer(server_address, DarHandler)
    print('listening')
    server.serve_forever()

class ConnectionManager(object):
    def __init__(self, sock):
        self.port = int(sock.recv(5))

    def newfile(self, *args, **kwargs):
        s = socket(AF_INET, SOCK_STREAM)
        s.connect(('localhost', self.port))
        return s.makefile(*args, **kwargs)

def client():
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(('localhost', 18010))
    sock.send('xform')
    cm = ConnectionManager(sock)

    dar_output = cm.newfile('wb',0)
    slave_output = cm.newfile('rb',0)
    slave_input = cm.newfile('wb', 0)

    try:
        os.unlink('toslave')
    except:
        pass
    os.mkfifo('toslave')

    cat_proc = subprocess.Popen(
        ['cat', 'toslave'], stdout=slave_input,
        stderr=subprocess.PIPE, stdin=subprocess.PIPE,
    )

    dar_proc = subprocess.Popen(
        ['dar', '-Q', '-B', 'config/test.dcf', '-c', '-', '-A', '-', '-o', 'toslave'],
        stdout = dar_output, stdin = slave_output,
        stderr = subprocess.PIPE
    )

    print dar_proc.communicate()[1],
    print 'dar returned {0}'.format(dar_proc.returncode)

    print cat_proc.communicate()[1],
    print 'cat returned {0}'.format(cat_proc.returncode)

    # sock.recv(5)  This doesn't work
    sock.close()
    print('socket closed, exiting')

if __name__ == "__main__":
    if sys.argv[1].startswith('serv'):
        server()
    else:
        client()

