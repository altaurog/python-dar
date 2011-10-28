from socket import socket, AF_INET, SOCK_STREAM
import os
import sys
import SocketServer
import subprocess

class DarHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024).strip()
        if data == 'xform':
            # socket 1 - xform input
            xf_s = socket(AF_INET, SOCK_STREAM)
            xf_s.bind(('',0))
            addr, port = xf_s.getsockname()
            self.request.send(str(port))
            xf_s.listen(1)
            print('listing on socket 1 at {0}'.format(port))
            xf_conn, xf_remoteaddr = xf_s.accept()
            print('accepted on socket 1 from {0!r}'.format(xf_remoteaddr))
            xform_input = xf_conn.makefile('rb',0)

            # socket 2 - slave output
            so_s = socket(AF_INET, SOCK_STREAM)
            so_s.bind(('',0))
            addr, port = so_s.getsockname()
            self.request.send(str(port))
            so_s.listen(1)
            print('listing on socket 2 at {0}'.format(port))
            so_conn, so_remoteaddr = so_s.accept()
            print('accepted on socket 2 from {0!r}'.format(xf_remoteaddr))
            slave_output = so_conn.makefile('wb',0)

            # socket 3 - slave input
            si_s = socket(AF_INET, SOCK_STREAM)
            si_s.bind(('',0))
            addr, port = si_s.getsockname()
            self.request.send(str(port))
            si_s.listen(1)
            print('listing on socket 3 at {0}'.format(port))
            si_conn, si_remoteaddr = si_s.accept()
            print('accepted on socket 3 from {0!r}'.format(xf_remoteaddr))
            slave_input = si_conn.makefile('rb',0)

            print('spawning dar_xform')
            xf_proc = subprocess.Popen(
                    ['dar_xform', '-Q', '-s', '10k', '-', 'archives/diffbackup',],
                    stdin = xform_input,
                    stdout = subprocess.PIPE,
                    stderr = subprocess.PIPE,
            )

            print('spawning dar_slave')
            slave_proc = subprocess.Popen(
                    ['dar_slave', '-Q', 'archives/remotehost'],
                    stdin = slave_input,
                    stdout = slave_output,
                    stderr = subprocess.PIPE,
            )

            print('communicating with dar_slave')
            print('\n'.join(map(str, slave_proc.communicate())))
            print('dar_slave returned {0}'.format(slave_proc.returncode))

            print('communicating with dar_xform')
            print('\n'.join(map(str, xf_proc.communicate())))
            print('dar_slave returned {0}'.format(xf_proc.returncode))

            si_conn.close()
            xf_conn.close()
            so_conn.close()

            # self.request.send(str(return_code)) # <--- this was the problem'
        else:
            return_code = 'bad request'
            self.request.send(str(return_code))
        print('exiting handler')

server_address = ('localhost', 18010)
def server():
    server = SocketServer.TCPServer(server_address, DarHandler)
    print('listening')
    server.serve_forever()

def client():
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(('localhost', 18010))
    sock.send('xform')


    # socket 1 - xform input
    port = sock.recv(1024)
    xf_s = socket(AF_INET, SOCK_STREAM)
    xf_s.connect(('localhost', int(port)))
    print('connected to socket 1 at {0}'.format(port))
    dar_output = xf_s.makefile('wb',0)

    # socket 2 - slave output
    port = sock.recv(1024)
    so_s = socket(AF_INET, SOCK_STREAM)
    so_s.connect(('localhost', int(port)))
    print('connected to socket 2 at {0}'.format(port))
    slave_output = so_s.makefile('rb',0)

    # socket 3 - slave input
    port = sock.recv(1024)
    si_s = socket(AF_INET, SOCK_STREAM)
    si_s.connect(('localhost', int(port)))
    print('connected to socket 3 at {0}'.format(port))
    slave_input = si_s.makefile('wb', 0)

    try:
        os.unlink('toslave')
    except:
        pass
    os.mkfifo('toslave')

    print('spawning cat')
    cat_proc = subprocess.Popen(
        ['cat', 'toslave'], stdout=slave_input,
        stderr=subprocess.PIPE, stdin=subprocess.PIPE,
    )

    print('spawning dar')
    dar_proc = subprocess.Popen(
        ['dar', '-Q', '-B', 'config/test.dcf', '-c', '-', '-A', '-', '-o', 'toslave'],
        stdout = dar_output, stdin = slave_output,
        stderr = subprocess.PIPE
    )

    print('communicating with dar')
    print('\n'.join(map(str, dar_proc.communicate())))
    print('dar returned {0}'.format(dar_proc.returncode))

    print('communicating with cat')
    print('\n'.join(map(str, cat_proc.communicate())))
    print('cat returned {0}'.format(cat_proc.returncode))

    so_s.close()
    xf_s.close()
    si_s.close()

    # result = sock.recv(1024)     #<-------------- DON'T DO THIS "
    # print('received: ' + result) #<---/                         "
    sock.close()
    print('socket closed, exiting')

if __name__ == "__main__":
    if sys.argv[1].startswith('serv'):
        server()
    else:
        client()

