from socket import socket, AF_INET, SOCK_STREAM
import sys
import SocketServer
import subprocess

class DarHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        print('entering handler')
        data = self.request.recv(1024).strip()
        print('got: ' + data)
        if data == 'xform':
            s = socket(AF_INET, SOCK_STREAM)
            s.bind(('',0))
            myaddr, myport = s.getsockname()
            print('bound new socket to {0}:{1}'.format(myaddr, myport))
            self.request.send(str(myport))
            s.listen(1)
            conn, remoteaddr = s.accept()
            print('accepted connection from {0}:{1}'.format(*remoteaddr))
            xform_input = conn.makefile('rb',0)
            return_code = subprocess.call(['dar_xform' '-s', '10k', '-', 'archives/sockbackup',], stdin=xform_input)
            print('dar_xform returned {0}'.format(return_code))
            conn.close()
        else:
            result = 'bad request'
        self.request.send(result)
        print('send result, exiting handler')

server_address = ('localhost', 18010)
def server():
    server = SocketServer.TCPServer(server_address, DarHandler)
    print('listening')
    server.serve_forever()

def client():
    sock = socket(AF_INET, SOCK_STREAM)
    print('connecting to server')
    sock.connect(('localhost', 18010))
    print('connected, sending request')
    sock.send('xform')
    print('waiting for response')
    port = sock.recv(1024)
    print('got: ' + port)
    s = socket(AF_INET, SOCK_STREAM)
    s.connect(('localhost', int(port)))
    print('connected to dar_xform port')
    dar_output = s.makefile('wb',0)
    return_code = subprocess.call(['dar', '-B', 'config/test.dcf', '-c', '-',], stdout=dar_output)
    print('dar returned {0}'.format(return_code))
    s.close()
    result = sock.recv(1024)
    print('received: ' + result)
    sock.close()
    print('socket closed, exiting')

if __name__ == "__main__":
    if sys.argv[1].startswith('serv'):
        server()
    else:
        client()

