from socket import socket, AF_INET, SOCK_STREAM
import os, sys
import SocketServer
from subprocess import Popen, PIPE

class DarHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        print('entering handler')
        data = self.request.recv(1024).strip()
        print('got: ' + data)
        if data == 'xform':
            cmd1 = 'nc -dl 41201 | dar_slave -Q archives/remotehost | nc -l 41202'
            print(cmd1)
            cmd2 = 'nc -dl 41200 | dar_xform -Q -s 10k - archives/diffbackup'
            print(cmd2)
            proc1 = Popen(cmd1, shell=True, stderr=PIPE)
            proc2 = Popen(cmd2, shell=True, stderr=PIPE)
            print('sending port number')
            self.request.send('41200')
            print('waiting')
            out1, err1 = proc1.communicate()
            print('nc-dar_slave-nc stdout: ' + str(out1))
            print('nc-dar_slave-nc stderr: ' + str(err1))
            print('nc-dar_slave-nc returned ' + str(proc1.returncode))
            out2, err2 = proc2.communicate()
            print('nc-dar_xform stdout: ' + str(out2))
            print('nc-dar_xform stderr: ' + str(err2))
            print('nc-dar_xform returned ' + str(proc2.returncode))
        else:
            self.request.send('bad request')
        print('send result, exiting handler')

myaddress = ('localhost', 18010)
def server():
    server = SocketServer.TCPServer(myaddress, DarHandler)
    print('listening')
    server.serve_forever()

def client():
    sock = socket(AF_INET, SOCK_STREAM)
    print('connecting')
    sock.connect(('localhost', 18010))
    print('connected, sending request')
    sock.send('xform')
    print('waiting for response')
    port = sock.recv(1024)
    print('got: ' + port)
    try:
        os.unlink('toslave')
    except:
        pass
    os.mkfifo('toslave')
    cmd1 = 'nc -w3 localhost 41201 < toslave'
    cmd2 = 'nc -d -w3 localhost 41202 | dar -Q -B config/test.dcf -A - -o toslave -c - | nc -w3 localhost ' + port
    print(cmd2)
    proc1 = Popen(cmd1, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    proc2 = Popen(cmd2, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    print('waiting')
    out2, err2 = proc2.communicate()
    print('nc-dar-nc stdout: ' + str(out2))
    print('nc-dar-nc stderr: ' + str(err2))
    print('nc-dar-nc returned ' + str(proc2.returncode))
    out1, err1 = proc1.communicate()
    print('nc<fifo stdout: ' + str(out1))
    print('nc<fifo stderr: ' + str(err1))
    print('nc<fifo returned ' + str(proc1.returncode))
    sock.close()
    print('socket closed, exiting')

if __name__ == "__main__":
    if sys.argv[1].startswith('serv'):
        server()
    else:
        client()

