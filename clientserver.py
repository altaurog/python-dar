from socket import socket, AF_INET, SOCK_STREAM
import os, sys
import SocketServer
import subprocess

class DarHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        print('entering handler')
        data = self.request.recv(1024).strip()
        print('got: ' + data)
        if data == 'xform':
            cmd1 = 'nc -l 41201 | dar_slave archives/remotehost | nc -l 41202'
            print(cmd1)
            cmd2 = 'nc -l 41200 | dar_xform -s 10k - archives/diffbackup'
            print(cmd2)
            proc1 = subprocess.Popen(cmd1, shell=True)
            proc2 = subprocess.Popen(cmd2, shell=True)
            print('sending port number')
            self.request.send('41200')
            print('waiting')
            result = str(proc1.wait())
            print('nc-dar_slave-nc returned ' + result)
            result = str(proc2.wait())
            print('nc-dar_xform returned ' + result)
        else:
            result = 'bad request'
        self.request.send(result)
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
    cmd2 = 'nc -w3 localhost 41202 | dar -B config/test.dcf -A - -o toslave -c - | nc -w3 localhost ' + port
    print(cmd2)
    proc1 = subprocess.Popen(cmd1, shell=True)
    proc2 = subprocess.Popen(cmd2, shell=True)
    print('waiting')
    result2 = proc2.wait()
    result1 = proc1.wait()
    print('nc<fifo returned: ' + str(result1))
    print('nc-dar-nc returned: ' + str(result2))
    result = sock.recv(1024)
    print('received: ' + result)
    sock.close()
    print('socket closed, exiting')

if __name__ == "__main__":
    if sys.argv[1].startswith('serv'):
        server()
    else:
        client()

