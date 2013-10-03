import os

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

def get_server(callback, address=('',0)):
    class Handler(BaseHTTPRequestHandler):
        def do_QUIT(self):
            callback(None)
            self.send_response(200)
            self.end_headers()
            self.wfile.write('OK\n')

        def do_GET(self):
            if not callback(self.path[1:]):
                self.send_error(404)
            else:
                self.send_response(200)
                self.end_headers()
                self.wfile.write('OK\n')

        error_message_format = "%(message)s"
        error_content_type = "text/plain"

    return HTTPServer(address, Handler)


class Server(object):
    def __init__(self, callback, address=None,):
        self.callback = callback
        self.address = address

    def run(self):
        args = filter(None, (self.handle_http_request, self.address))
        self.server = get_server(*args)
        addrport = (self.server.server_name, self.server.server_port)
        print "listening on %s:%s" % addrport
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()

    def handle_http_request(self, request):
        if request is None:
            self.callback(None)
            return

        try:
            if os.access(request, os.R_OK):
                self.callback(request)
                return True
        except os.error:
            # file doesn't exist or inaccessible
            pass
        return False


