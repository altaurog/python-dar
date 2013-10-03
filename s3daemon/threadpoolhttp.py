import signal
import threading
import Queue

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

class Exit(int):
    """
    Sentinel sent on exit, also specifies whether or not to send
    a shutdown message back to the http server
    """

class ThreadPoolHTTPRequestHandler(BaseHTTPRequestHandler):
    def shutdown(self):
        self.queue.put(Exit(True))

def sig_handler(_, __):
    raise KeyboardInterrupt


class ThreadPoolHTTPServer(object):
    def __init__(self, taskmaster, worker,
                    worker_count=2,
                    address='',
                    port=0,
                    handler_base_class=ThreadPoolHTTPRequestHandler,):
        self.taskmaster = taskmaster
        self.worker = worker
        self.worker_count = worker_count
        self.master_queue = Queue.Queue()
        self.worker_queue = Queue.Queue()

        class HandlerClass(handler_base_class):
            queue = self.master_queue

        self.handler_class = HandlerClass

        addrport = (address, port)
        self.server = HTTPServer(addrport, self.handler_class)
        self.address = self.server.server_name
        self.port = self.server.server_port

    def exit_signals(self, *args):
        for s in args:
            signal.signal(s, sig_handler)

    def serve(self):
        self.start_threads()
        try:
            self.server.serve_forever()
        except KeyboardInterrupt:
            self.master_queue.put(Exit())
        self.join_threads()
        print("exiting")

    def start_threads(self):
        self._master = threading.Thread(target=self.master_thread)
        self._master.start()
        self._workers = []
        for i in range(self.worker_count):
            t = threading.Thread(target=self.worker_thread)
            t.start()
            self._workers.append(t)

    def join_threads(self):
        for t in self._workers:
            if t.is_alive():
                self.worker_queue.put(Exit)
        self.master_queue.join()
        self._master.join()
        self.worker_queue.join()
        while self._workers:
            self._workers.pop().join()

    def master_thread(self):
        taskmaster = self.taskmaster(self.worker_queue, self.master_queue)
        taskmaster.next()
        while True:
            message = self.master_queue.get()
            try:
                if isinstance(message, Exit):
                    if message:
                        self.shutdown()
                    break
                else:
                    taskmaster.send(message)
            except StopIteration:
                self.shutdown()
                break
            finally:
                self.master_queue.task_done()

    def worker_thread(self):
        worker = self.worker(self.master_queue)
        worker.next()
        while True:
            task = self.worker_queue.get()
            try:
                if task is Exit:
                    break
                else:
                    worker.send(task)
            finally:
                self.worker_queue.task_done()

    def shutdown(self):
        self.server.shutdown()


class SimpleHTTPHandler(ThreadPoolHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write('OK\n')
        if self.path == '/quit/':
            self.shutdown()
        else:
            self.queue.put(self.path)

def master_coroutine(worker_queue, _):
    while True:
        message = (yield)
        worker_queue.put(message)

def worker_coroutine(_):
    while True:
        message = (yield)
        print(threading.current_thread(), message)

def test():
    import sys
    port = int(sys.argv[1])
    server = ThreadPoolHTTPServer(
                        master_coroutine,
                        worker_coroutine,
                        handler_base_class=SimpleHTTPHandler,
                        port=port,
                )
    print("serving at %s:%d" % (server.address, server.port))
    server.exit_signals(signal.SIGTERM)
    server.serve()

if __name__ == '__main__':
    test()

