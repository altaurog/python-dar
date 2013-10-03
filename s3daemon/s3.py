import csv
import logging
import threading
import optparse
import os
import Queue
import re
import signal
from contextlib import closing

from boto.s3.connection import S3Connection
from boto.s3.multipart import MultiPartUpload
from boto.s3.key import Key

from .filesplit import FileSplit
from .http import Server

DEFAULT_NUM_WORKERS = 8
__version__ = '0.4'

def worker(task_queue, master_queue, loglevel=logging.WARNING):
    logger = logging.getLogger('worker')
    logger.setLevel(loglevel)
    logger.debug('entering worker thread')
    while True:
        s3obj, message = task_queue.get()
        if s3obj is None:
            logger.debug('exiting worker thread')
            break
        logger.debug('uploading %s to %r' % (message, s3obj))
        with closing(message.open()) as fobj:
            for n in range(3):
                try:
                    if isinstance(s3obj, MultiPartUpload):
                        fobj.seek(0)
                        s3obj.upload_part_from_file(fobj, fobj.i, size=fobj.chunk_size)
                    elif isinstance(s3obj, Key):
                        s3obj.set_contents_from_file(fobj, rewind=True)
                except Exception, e:
                    logger.warning('Got exception %r' % e)
                else:
                    master_queue.put(s3obj)

        
class UploadMaster(object):
    def __init__(self, bucketname, credentials_path,
                    transforms=[],
                    num_workers=DEFAULT_NUM_WORKERS,
                    loglevel=logging.WARNING):
        self.logger = logging.getLogger('master')
        self.logger.setLevel(loglevel)
        self.bucketname = bucketname
        with open(os.path.expanduser(credentials_path)) as f:
            rec = csv.DictReader(f).next()
            self.aws_key_id = rec['Access Key Id']
            self.aws_secret = rec['Secret Access Key']
        self.s3conn = S3Connection(self.aws_key_id, self.aws_secret)
        self.bucket = self.s3conn.get_bucket(self.bucketname, validate=False)
        self.transforms = transforms + [self.remove_initial_slash]
        self.num_workers = num_workers
        self.master_queue = Queue.Queue()
        self.threads = []
        self.task_queue = Queue.Queue()
        thread_args = (self.task_queue, self.master_queue, loglevel)
        for i in range(self.num_workers):
            t = threading.Thread(target=worker, args=thread_args)
            t.start()
            self.threads.append(t)
        self.multipart_uploads = {}

    def get_keyname(self, filepath):
        key_name = filepath
        for t in self.transforms:
            key_name = t(key_name)
        return key_name

    initial_slash_re = re.compile(r'^/+')
    def remove_initial_slash(self, key_name):
        return self.initial_slash_re.sub('', key_name)

    def run(self, shutdown_callback):
        while True:
            message = self.master_queue.get()
            if isinstance(message, basestring):
                self.logger.debug("got request %s" % message)
                self.upload(message)
            elif isinstance(message, MultiPartUpload):
                self.complete_upload(message)
                self.logger.info("completed upload: " + message.key_name)
            elif isinstance(message, Key):
                self.logger.info("completed upload: " + message.key_name)
            elif not message:
                self.logger.debug("got exit signal")
                # if message is False, http server is already shut down
                if message is None:
                    shutdown_callback()

    def upload(self, filepath):
        filesplit = FileSplit(filepath)
        key_name = self.get_keyname(filepath)
        self.logger.debug("uploading %s in %d parts" % 
                                        (filepath, filesplit.num_chunks))
        if filesplit.num_chunks > 1:
            s3obj = self.bucket.initiate_multipart_upload(key_name)
            self.multipart_uploads[key_name] = filesplit.num_chunks
        else:
            s3obj = Key(self.bucket, key_name)

        for split in filesplit:
            self.task_queue.put((s3obj, split))

    def complete_upload(self, mp):
        count = self.multipart_uploads.get(mp.key_name)
        if count is None:
            self.logger.warning("Unrecognized multipart upload: %r" % mp)
            return
        count -= 1
        if count == 0:
            del self.multipart_uploads[mp.key_name]
        else:
            self.multipart_uploads[mp.key_name] = count
            self.logger.debug("completing upload: %r" % mp)
            # todo: handle in worker thread
            mp.complete_upload()

    def exit(self):
        for t in self.threads:
            self.task_queue.put((None,''))
 
        self.logger.info("waiting for child processes")
        for t in self.threads:
            t.join()
 
        while True:
            try:
                name = self.task_queue.get_nowait()
                self.logger.warning("not uploaded: " + name)
            except Queue.Empty:
                break

def sig_handler(_, __):
    raise KeyboardInterrupt

def parse_options():
    option_parser = optparse.OptionParser(version='%prog ' + __version__)
    option_parser.add_option('--bucketname','-b')
    option_parser.add_option('--credentials_path', '-c')
    option_parser.add_option('--num_workers', '-n', type='int',
                                default=DEFAULT_NUM_WORKERS)
    option_parser.add_option('--address','-a', default='')
    option_parser.add_option('--port','-p', type='int', default=0)
    option_parser.add_option('--verbose','-v', action='count', default=0)
    option_parser.add_option('--quiet','-q', action='count', default=0)
    return option_parser.parse_args()

def main():
    logging.basicConfig()
    # signal.signal(signal.SIGTERM, sig_handler)
    try:
        options, arguments = parse_options()
        if arguments:
            os.chdir(arguments[0])

        loglevel = logging.WARNING - 10 * (options.verbose - options.quiet)

        upload = UploadMaster(bucketname = options.bucketname,
                            credentials_path = options.credentials_path, 
                            num_workers = options.num_workers,
                            loglevel = loglevel)

        http_server = Server(upload.master_queue.put,
                            (options.address, options.port))

        master_thread = threading.Thread(target=upload.run,
                                            args=(http_server.shutdown,))
        master_thread.start()
        http_server.run()
    except KeyboardInterrupt:
        # Server stopped by signal. worker doesn't need to call shutdown
        upload.master_queue.put(False)
    upload.exit()
    master_thread.join()

if __name__ == '__main__':
    main()

