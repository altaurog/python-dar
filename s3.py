from boto.s3 import connection
import csv
import logging
import multiprocessing
import os
import optparse
from contextlib import closing
from cStringIO import StringIO

MIN_CHUNK_SIZE = 5 * 2 ** 20
NUM_WORKERS = multiprocessing.cpu_count() * 2
logger=multiprocessing.log_to_stderr(logging.DEBUG)

class MPFile(object):
    def __init__(self, filepath):
        self.filepath = filepath
        self.filesize = os.stat(filepath).st_size
        self.num_chunks = self.filesize // MIN_CHUNK_SIZE
        self.chunk_size = self.filesize // self.num_chunks + 1

    def __iter__(self):
        with open(self.filepath) as f:
            for i in range(1, 1 + self.num_chunks):
                yield i, f.read(self.chunk_size)

def worker(queue, mp, num_chunks):
    while True:
        i, chunk_data = queue.get()
        if i is None:
            break
        logger.info('chunk %d of %d, %d bytes' % (i, num_chunks, len(chunk_data)))
        with closing(StringIO(chunk_data)) as chunk:
            for n in range(3):
                try:
                    chunk.seek(0)
                    mp.upload_part_from_file(chunk, i)
                    break
                except Exception, e:
                    logger.warning('Got exception %r' % e)
                    logger.warning('retrying chunk %d (%d)' % (i,n))
        

class Uploader(object):
    def __init__(self, bucketname, credentials_path):
        self.bucketname = bucketname
        with open(os.path.expanduser(credentials_path)) as f:
            rec = csv.DictReader(f).next()
            self.aws_key_id = rec['Access Key Id']
            self.aws_secret = rec['Secret Access Key']

    def put(self, filepath):
        logger.info("uploading %s" % filepath)
        filesize = os.stat(filepath).st_size
        if filesize <= MIN_CHUNK_SIZE:
            k = self.bucket.new_key(filepath)
            k.set_contents_from_filename(filepath)
        else:
            self.mpput(filepath)

    def mpput(self, filepath):
        queue = multiprocessing.Queue(NUM_WORKERS)
        mp = self.bucket.initiate_multipart_upload(filepath)
        file_chunks = MPFile(filepath)
        proc_args = (queue, mp, file_chunks.num_chunks)
        processes = [multiprocessing.Process(target=worker, args=proc_args)
                    for i in range(NUM_WORKERS)]
        for p in processes:
            p.start()

        for chunk in file_chunks:
            queue.put(chunk)

        for p in processes:
            queue.put((None,''))

        for p in processes:
            p.join()

        mpcomplete = mp.complete_upload()

    _conn = None
    @property
    def connection(self):
        if self._conn is None:
            self._conn = connection.S3Connection(self.aws_key_id, self.aws_secret)
        return self._conn

    _bucket = None
    @property
    def bucket(self):
        if self._bucket is None:
            c = self.connection
            self._bucket = c.get_bucket(self.bucketname, validate=False)
        return self._bucket

def main():
    option_parser = optparse.OptionParser()
    option_parser.add_option('--bucketname','-b')
    option_parser.add_option('--credentials_path', '-c')
    options, arguments = option_parser.parse_args()
    uploader = Uploader(options.bucketname, options.credentials_path)
    for f in arguments:
        uploader.put(f)

if __name__ == '__main__':
    main()

