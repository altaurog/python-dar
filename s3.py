import csv
import hashlib
import logging
import multiprocessing
import os
import optparse
import re
import time
from contextlib import closing
from cStringIO import StringIO

from boto.s3 import connection
from boto import exception

MIN_CHUNK_SIZE = 5 * 2 ** 20
NUM_WORKERS = multiprocessing.cpu_count() * 4
logger=multiprocessing.log_to_stderr(logging.INFO)

class MPFile(object):
    def __init__(self, filepath):
        self.filepath = filepath
        self.filesize = os.stat(filepath).st_size
        self.num_chunks = self.filesize // MIN_CHUNK_SIZE
        self.chunk_size = self.filesize // self.num_chunks + 1

    def __iter__(self):
        _md5 = hashlib.md5()
        with open(self.filepath) as f:
            for i in range(1, 1 + self.num_chunks):
                data = f.read(self.chunk_size)
                _md5.update(data)
                yield i, data
        self.md5 = _md5.hexdigest()

def worker(queue, mp, filepath, num_chunks):
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
                    logger.warning('retrying chunk %d/%d of file %s (%d)' % (i, num_chunks, filepath, n))
        

class Uploader(object):
    def __init__(self, bucketname, credentials_path, transforms=[]):
        self.bucketname = bucketname
        with open(os.path.expanduser(credentials_path)) as f:
            rec = csv.DictReader(f).next()
            self.aws_key_id = rec['Access Key Id']
            self.aws_secret = rec['Secret Access Key']
        self.transforms = transforms + [self.remove_initial_slash]

    def get_keyname(self, filepath):
        keyname = filepath
        for t in self.transforms:
            keyname = t(keyname)
        return keyname

    initial_slash_re = re.compile(r'^/+')
    def remove_initial_slash(self, keyname):
        return self.initial_slash_re.sub('', keyname)

    def put(self, filepath):
        logger.info("uploading %s" % filepath)
        filesize = os.stat(filepath).st_size
        if filesize <= MIN_CHUNK_SIZE:
            keyname = self.get_keyname(filepath)
            k = self.bucket.new_key(keyname)
            k.set_contents_from_filename(filepath)
        else:
            self.mpput(filepath)

    def mpput(self, filepath):
        keyname = self.get_keyname(filepath)
        queue = multiprocessing.Queue(NUM_WORKERS)
        mp = self.bucket.initiate_multipart_upload(keyname)
        file_chunks = MPFile(filepath)
        proc_args = (queue, mp, filepath, file_chunks.num_chunks)
        processes = [multiprocessing.Process(target=worker, args=proc_args)
                    for i in range(min(file_chunks.num_chunks, NUM_WORKERS))]
        for p in processes:
            p.start()

        for chunk in file_chunks:
            queue.put(chunk)

        for p in processes:
            queue.put((None,''))

        logger.info("waiting for child processes")
        for p in processes:
            p.join()

        logger.info("completing upload")
        mpcomplete = mp.complete_upload()
        time.sleep(5)
        logger.info("attempting key copy")
        try:
            k = self.bucket.get_key(keyname)
            logger.debug(k)
            newk = k.copy(self.bucketname, keyname) # copy key onto itself to get md5 etag
            logger.debug(newk)
            if newk.etag == '"%s"' % file_chunks.md5:
                logger.info("file %s etag matches: %s, %s" % (keyname, newk.etag, file_chunks.md5))
            else:
                logger.error("file %s etag didn't match: %s, %s" % (keyname, newk.etag, file_chunks.md5))
        except exception.S3ResponseError, e:
            logging.error("An exception occurred: %s" % e)

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

