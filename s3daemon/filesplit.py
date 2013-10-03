import os

class FileSplit(object):
    MIN_SPLIT_SIZE = 5 * 2 ** 20

    def __init__(self, filepath, min_split_size=MIN_SPLIT_SIZE):
        self.filepath = filepath
        self.filesize = os.stat(filepath).st_size
        if self.filesize < 2 * min_split_size:
            self.num_chunks = 1
            self.chunk_size = self.filesize
        else:
            self.num_chunks = self.filesize // min_split_size
            self.chunk_size = self.filesize // self.num_chunks + 1

    def __iter__(self):
        for i in range(self.num_chunks):
            yield Split(self.filepath, self.chunk_size, i)


class Split(object):
    def __init__(self, filepath, chunk_size, i):
        self.name = filepath
        self.chunk_size = chunk_size
        self.startpos = chunk_size * i
        self.i = i

    def open(self):
        self.fobj = open(self.name)
        self.fobj.seek(0, os.SEEK_END)
        self.endpos = min(self.fobj.tell(), self.startpos + self.chunk_size)
        self.size = self.endpos - self.startpos
        self.seek(0)
        return self

    def close(self):
        self.fobj.close()

    def read(self, size):
        bytes_ahead = self.size - self.tell()
        return self.fobj.read(min(size, bytes_ahead))

    def tell(self):
        return self.fobj.tell() - self.startpos

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            self.fobj.seek(self.startpos + offset)
        elif whence == os.SEEK_END:
            self.fobj.seek(self.endpos + offset)
        else: # SEEK_CUR
            self.fobj.seek(offset, whence)
