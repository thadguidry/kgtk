"""Input stream wrappers for automatic compression detection and header capture.

These classes read in large chunks optimized for speed intended to serve as
input streams to the STELLA-based validator.
"""

import os
import sys
import io
import gzip, bz2, lz4.frame, lzma
import multiprocessing as mp

from kgtk.exceptions import KGTKException


# NOTES:
# - we need to support two modes:
#   (1) one when called from within python where we run a `StreamReaderProcess' in the background
#   (2) one where we simply buffer the initial header line and bytes to determine the compression
#       scheme, but then do the compression in a piped sh process directly
# - TO DO: finish handled compression schemes in `get_stream_data_type' and the various streams


DEFAULT_ENCODING = 'utf_8'

file_type_table = {
    'gzip':  {'cat': 'zcat',   'compress': 'gzip',  'open': gzip.open, 'ext': ('.gz', '.gzip')},
    'bzip2': {'cat': 'bzcat',  'compress': 'bzip2', 'open': bz2.open,  'ext': ('.bz2', '.bzip2')},
    'xz':    {'cat': 'xzcat',  'compress': 'xz',    'open': lzma.open, 'ext': ('.xz', '.lzma')},
    'lz4':   {'cat': 'lz4cat', 'compress': 'lz4',   'open': lz4.frame.open,  'ext': ('.lz4')},
    'text':  {'cat': 'cat', 'open': open, 'ext': ('.tsv', '.csv')},
}

def lookup_file_type_info(file):
    for ftype, info in file_type_table.items():
        for ext in info.get('ext', ()):
            if file.endswith(ext):
                return ftype, info
    return None, None

def get_io_buffer(data):
    """Return an IO buffer appropriate for `data'.
    """
    if isinstance(data, bytes):
        return io.BytesIO(data)
    elif isinstance(data, str):
        return io.StringIO(data, newline=None)
    else:
        raise KGTKException('get_data_buffer: unhandled data type: %s' % str(type(data)))
                            
def get_stream_data_type(header_data):
    """Determine which type of compression was used on `header_data' if any.
    """
    if isinstance(header_data, bytes):
        try:
            gzip.GzipFile(fileobj=get_io_buffer(header_data), mode='rb').peek(1)
            return 'gzip'
        except:
            pass
        try:
            bz2.BZ2File(get_io_buffer(header_data), mode='rb').peek(1)
            return 'bzip2'
        except:
            pass
        # TO DO: handle other types here
    return 'text'


class StreamReaderProcess(mp.Process):
    """Read from a stream in a separate process and copy the data read to the process queue.
    Supplied streams can be byte or string/text streams, and returned values will be of the right type.
    Iteration here generates chunks of data (not lines) for efficiency - which is non-standard.
    This is generally useful for things like decompression streams that take some extra compute time.
    This is a variant of Craig's GunzipProcess that uses a more efficient buffer size.
    This works well from within python but doesn't seem to work efficiently as an `sh' input stream.
    """

    # this buffers 64M bytes or chars until it blocks:
    DEFAULT_QUEUE_SIZE  = 2 ** 6
    DEFAULT_BUFFER_SIZE = 2 ** 20

    def __init__(self,  stream, queue=None, buffer_size=None, autostart=True):
        super().__init__()
        self.stream = stream
        self.queue = queue or mp.Queue(self.DEFAULT_QUEUE_SIZE)
        self.buffer_size = buffer_size or self.DEFAULT_BUFFER_SIZE
        self.autostart = autostart
        self.buffer = None

    def run(self):
        while True:
            chunk = self.stream.read(self.buffer_size)
            # ensure at least one chunk even on an empty stream:
            self.queue.put(chunk)
            if len(chunk) == 0:
                break
        # plug the queue:
        self.queue.put(None)

    def _get_buffer(self):
        buffer = self.buffer
        if buffer is None:
            if self.autostart:
                self.start()
            # we are guaranteed at least one (possibly empty) chunk:
            chunk = self.queue.get()
            if isinstance(chunk, bytes):
                buffer = io.BytesIO(chunk)
            else:
                buffer = io.StringIO(chunk)
            self.buffer = buffer
        return buffer

    def read(self, size=-1):
        buffer = self._get_buffer()
        size = size < 0 and sys.maxsize or size
        data = buffer.read(size)
        nread = len(data)
        if nread < size and self.queue is not None:
            buflen = buffer.tell()
            buffer.seek(0)
            buffer.write(data)
            while nread < size:
                chunk = self.queue.get()
                if chunk is None:
                    self.queue = None
                    break
                buffer.write(chunk)
                nread += len(chunk)
            buffer.seek(0)
            if nread < buflen:
                buffer.truncate(nread)
            return buffer.read(size)
        else:
            return data

    def __iter__(self):
        return self
    
    def __next__(self):
        # now implemented via read for uniform API:
        data = self.read(self.buffer_size)
        if len(data) == 0 and self.queue is None:
            raise StopIteration
        else:
            return data

    def fileno(self):
        """Needed so `sh' doesn't think this is a file stream.
        """
        raise io.UnsupportedOperation()

    def close(self):
        self.queue = None
        self.stream.close()
        self.kill()


class DuoReader(io.BufferedReader):
    """Utility that generates the concatenation of `stream1' and `stream2' which are assumed to
    be of the same bytes or string type.  We only handle two streams here for simplicity, additional
    streams can be added through composition.
    """
    
    def __init__(self, stream1, stream2):
        self.stream = stream1
        self.stream2 = stream2

    def advance_streams(self):
        self.stream = self.stream2
        self.stream2 = None
        
    def read(self, size=-1):
        if size == -1:
            if self.stream2 is None:
                return self.stream.read()
            else:
                data = self.stream.read()
                self.advance_streams()
                return data + self.stream.read()
        else:
            data = self.stream.read(size)
            if len(data) < size and self.stream2 is not None:
                self.advance_streams()
                data += self.stream.read(size - len(data))
            return data

class AutoDecompressionReader(io.BufferedReader):
    """Utility that automatically decompresses a given stream if necessary.
    Handles both byte and text streams, but for compressed streams to be
    recognized properly, they need to have been opened in `rb' mode.
    """
    # TO DO: this should have an option that just returns the file type but doesn't decompress
    
    def __init__(self, raw, bg=False, buffer_size=io.DEFAULT_BUFFER_SIZE):
        super().__init__(raw=raw, buffer_size=buffer_size)
        self.stream = raw
        self.base_stream = raw
        self.buffer_size = buffer_size
        self.background = bg
        self.setup()

    def setup(self):
        # we don't use the supplied buffer size which might be too small:
        data = self.stream.read(io.DEFAULT_BUFFER_SIZE)
        if len(data) == 0:
            return
        stype = get_stream_data_type(data)
        header_stream = get_io_buffer(data)
        mode = isinstance(data, str) and 'r' or 'rb'
        if stype == 'gzip':
            self.stream = gzip.GzipFile(filename='', fileobj=DuoReader(header_stream, self.stream), mode=mode)
            if self.background:
                self.stream = StreamReaderProcess(self.stream)
        elif stype == 'bzip2':
            self.stream = bz2.BZ2File(DuoReader(header_stream, self.stream), mode=mode)
            if self.background:
                self.stream = StreamReaderProcess(self.stream)
        elif stype == 'text':
            self.stream = DuoReader(header_stream, self.stream)

    def read(self, size=-1):
        return self.stream.read(size)

    def __iter__(self):
        return self

    def __next__(self):
        data = self.read(self.buffer_size)
        if len(data) == 0:
            raise StopIteration()
        else:
            return data

    def fileno(self):
        """Needed so `sh' doesn't think this is a file stream.
        """
        raise io.UnsupportedOperation()

    def close(self):
        self.base_stream.close()

        
class HeaderStreamReader(AutoDecompressionReader):
    def __init__(self, raw, buffer_size=io.DEFAULT_BUFFER_SIZE, bg=False):
        super().__init__(raw=raw, bg=bg, buffer_size=buffer_size)
        self.header = None

    def get_header(self, preserve=False):
        stream_type = str
        # text buffer with universal newline mode:
        buffer = io.StringIO(newline=None)
        buflen = 0
        for chunk in self:
            if isinstance(chunk, bytes):
                stream_type = bytes
                chunk = chunk.decode(DEFAULT_ENCODING)
            buffer.seek(buflen)
            buffer.write(chunk)
            buflen = buffer.tell()
            buffer.seek(0)
            self.header = buffer.readline()
            # universal newline mode translates into '\n':
            if self.header.endswith('\n'):
                self.header = self.header.rstrip()
                break
        if preserve:
            header_data = buffer.getvalue()
        else:
            header_data = buffer.read()
        if stream_type == bytes:
            header_data = header_data.encode(DEFAULT_ENCODING)
        self.stream = DuoReader(get_io_buffer(header_data), self.stream)
        return self.header


# Test drivers:

def stream_reader_test1(gzfile):
    count = 0
    proc = StreamReaderProcess(gzip.open(gzfile, mode='rb'))
    for chunk in proc:
        count += len(chunk)
    proc.close()
    return count, proc

def stream_reader_test2(gzfile):
    count = 0
    proc = StreamReaderProcess(gzip.open(gzfile, mode='rb'))
    while True:
        data = proc.read(2**16)
        dlen = len(data)
        if dlen <= 0:
            break
        count += dlen
    proc.close()
    return count, proc

"""
# this is about 26x faster than the original GunzipProcess, also a tiny bit faster than `zcat file | wc -c':
>>> with Timer() as t:
...     sr.stream_reader_test1('/data/kgtk/wikidata/run1/nodes-v2.csv.gz')
... 
(8918052340, <StreamReaderProcess(StreamReaderProcess-6, started)>)
>>> t.elapsed
38.38519595935941

>>> with Timer() as t:
...     sr.stream_reader_test2('/data/kgtk/wikidata/run1/nodes-v2.csv.gz')
... 
(8918052340, <StreamReaderProcess(StreamReaderProcess-7, started)>)
>>> t.elapsed
38.80276162317023
"""

"""
# this seems to work, but we are a tiny bit slower than doing the compression in the shell,
# somehow we never get beyond a total of 100% between the two python+sort processes:

>>> reader = sr.HeaderStreamReader(open('/data/kgtk/wikidata/run1/nodes-v2.csv.gz', mode='rb'), buffer_size=2 ** 18)
>>> print(reader.get_header())
id	label	type	descriptions	aliases	document_id
>>> 
>>> with Timer() as t:
...     sh.sort(_in=reader, _in_bufsize=2 ** 18, _bg=True, _out='/tmp/nodes-v2.sorted.csv')
... 
>>> reader.close()
>>> t.elapsed
310.13663909072056

>>> sh.ls('-l', '/tmp/nodes-v2.sorted.csv')
-rw-r--r-- 1 hans isdstaff 8918050194 May 27 21:38 /tmp/nodes-v2.sorted.csv

>>> sh.head('-5', '/tmp/nodes-v2.sorted.csv')
P1000	'record held'@en	property	'notable record achieved by a person or entity, include qualifiers for dates held'@en		wikidata-20200203
P1001	'applies to jurisdiction'@en	property	'the item (an institution, law, public office ...) or statement belongs to or has power over or applies to the value (a territorial jurisdiction: a country, state, municipality, ...)'@en	'of jurisdiction'@en|'linked to jurisdiction'@en|'belongs to jurisdiction'@en|'jurisdiction'@en|'country of jurisdiction'@en|'valid in jurisdiction'@en|'applies to territorial jurisdiction'@en|'applied to jurisdiction'@en	wikidata-20200203
P1002	'engine configuration'@en	property	'configuration of an engine's cylinders'@en	'configuration of engine cylinders'@en	wikidata-20200203
P1003	'NLR ID (Romania)'@en	property	'identifier for authority control used at the National Library of Romania'@en	'National Library of Romania ID'@en|'NLR ID'@en	wikidata-20200203
P1004	'MusicBrainz place ID'@en	property	'Identifier for a place in the MusicBrainz open music encyclopedia'@en	'place MBID'@en|'MB place id'@en|'place ID'@en|'MBID place'@en|'MBP'@en	wikidata-20200203
>>> 
"""

def header_reader_test1(gzip_file, out_file):
    reader_chunk_size = 2 ** 18
    writer_chunk_size = 2 ** 20
    with HeaderStreamReader(open(gzip_file, mode='rb'), buffer_size=reader_chunk_size, bg=True) as reader:
        # note that the default compression level is 9, we use 6 here to match gzip default:
        with gzip.open(out_file, mode='wb', compresslevel=6) as writer:
            reader.get_header()
            while True:
                chunk = reader.read(writer_chunk_size)
                res = writer.write(chunk)
                if len(chunk) == 0:
                    break

"""
# This performs very similar to the Unix command-line equivalent:
>>> with Timer() as t:
...     sr.header_reader_test1('/data/kgtk/wikidata/run1/nodes-v2.csv.gz', '/tmp/nodes-v2.sans.header.gz')
>>> t.elapsed
245.17786729894578
>>> t.elapsed / 60.0
4.086297788315763

> date; zcat /data/kgtk/wikidata/run1/nodes-v2.csv.gz | gzip -c > nodes-v2.sans.header-2.gz; date
Thu 28 May 2020 09:38:28 AM PDT
Thu 28 May 2020 09:42:40 AM PDT

> ls -l nodes-v2.sans.header*
-rw-r--r-- 1 hans isdstaff 2079495807 May 28 10:17 nodes-v2.sans.header.gz
-rw-r--r-- 1 hans isdstaff 2080489142 May 28 09:42 nodes-v2.sans.header-2.gz
"""
