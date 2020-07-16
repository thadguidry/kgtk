############################# BEGIN LICENSE BLOCK ############################
#                                                                            #
# Copyright (C) 2020                                                         #
# UNIVERSITY OF SOUTHERN CALIFORNIA, INFORMATION SCIENCES INSTITUTE          #
# 4676 Admiralty Way, Marina Del Rey, California 90292, U.S.A.               #
#                                                                            #
# Permission is hereby granted, free of charge, to any person obtaining      #
# a copy of this software and associated documentation files (the            #
# "Software"), to deal in the Software without restriction, including        #
# without limitation the rights to use, copy, modify, merge, publish,        #
# distribute, sublicense, and/or sell copies of the Software, and to         #
# permit persons to whom the Software is furnished to do so, subject to      #
# the following conditions:                                                  #
#                                                                            #
# The above copyright notice and this permission notice shall be             #
# included in all copies or substantial portions of the Software.            #
#                                                                            #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,            #
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF         #
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND                      #
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE     #
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION     #
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION      #
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.            #
#                                                                            #
############################# END LICENSE BLOCK ##############################


import sys
import os.path

import stellapi as spi
from   stellapi.api import _ffi
import stellapi.stella.pdef as pstella

import kgtk.io.smartreader as sr
from   kgtk.exceptions import KGTKException


### System startup

apiModule = sys.modules['kgtk.stetools']

# build the library if necessary:
_reader_cc = os.path.join(os.path.dirname(__file__), 'native', 'cpp', 'kgtk', 'reader.cc')
_libkgtk_o = os.path.join(os.path.dirname(__file__), '_libkgtk.o')
if not os.path.exists(_libkgtk_o) or not os.path.exists(_reader_cc) or \
   os.path.getmtime(_reader_cc) > os.path.getmtime(_libkgtk_o):
    import kgtk.stetools.build_libkgtk as build_libkgtk
    build_libkgtk.ffibuilder.compile(verbose=False)

spi.loadSystem('kgtk', 'kgtk.stetools._libkgtk')
spi.defineStellaModuleNamespace('/KGTK')
pkgtk = spi.kgtk.pdef


### STELLA definitions

spi.defclass('stella/string-buffer')
spi.defslot('stella/string-buffer.fill-pointer')
spi.defmethod('stella/string-buffer.the-string')

@pkgtk
class _KgtkValidationIterator(spi.ProxyClass):
    _stellaTypePath = "/KGTK/@KGTK-VALIDATION-ITERATOR"

spi.defslot('kgtk/kgtk-validation-iterator.input-stream')
spi.defslot('kgtk/kgtk-validation-iterator.log-stream')
spi.defslot('kgtk/kgtk-validation-iterator.invalid-stream')
spi.defslot('kgtk/kgtk-validation-iterator.suppress-header?')
spi.defslot('kgtk/kgtk-validation-iterator.invalid-value-action')
spi.defslot('kgtk/kgtk-validation-iterator.error-limit')
spi.defslot('kgtk/kgtk-validation-iterator.chunk-size')
spi.defslot('kgtk/kgtk-validation-iterator.field-separator')
spi.defslot('kgtk/kgtk-validation-iterator.row-separator')
spi.defslot('kgtk/kgtk-validation-iterator.header-columns')
spi.defslot('kgtk/kgtk-validation-iterator.line-number')
spi.defslot('kgtk/kgtk-validation-iterator.value')

spi.defun('kgtk/allocate-kgtk-validation-iterator', file=None, logfile=None, invalidfile=None)
spi.defmethod('kgtk/kgtk-validation-iterator.number-of-columns')
spi.defmethod('kgtk/kgtk-validation-iterator.next?')
spi.defmethod('kgtk/kgtk-validation-iterator.close')

spi.defclass('kgtk/callback-input-stream')
spi.defconstructor('kgtk/callback-input-stream')
spi.defslot('kgtk/callback-input-stream.python-stream')
spi.defslot('kgtk/callback-input-stream.python-reader')
spi.defslot('kgtk/callback-input-stream.buffer-size')
spi.defslot('kgtk/callback-input-stream.debug-stream')
spi.defslot('kgtk/callback-input-stream.buffer')
spi.defmethod('kgtk/callback-input-stream.initialize-object')


### API definitions

DEFAULT_BUFFER_SIZE = 2 ** 20
DEFAULT_ERROR_LIMIT = 1000
DEFAULT_CHUNK_SIZE  = 100000
DEFAULT_INVALID_VALUE_ACTION = 'pass'

@_ffi.callback("int(void*, char*, int)")
def callback_stream_reader(pystream_ptr, buffer, size):
    """Read `size' bytes from the Python stream identified by `pystream_ptr' into `buffer'
    and return the actual number of bytes read (0 means EOF).
    """
    # TO DO: consider supporting/using read_into instead of read
    #print('>>> callback_stream_reader: ', pystream_ptr)
    pystream = spi.getPythonObjectFromStellaPointer(pystream_ptr)
    if hasattr(pystream, 'encoding'): # is this a sufficient test?
        data = pystream.read(max(size // 2, 1))
    else:
        data = pystream.read(size)
    if isinstance(data, str):
        data = data.encode(sr.DEFAULT_ENCODING)
    datalen = len(data)
    if datalen > size or datalen < 0:
        raise KGTKException('INTERNAL ERROR: callback_stream_reader: buffer overflow, size=%d, datalen=%d\n' % (size, datalen))
    # this simply copies a Python char* `data' into a preallocated STELLA char* `buffer':
    _ffi.memmove(buffer, data, datalen)
    #print('>>> callback_stream_reader: ', pystream_ptr, buffer, size, datalen)
    return datalen


# new-style callbacks (we need to switch to those eventually to avoid security exceptions):
from kgtk.stetools._libkgtk import ffi, lib

@ffi.def_extern()
def new_callback_stream_reader(pystream_ptr, buffer, size):
    """Read `size' bytes from the Python stream identified by `pystream_ptr' into `buffer'
    and return the actual number of bytes read (0 means EOF).
    """
    # TO DO: consider supporting/using read_into instead of read
    #print('>>> new_callback_stream_reader: ', pystream_ptr)
    pystream = spi.getPythonObjectFromStellaPointer(pystream_ptr)
    if hasattr(pystream, 'encoding'): # is this a sufficient test?
        data = pystream.read(max(size // 2, 1))
    else:
        data = pystream.read(size)
    if isinstance(data, str):
        data = data.encode(sr.DEFAULT_ENCODING)
    datalen = len(data)
    if datalen > size or datalen < 0:
        raise KGTKException('INTERNAL ERROR: callback_stream_reader: buffer overflow, size=%d, datalen=%d\n' % (size, datalen))
    # this simply copies a Python char* `data' into a preallocated STELLA char* `buffer':
    ffi.memmove(buffer, data, datalen)
    return datalen

def allocate_callback_stream(pystream=None, bufsize=DEFAULT_BUFFER_SIZE):
    cbstream = pkgtk.CallbackInputStream()
    cbstream.bufferSize = bufsize
    if pystream is not None:
        pystreamPtr = spi.getPythonObjectStellaPointer(pystream)
        cbstream.pythonStream = pystreamPtr
        #print('>>> allocate_callback_stream: pystream_ptr=', repr(pystreamPtr), pystreamPtr._stellaObject)
    cbstream.pythonReader = callback_stream_reader
    #cbstream.pythonReader = lib.new_callback_stream_reader
    cbstream.initializeObject()
    return cbstream


def prepare_validation_input_stream(file, smart=True, bg=False):
    if file is None:
        return file
    elif isinstance(file, str):
        ftype, info = sr.lookup_file_type_info(file)
        if ftype == 'text':
            return file
        elif info is not None:
            stream = info['open'](file, mode='rb')
            if bg:
                stream = sr.StreamReaderProcess(stream)
            return stream
    else:
        if smart:
            bg = False # FIXME: causes breakage, slowish on uncompressed streams
            return sr.AutoDecompressionReader(file, bg=bg, buffer_size=DEFAULT_BUFFER_SIZE)
        else:
            return file
    KGTKException("prepare_validation_input_stream: don't know how to handle input of type %s" % type(file))

def allocate_validation_iterator(file=sys.stdin, log_file=sys.stderr, invalid_file=None, invalid_value_action=None,
                                 error_limit=None, chunk_size=None, smart=True, bg=False, _expert=False, _debug=False):
    if (file == sys.stdin or file == sys.stdin.buffer) and not smart:
        file = None
    else:
        file = prepare_validation_input_stream(file, smart=smart, bg=bg)
    if log_file == sys.stderr or log_file == sys.stderr.buffer or log_file is None:
        log_file = None
    elif not isinstance(log_file, str):
        # TO DO: to accept streams here, we need to implement a CallbackOutputStream:
        raise KGTKException('allocate_validation_iterator: can only handle named log files at the moment')
    if not isinstance(invalid_file, str) and invalid_file is not None:
        raise KGTKException('allocate_validation_iterator: can only handle named invalid line files at the moment')
    invalid_value_action = (invalid_file is not None and 'exclude-line') or invalid_value_action or DEFAULT_INVALID_VALUE_ACTION

    if isinstance(file, str):
        iter = pkgtk.allocateKgtkValidationIterator(file=file, logfile=log_file, invalidfile=invalid_file)
    elif file is not None:
        iter = pkgtk.allocateKgtkValidationIterator(logfile=log_file, invalidfile=invalid_file)
        iter.inputStream = allocate_callback_stream(file, bufsize=DEFAULT_BUFFER_SIZE)
    else:
        iter = pkgtk.allocateKgtkValidationIterator(logfile=log_file, invalidfile=invalid_file)

    iter.invalidValueAction = pstella.internKeyword(invalid_value_action)
    iter.skipHeaderP = False
    iter.errorLimit = (error_limit is None and DEFAULT_ERROR_LIMIT) or error_limit
    iter.chunkSize = chunk_size or DEFAULT_CHUNK_SIZE
    return iter

def close_validation_iterator(iter):
    # we can't easily build this into iter.close() since it would require another STELLA to Python callback:
    input_stream = iter.inputStream
    if isinstance(input_stream, pkgtk.CallbackInputStream):
        pystreamPtr = input_stream.pythonStream
        if pystreamPtr is not None:
            pystream = spi.getPythonObjectFromStellaPointer(pystreamPtr)
            pystream.close()
    iter.close()


### Test drivers:

def testKgtkValidationIterator1(file, logFile, chunkSize=10000):
    # Just run the validation, do not bring the string into Python.
    iter = pkgtk.allocateKgtkValidationIterator(file, logFile)
    iter.chunkSize = chunkSize
    iter.rowSeparator = iter.fieldSeparator
    for value in iter:
        pass
    iter.close()
    return iter.lineNumber, iter

def testKgtkValidationIterator2(file, logFile, chunkSize=10000):
    # Run the validation, plus copy the string into Python.
    iter = pkgtk.allocateKgtkValidationIterator(file, logFile)
    iter.chunkSize = chunkSize
    nchars = 0
    for value in iter:
        #value = value.theString()
        nchars += len(value.theString())
    iter.data = value
    iter.close()
    return iter.lineNumber, iter, nchars

def testKgtkValidationIterator3(file, logFile=None, chunkSize=10000):
    # Run the validation, copy the string and split into a list of field value strings.
    iter = pkgtk.allocateKgtkValidationIterator(file, logFile)
    iter.chunkSize = chunkSize
    iter.rowSeparator = iter.fieldSeparator
    nTotalItems = 0
    for value in iter:
        value = value.theString()
        frame = value.split('\t')
        nTotalItems += len(frame)
    iter.frame = frame
    iter.close()
    return iter.lineNumber, nTotalItems, iter

"""
>>> import kgtk.stetools.api as kpi
>>> with Timer() as t:
...     kpi.testKgtkValidationIterator1("/data/kgtk/wikidata/run2/wikidata_edges_20200330-slice.tsv", "/tmp/errors.log", chunkSize=100000)
(1000000, <proxy.KgtkValidationIterator |i|/KGTK/@KGTK-VALIDATION-ITERATOR>)
>>> t.elapsed
0.27631187438964844
>>> 
>>> with Timer() as t:
...     kpi.testKgtkValidationIterator2("/data/kgtk/wikidata/run2/wikidata_edges_20200330-slice.tsv", "/tmp/errors.log", chunkSize=100000)
(1000000, <proxy.KgtkValidationIterator |i|/KGTK/@KGTK-VALIDATION-ITERATOR>)
>>> t.elapsed
0.3292710781097412
>>> 
>>> with Timer() as t:
...     kpi.testKgtkValidationIterator3("/data/kgtk/wikidata/run2/wikidata_edges_20200330-slice.tsv", "/tmp/errors.log", chunkSize=100000)
(1000000, 12999996, <proxy.KgtkValidationIterator |i|/KGTK/@KGTK-VALIDATION-ITERATOR>)
>>> t.elapsed
0.6946520805358887
>>> 
"""
