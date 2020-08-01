#!/usr/bin/env python

import sys
import os
import os.path
import io
import re

import cffi
import stellapi as spi


# NOTES:
# - there are two principle ways to add this system as a Python extension:
#   (1) Similar to what we do for stellapi, we build it with the standard STELLA Makefile
#       machinery, write the libs into a .libs directory and then create an extension module
#       wrapper simply around that.  The advantage of that is that it exposes the underlying
#       system libraries which can then be linked to by other systems.  The disadvantage is
#       that we have to go through `make' which is less portable.
#   (2) We simply concatenate all the C++ files and directly build a Python extension module
#       from them using cffi.  This is likely more portable which is what we do here, and
#       since we don't have any dependent systems we don't lose anything.
# TO DO: we should support generation of build files like this directly from STELLA

# GC segfault bug summary:
# - we had some real bugs in the reader, such as incorrect resetting of `lineStart' which we fixed
# - we figured out that the bug was GC-related, since disabling it made everything work fine
# - the root cause seems to have been the missing -DSTELLA_USE_GC definition during compilation
#   which only showed up in the Python version but not in our regular kgtk build
# - even once we switched to `build_libkgtk_dbg.py' and compiled with the regular STELLA make process,
#   we had introduced new errors that continued giving us similar problems, such as, eliminating the
#   link of the read buffer to the callback stream which caused premature collection, and also
#   setting GC_DEBUG to 1 while still using the non-debug finalizer functions in stellapi directly
# - there were many other potential problems and confounders which served as distractors (GC library
#   mismatches, new C++ stream class, Python 3, data transfer from Python to C++, multi-processing,...)
# - eventually, we dug ourselves out of that and finally, when we moved back to the original build
#   process we discovered the missing STELLA_USE_GC which has now been added - phew


module_dir = os.path.realpath(os.path.dirname(__file__))
source_dir = os.path.join(module_dir, 'sources')
native_dir = os.path.join(module_dir, 'native')
cpp_dir    = os.path.join(native_dir, 'cpp')
spi_dir    = os.path.realpath(os.path.dirname(spi.__file__))
kgtk_dir   = os.path.join(cpp_dir, 'kgtk')
kgtk_files = ['reader.cc', 'kgtk.cc', 'startup-system.cc']

module_name = 'kgtk.stetools._libkgtk'


# retranslate STELLA sources to C++:

spi.loadFile(os.path.join(os.path.dirname(spi.__file__), 'sources', 'systems', 'stella-system.ste'))
spi.loadFile(os.path.join(source_dir, 'systems', 'kgtk-system.ste'))
spi.translateSystem("kgtk", "cpp", force=True, devel=False)


# compile C++ into Python extension library:

def get_namespaces(file='startup-system.cc'):
    nss = set()
    nsregex = re.compile('^ *(using )?namespace ([a-zA-Z0-9_]+)(;| {)$')
    with open(os.path.join(kgtk_dir, file), 'rt') as kf:
        for line in kf:
            match = nsregex.match(line)
            if match is not None:
                nss.add(match.groups()[-2])
    return nss

module_code = io.StringIO()
module_code.write('#define STELLA_USE_GC\n') # IMPORTANT, usually defined as -DSTELLA_USE_GC make switch
for file in kgtk_files:
    with open(os.path.join(kgtk_dir, file), 'rt') as kf:
        module_code.write(kf.read())
# since we are adding the code in-line, we need to add these namespace settings
# so that the reference to the extern "C" system startup function will work:
for ns in get_namespaces():
    module_code.write('using namespace %s;\n' % ns)
module_code = module_code.getvalue()

ffibuilder = cffi.FFI()

ffibuilder.set_source(module_name,
                      module_code,
                      language='c++',
                      source_extension='.cc',
                      include_dirs=[cpp_dir, os.path.join(spi_dir, 'include'), os.path.join(spi_dir, 'include', 'gc')],
                      library_dirs=[os.path.join(spi_dir, '.libs')],
                      runtime_library_dirs=['$ORIGIN', '$ORIGIN/.libs', os.path.join(spi_dir, '.libs')],
                      libraries = ['stella', 'gc'],
                      # force a consistent SOABI extension in Py3 in case we build with setuptools:
                      py_limited_api = False,
)

# since we are not going through the normal build machinery, we have to explicitly define the signatures we need:
for line in module_code.splitlines():
    if line.startswith('extern "C"'):
        ffibuilder.cdef(line.replace('extern "C" ', '').replace('{', ';'))

# define a new-style callback we need:
ffibuilder.cdef('extern "Python" int new_callback_stream_reader(void*, char*, int);')

if __name__ == "__main__":
    ffibuilder.compile(verbose=False)
