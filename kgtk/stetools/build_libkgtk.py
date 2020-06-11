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


module_dir = os.path.realpath(os.path.dirname(__file__))
source_dir = os.path.join(module_dir, 'sources')
native_dir = os.path.join(module_dir, 'native')
cpp_dir    = os.path.join(native_dir, 'cpp')
spi_dir    = os.path.realpath(os.path.dirname(spi.__file__))
kgtk_dir   = os.path.join(cpp_dir, 'kgtk')
kgtk_files = ['reader.cc', 'kgtk.cc', 'startup-system.cc']

module_name = 'kgtk.stetools._libkgtk'


# retranslate STELLA sources to C++:

if not os.path.exists(kgtk_dir):
    os.makedirs(kgtk_dir)

spi.defun('stella/load-file')
spi.defun('stella/evaluate-string')
spi.stella.pdef.loadFile(os.path.join(os.path.dirname(spi.__file__), 'sources', 'systems', 'stella-system.ste'))
#spi.stella.pdef.loadFile(os.path.join(source_dir, 'systems', 'kgtk-system.ste')) # not needed

spi.stella.pdef.evaluateString("""
    (translate-system "kgtk" :cpp
                      :force-translation? true
                      :recursive? false
                      :production-settings? true
                      ;; setting this locates the system and generates relative include pathnames:
                      :root-native-directory "%s")
    """ % native_dir)


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
                      include_dirs=[cpp_dir, os.path.join(spi_dir, 'include')],
                      library_dirs=[os.path.join(spi_dir, '.libs'), '/usr/lib/x86_64-linux-gnu'], ######## EXPERIMENT
                      runtime_library_dirs=['$ORIGIN', '$ORIGIN/.libs', os.path.join(spi_dir, '.libs'), '/usr/lib/x86_64-linux-gnu'],
                      libraries = ['stella', #gc'
                                   ':libgc.so.1'], ######## EXPERIMENT
                      # force a consistent SOABI extension in Py3 in case we build with setuptools:
                      py_limited_api = False,
)

# since we are not going through the normal build machinery, we have to explicitly define the signatures we need:
for line in module_code.splitlines():
    if line.startswith('extern "C"'):
        ffibuilder.cdef(line.replace('extern "C" ', '').replace('{', ';'))


if __name__ == "__main__":
    ffibuilder.compile(verbose=False)
