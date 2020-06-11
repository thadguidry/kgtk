"""
Validate the syntax of a KGTK file, producing error messages.

Similar functionality to `validate' but focuses on fast, syntactic
validation only and does not attempt any automatic repairs.

Besides reporting errors, this command can record invalid lines, which,
after correction, can be appended to the validated portion to fix the data.
"""

import sys
import os
import io
import sh

from kgtk.exceptions import KGTKException


# these mirror what's in kpi so we don't have to import the module early:
DEFAULT_ERROR_LIMIT = 1000
DEFAULT_CHUNK_SIZE  = 100000
DEFAULT_INVALID_VALUE_ACTION = 'pass'

def parser():
    return {
        'help': 'Validate the syntax of a KGTK file',
        'description': 'Validate the syntax of a KGTK file, producing error messages.',
    }

def add_arguments_extended(parser, parsed_shared_args):
    parser.accept_shared_argument('_debug')
    parser.accept_shared_argument('_expert')

    parser.add_argument('input', metavar='INPUT', nargs='?', action='store',
                        help="input file to validate, if empty or `-' process stdin")
    parser.add_argument('--log', '--log-file', default=None, action='store', dest='log_file',
                        help='log file to write errors to, otherwise error messages go to stderr')
    parser.add_argument('--error-limit', type=int, default=DEFAULT_ERROR_LIMIT, action='store', dest='error_limit',
                        help='the maximum number of errors to report before failing (default=%d), ' % DEFAULT_ERROR_LIMIT
                        + 'a value of 0 means unbounded')
    parser.add_argument('--invalid-value-action', default=None, choices=['pass', 'exclude', 'exclude-line'],
                        action='store', dest='invalid_value_action',
                        help='action when an invalid value is encountered; if an invalid lines file is specified'
                        + " the action is always `exclude_line', otherwise the default is `%s'" % DEFAULT_INVALID_VALUE_ACTION)
    parser.add_argument('--invalid-file', default=None, action='store', dest='invalid_file',
                        help='file to write all invalid lines to, useful for fixing the data')
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE, action='store', dest='chunk_size',
                        help='number of rows to read and validate at a time (default=%d)' % DEFAULT_CHUNK_SIZE)
    parser.add_argument('-o', '--out', default=None, action='store', dest='output',
                        help="output file to write to, if `-' validated output goes to stdout, "
                        + " if omitted, no output will be written")

def import_modules():
    """Import command-specific modules that are only needed when we actually run.
    """
    mod = sys.modules[__name__]
    import stellapi as spi
    setattr(mod, "spi", spi)
    import kgtk.stetools.api as kpi
    setattr(mod, "kpi", kpi)

def validate_syntax(file, log_file=sys.stderr, invalid_file=None, output=None,
                    invalid_value_action=None, error_limit=None, chunk_size=None,
                    _expert=False, _debug=False):
    """Validate `file' with errors to `log_file'.  If `output' is not None
    copy validated output to it, otherwise simply ignore it.
    """
    iter = kpi.allocate_validation_iterator(file=file, log_file=log_file, invalid_file=invalid_file,
                                            invalid_value_action=invalid_value_action,
                                            error_limit=error_limit, chunk_size=chunk_size,
                                            smart=True, bg=True)
    if isinstance(output, str):
        output = open(output, mode='wt')
    try:
        for value in iter:
            if output is not None:
                # TO DO: upgrade this to retrieve bytes instead of a string to avoid en/decoding:
                data = value.theString()
                output.write(data)
    finally:
        kpi.close_validation_iterator(iter)
        if output is not None:
            output.close()
    
def run(input=None, log_file=None, invalid_file=None, output=None,
        invalid_value_action=None, error_limit=None, chunk_size=None,
        _expert=False, _debug=False):
    """Run validate_syntax according to the provided command-line arguments.
    """
    try:
        import_modules()
        input = input or '-'
        if input == '-':
            input = sys.stdin.buffer
        if output == '-':
            output = sys.stdout
        log_file = log_file or sys.stderr.buffer

        if _debug:
            sys.stderr.write('validate_syntax args:\n' +
                             '  in: %s, log: %s, invfile: %s, out: %s\n'
                             % (input, log_file, invalid_file, output) +
                             '  invact: %s, elim: %d, chunk: %d, exp: %s, dbg: %s\n'
                             % (invalid_value_action, error_limit, chunk_size, _expert, _debug))

        validate_syntax(input, log_file=log_file, invalid_file=invalid_file, output=output,
                        invalid_value_action=invalid_value_action, error_limit=error_limit, chunk_size=chunk_size,
                        _expert=_expert, _debug=_debug)

    except sh.SignalException_SIGPIPE:
        # hack to work around Python3 issue when stdout is gone when we try to report an exception;
        # without this we get an ugly 'Exception ignored...' msg when we quit with head or a pager:
        sys.stdout = os.fdopen(1)
    except Exception as e:
        #import traceback
        #traceback.print_tb(sys.exc_info()[2], 10)
        raise KGTKException(str(e) + '\n')
