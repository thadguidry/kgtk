"""
Write a KGTK edge or node file in TSV format.

"""

from argparse import ArgumentParser
import attr
import bz2
from enum import Enum
import gzip
import json
import lz4 # type: ignore
import lzma
from pathlib import Path
from multiprocessing import Queue
import sys
import typing

from kgtk.kgtkformat import KgtkFormat
from kgtk.io.kgtkbase import KgtkBase
from kgtk.io.kgtkreader import KgtkReader, KgtkReaderMode
from kgtk.utils.enumnameaction import EnumNameAction
from kgtk.utils.gzipprocess import GzipProcess
from kgtk.utils.validationaction import ValidationAction

class KgtkWriterMode(Enum):
    """
    There are four file reading/writing modes:
    """
    NONE = 0 # Enforce neither edge nore node file required columns
    EDGE = 1 # Enforce edge file required columns
    NODE = 2 # Enforce node file require columns
    AUTO = 3 # Automatically decide whether to enforce edge or node file required columns

ttr.s(slots=True, frozen=True)
class KgtkWriterOptions():
    GZIP_QUEUE_SIZE_DEFAULT: int = GzipProcess.GZIP_QUEUE_SIZE_DEFAULT

    # TODO: use an enum
    OUTPUT_FORMAT_CSV: str = "csv"
    OUTPUT_FORMAT_JSON: str = "json"
    OUTPUT_FORMAT_JSON_MAP: str = "json-map"
    OUTPUT_FORMAT_JSON_MAP_COMPACT: str = "json-map-compact"
    OUTPUT_FORMAT_JSONL: str = "jsonl"
    OUTPUT_FORMAT_JSONL_MAP: str = "jsonl-map"
    OUTPUT_FORMAT_JSONL_MAP_COMPACT: str = "jsonl-map-compact"
    OUTPUT_FORMAT_KGTK: str = "kgtk"
    OUTPUT_FORMAT_MD: str = "md"

    OUTPUT_FORMAT_CHOICES: typing.List[str] = [
        OUTPUT_FORMAT_CSV,
        OUTPUT_FORMAT_JSON,
        OUTPUT_FORMAT_JSON_MAP,
        OUTPUT_FORMAT_JSON_MAP_COMPACT,
        OUTPUT_FORMAT_JSONL,
        OUTPUT_FORMAT_JSONL_MAP,
        OUTPUT_FORMAT_JSONL_MAP_COMPACT,
        OUTPUT_FORMAT_KGTK,
        OUTPUT_FORMAT_MD,
    ]
    OUTPUT_FORMAT_DEFAULT: str = OUTPUT_FORMAT_KGTK

    mode: KgtkReaderMode = attr.ib(validator=attr.validators.instance_of(KgtkWriterMode), default=KgtkWriterMode.AUTO)

    column_separator: str = attr.ib(validator=attr.validators.instance_of(str), default=KgtkFormat.COLUMN_SEPARATOR)

    # Require or fill trailing fields?
    require_all_columns: bool = attr.ib(validator=attr.validators.instance_of(bool), default=True)
    prohibit_extra_columns: bool = attr.ib(validator=attr.validators.instance_of(bool), default=True)
    fill_missing_columns: bool = attr.ib(validator=attr.validators.instance_of(bool), default=False)

    # Rename the output columns?
    #
    # TODO: use proper validators.
    output_column_names: typing.Optional[typing.List[str]] = attr.ib(default=None)
    old_column_names: typing.Optional[typing.List[str]] = attr.ib(default=None)
    new_column_names: typing.Optional[typing.List[str]] = attr.ib(default=None)

    # How should header errors be processed?
    header_error_action: ValidationAction = attr.ib(validator=attr.validators.instance_of(ValidationAction), default=ValidationAction.EXIT)

    # Other implementation options?
    compression_type: typing.Optional[str] = attr.ib(validator=attr.validators.optional(attr.validators.instance_of(str)), default=None) # TODO: use an Enum
    gzip_in_parallel: bool = attr.ib(validator=attr.validators.instance_of(bool), default=False)
    gzip_queue_size: int = attr.ib(validator=attr.validators.instance_of(int), default=GZIP_QUEUE_SIZE_DEFAULT)

    output_format: str = attr.ib(validator=attr.validators.instance_of(str), default=OUTPUT_FORMAT_DEFAULT) # TODO: use an enum

    @classmethod
    def add_arguments(cls,
                      parser: ArgumentParser,
                      mode_options: bool = False,
                      default_mode: KgtkWriterMode = KgtkWriterMode.AUTO,
                      expert: bool = False,
                      defaults: bool = True,
                      who: str = "",
    ):

        # This helper function makes it easy to suppress options from
        # The help message.  The options are still there, and initialize
        # what they need to initialize.
        def h(msg: str)->str:
            if expert:
                return msg
            else:
                return SUPPRESS

        # This helper function decices whether or not to include defaults
        # in argument declarations. If we plan to make arguments with
        # prefixes and fallbacks, the fallbacks (the ones without prefixes)
        # should get default values, while the prefixed arguments should
        # not get defaults.
        #
        # Note: In obscure circumstances (EnumNameAction, I'm looking at you),
        # explicitly setting "default=None" may fail, whereas omitting the
        # "default=" phrase succeeds.
        #
        # TODO: continue researching these issues.
        def d(default: typing.Any)->typing.Mapping[str, typing.Any]:
            if defaults:
                return {"default": default}
            else:
                return { }

        prefix1: str = "--" if len(who) == 0 else "--" + who + "-"
        prefix2: str = "" if len(who) == 0 else who + "_"
        prefix3: str = "" if len(who) == 0 else who + ": "
        prefix4: str = "" if len(who) == 0 else who + " file "

        fgroup: _ArgumentGroup = parser.add_argument_group(h(prefix3 + "File options"),
                                                           h("Options affecting " + prefix4 + "processing."))
        fgroup.add_argument(prefix1 + "column-separator",
                            dest=prefix2 + "column_separator",
                            help=h(prefix3 + "Column separator (default=<TAB>)."), # TODO: provide the default with escapes, e.g. \t
                            type=str, **d(default=KgtkFormat.COLUMN_SEPARATOR))

        # TODO: use an Enum or add choices.
        fgroup.add_argument(prefix1 + "compression-type",
                            dest=prefix2 + "compression_type",
                            help=h(prefix3 + "Specify the compression type (default=%(default)s)."))

        fgroup.add_argument(prefix1 + "error-limit",
                            dest=prefix2 + "error_limit",
                            help=h(prefix3 + "The maximum number of errors to report before failing (default=%(default)s)"),
                            type=int, **d(default=cls.ERROR_LIMIT_DEFAULT))

        fgroup.add_argument(prefix1 + "gzip-in-parallel",
                            dest=prefix2 + "gzip_in_parallel",
                            metavar="optional True|False",
                            help=h(prefix3 + "Execute gzip in parallel (default=%(default)s)."),
                            type=optional_bool, nargs='?', const=True, **d(default=False))

        fgroup.add_argument(prefix1 + "gzip-queue-size",
                            dest=prefix2 + "gzip_queue_size",
                            help=h(prefix3 + "Queue size for parallel gzip (default=%(default)s)."),
                            type=int, **d(default=cls.GZIP_QUEUE_SIZE_DEFAULT))

        if mode_options:
            fgroup.add_argument(prefix1 + "mode",
                                dest=prefix2 + "mode",
                                help=h(prefix3 + "Determine the KGTK file mode (default=%(default)s)."),
                                type=KgtkWriterMode, action=EnumNameAction, **d(default_mode))
            
        hgroup: _ArgumentGroup = parser.add_argument_group(h(prefix3 + "Header processing"),
                                                           h("Options affecting " + prefix4 + "header processing."))

        hgroup.add_argument(prefix1 + "header-error-action",
                            dest=prefix2 + "header_error_action",
                            help=h(prefix3 + "The action to take when a header error is detected.  Only ERROR or EXIT are supported (default=%(default)s)."),
                            type=ValidationAction, action=EnumNameAction, **d(default=ValidationAction.EXIT))

        hgroup.add_argument(prefix1 + "output-columns",
                            dest=prefix2 + "output_column_names",
                            metavar="NEW_COLUMN_NAME",
                            help=h(prefix3 + "The list of new column names when renaming all columns. (default=None)"),
                            type=str, nargs='+')

        hgroup.add_argument(prefix1 + "old-columns",
                            dest=prefix2 + "old_column_names",
                            metavar="OLD_COLUMN_NAME",
                            help=h(prefix3 + "The list of old column names for selective renaming. (default=None)"),
                            type=str, nargs='+')

        hgroup.add_argument(prefix1 + "new-columns",
                            dest=prefix2 + "new_column_names",
                            metavar="NEW_COLUMN_NAME",
                            help=h(prefix3 + "The list of new column names for selective renaming. (default=None)"),
                            type=str, nargs='+')

        lgroup: _ArgumentGroup = parser.add_argument_group(h(prefix3 + "Line processing"),
                                                           h("Options affecting " + prefix4 + "line processing."))

        lgroup.add_argument(prefix1 + "require-all-columns",
                            dest=prefix2 + "require_all_columms",
                            metavar="optional True|False",
                            help=h(prefix3 + "Require all columns on each line. (default=%(default)s)."),
                            type=optional_bool, nargs='?', const=True, **d(default=False))

        lgroup.add_argument(prefix1 + "prohibit-extra-columns",
                            dest=prefix2 + "prohibit_extra_columms",
                            metavar="optional True|False",
                            help=h(prefix3 + "Prohibit extra columns on each line. (default=%(default)s)."),
                            type=optional_bool, nargs='?', const=True, **d(default=False))

        lgroup.add_argument(prefix1 + "fill-missing-columns",
                            dest=prefix2 + "fill_missing_columms",
                            metavar="optional True|False",
                            help=h(prefix3 + "Fill missing columns on each line. (default=%(default)s)."),
                            type=optional_bool, nargs='?', const=True, **d(default=False))


    @classmethod
    # Build the value parsing option structure.
    def from_dict(cls,
                  d: dict,
                  who: str = "",
                  mode: typing.Optional[KgtkWriterMode] = None,
                  fallback: bool = False,
    )->'KgtkWriterOptions':
        prefix: str = ""   # The destination name prefix.
        if len(who) > 0:
            prefix = who + "_"

        # TODO: Figure out how to type check this method.
        def lookup(name: str, default):
            prefixed_name = prefix + name
            if prefixed_name in d and d[prefixed_name] is not None:
                return d[prefixed_name]
            elif fallback and name in d and d[name] is not None:
                return d[name]
            else:
                return default
            
        writer_mode: KgtkWriterMode
        if mode is not None:
            writer_mode = mode
        else:
            writer_mode = lookup("mode", KgtkWriterMode.AUTO)

        return cls(
            mode=writer_mode,
            column_separator=lookup("column_separator", KgtkFormat.COLUMN_SEPARATOR),
            require_all_columns=lookup("require_all_columns", True),
            prohibit_extra_columns=lookup("prohibit_extra_columns", True),
            fill_missing_columns=lookup("fill_missing_columns", False),
            header_error_action=lookup("header_error_action", default=ValidationAction.EXIT),
            compression_type=lookup("compression_type", default=None),
            gzip_in_parallel=lookup("gzip_in_parallel", default=False),
            gzip_queue_size=looklup("gzip_queue_size", default=KgtkWriterOptions.GZIP_QUEUE_SIZE_DEFAULT),
            output_format=lookup("output_format", default=KgtkWriterOptions.OUTPUT_FORMAT_DEFAULT),
            output_columns=lookup('****************`****************************************************
        )

    # Build the value parsing option structure.
    @classmethod
    def from_args(cls,
                  args: Namespace,
                  who: str = "",
                  mode: typing.Optional[KgtkWriterMode] = None,
                  fallback: bool = False,
    )->'KgtkWriterOptions':
        return cls.from_dict(vars(args), who=who, mode=mode, fallback=fallback)

    def show(self, who: str="", out: typing.TextIO=sys.stderr):
        prefix: str = "--" if len(who) == 0 else "--" + who + "-"
        print("%scolumn-separator=%s" % (prefix, repr(self.column_separator)), file=out)
        if self.compression_type is not None:
            print("%scompression-type=%s" % (prefix, str(self.compression_type)), file=out)
        print("%sgzip-in-parallel=%s" % (prefix, str(self.gzip_in_parallel)), file=out)
        print("%sgzip-queue-size=%s" % (prefix, str(self.gzip_queue_size)), file=out)
        print("%smode=%s" % (prefix, self.mode.name), file=out)
        print("%sheader-error-action=%s" % (prefix, self.header_error_action.name), file=out)
        print("%srequire-all-columns=%s" % (prefix, str(self.require_all_columns)), file=out)
        print("%sprohibit-extra-columns=%s" % (prefix, str(self.prohibit_extra_columns)), file=out)
        print("%sfill-missing-columns=%s" % (prefix, str(self.fill_missing_columns)), file=out)
    


DEFAULT_KGTK_WRITER_OPTIONS: KgtkWriterOptions = KgtkReaderOptions()


@attr.s(slots=True, frozen=False)
class KgtkWriter(KgtkBase):

    file_path: typing.Optional[Path] = attr.ib(validator=attr.validators.optional(attr.validators.instance_of(Path)))
    file_out: typing.TextIO = attr.ib() # Todo: validate TextIO
    column_names: typing.List[str] = attr.ib(validator=attr.validators.deep_iterable(member_validator=attr.validators.instance_of(str),
                                                                                     iterable_validator=attr.validators.instance_of(list)))

    options: KgtkWriterOptions = attr.ib()

    # For convenience, the count of columns. This is the same as len(column_names).
    column_count: int = attr.ib(validator=attr.validators.instance_of(int))

    column_name_map: typing.Mapping[str, int] = attr.ib(validator=attr.validators.deep_mapping(key_validator=attr.validators.instance_of(str),
                                                                                               value_validator=attr.validators.instance_of(int)))

    # Use these names in the output file, but continue to use
    # column_names for shuffle lists.
    output_column_names: typing.List[str] = \
        attr.ib(validator=attr.validators.deep_iterable(member_validator=attr.validators.instance_of(str),
                                                        iterable_validator=attr.validators.instance_of(list)))

    line_count: int = attr.ib(validator=attr.validators.instance_of(int), default=0)

    gzip_thread: typing.Optional[GzipProcess] = attr.ib(validator=attr.validators.optional(attr.validators.instance_of(GzipProcess)), default=None)

    error_file: typing.TextIO = attr.ib(default=sys.stderr)
    verbose: bool = attr.ib(validator=attr.validators.instance_of(bool), default=False)
    very_verbose: bool = attr.ib(validator=attr.validators.instance_of(bool), default=False)

    @classmethod
    def open(cls,
             column_names: typing.List[str],
             file_path: typing.Optional[Path],
             who: str = "output",
             error_file: typing.TextIO = sys.stderr,
             mode: typing.Union[KgtkReaderMode, KgtkWriterMode] = KgtkWriterMode.AUTO,
             options: typing.Optional[KgtkWriterOptions] = None,
             verbose: bool = False,
             very_verbose: bool = False)->"KgtkWriter":

        # The following dance allows the convenience of passing either a
        # KgtkWriterMode or a KgtkReaderMode to open(...).
        wmode: KgtkWriterMode
        if isinstance(mode, KgtkWriterMode):
            wmode = mode
        elif isinstance(mode, KgtkReaderMode):
            wmode = KgtkWriterMode[mode.name]

        if options is None:
            options = DEFAULT_KGTK_WRITER_OPTIONS

        if file_path is None or str(file_path) == "-":
            if verbose:
                print("KgtkWriter: writing stdout", file=error_file, flush=True)

            return cls._setup(column_names=column_names,
                              file_path=None,
                              who=who,
                              file_out=sys.stdout,
                              require_all_columns=require_all_columns,
                              prohibit_extra_columns=prohibit_extra_columns,
                              fill_missing_columns=fill_missing_columns,
                              error_file=error_file,
                              header_error_action=header_error_action,
                              gzip_in_parallel=gzip_in_parallel,
                              gzip_queue_size=gzip_queue_size,
                              column_separator=column_separator,
                              mode=wmode,
                              output_format=output_format,
                              output_column_names=output_column_names,
                              old_column_names=old_column_names,
                              new_column_names=new_column_names,
                              verbose=verbose,
                              very_verbose=very_verbose,
            )
        
        if verbose:
            print("File_path.suffix: %s" % file_path.suffix, file=error_file, flush=True)

        if file_path.suffix in [".gz", ".bz2", ".xz", ".lz4"]:
            # TODO: find a better way to coerce typing.IO[Any] to typing.TextIO
            gzip_file: typing.TextIO
            if file_path.suffix == ".gz":
                if verbose:
                    print("KgtkWriter: writing gzip %s" % str(file_path), file=error_file, flush=True)
                gzip_file = gzip.open(file_path, mode="wt") # type: ignore
            elif file_path.suffix == ".bz2":
                if verbose:
                    print("KgtkWriter: writing bz2 %s" % str(file_path), file=error_file, flush=True)
                gzip_file = bz2.open(file_path, mode="wt") # type: ignore
            elif file_path.suffix == ".xz":
                if verbose:
                    print("KgtkWriter: writing lzma %s" % str(file_path), file=error_file, flush=True)
                gzip_file = lzma.open(file_path, mode="wt") # type: ignore
            elif file_path.suffix ==".lz4":
                if verbose:
                    print("KgtkWriter: writing lz4 %s" % str(file_path), file=error_file, flush=True)
                gzip_file = lz4.frame.open(file_or_path, mode="wt") # type: ignore
            else:
                # TODO: throw a better exception.
                raise ValueError("Unexpected file_path.suffiz = '%s'" % file_path.suffix)

            return cls._setup(column_names=column_names,
                              file_path=file_path,
                              who=who,
                              file_out=gzip_file,
                              require_all_columns=require_all_columns,
                              prohibit_extra_columns=prohibit_extra_columns,
                              fill_missing_columns=fill_missing_columns,
                              error_file=error_file,
                              header_error_action=header_error_action,
                              gzip_in_parallel=gzip_in_parallel,
                              gzip_queue_size=gzip_queue_size,
                              column_separator=column_separator,
                              mode=wmode,
                              output_format=output_format,
                              output_column_names=output_column_names,
                              old_column_names=old_column_names,
                              new_column_names=new_column_names,
                              verbose=verbose,
                              very_verbose=very_verbose,
            )
            
        else:
            if output_format is None:
                # TODO: optionally stack these on top of compression
                if file_path.suffix == ".md":
                    output_format = "md"
                elif file_path.suffix == ".csv":
                    output_format = "csv"
                elif file_path.suffix == ".json":
                    output_format = "json"
                elif file_path.suffix == ".jsonl":
                    output_format = "jsonl"
                else:
                    output_format = "kgtk"

            if verbose:
                print("KgtkWriter: writing file %s" % str(file_path), file=error_file, flush=True)
            return cls._setup(column_names=column_names,
                              file_path=file_path,
                              who=who,
                              file_out=open(file_path, "w"),
                              require_all_columns=require_all_columns,
                              prohibit_extra_columns=prohibit_extra_columns,
                              fill_missing_columns=fill_missing_columns,
                              error_file=error_file,
                              header_error_action=header_error_action,
                              gzip_in_parallel=gzip_in_parallel,
                              gzip_queue_size=gzip_queue_size,
                              column_separator=column_separator,
                              mode=wmode,
                              output_format=output_format,
                              output_column_names=output_column_names,
                              old_column_names=old_column_names,
                              new_column_names=new_column_names,
                              verbose=verbose,
                              very_verbose=very_verbose,
)
    
    @classmethod
    def _setup(cls,
               column_names: typing.List[str],
               file_path: typing.Optional[Path],
               who: str,
               file_out: typing.TextIO,
               require_all_columns: bool,
               prohibit_extra_columns: bool,
               fill_missing_columns: bool,
               error_file: typing.TextIO,
               header_error_action: ValidationAction,
               gzip_in_parallel: bool,
               gzip_queue_size: int,
               column_separator: str,
               mode: KgtkWriterMode = KgtkWriterMode.AUTO,
               output_format: typing.Optional[str] = None,
               output_column_names: typing.Optional[typing.List[str]] = None,
               old_column_names: typing.Optional[typing.List[str]] = None,
               new_column_names: typing.Optional[typing.List[str]] = None,
               verbose: bool = False,
               very_verbose: bool = False,
    )->"KgtkWriter":

        if output_format is None:
            output_format = cls.OUTPUT_FORMAT_DEFAULT
            if verbose:
                print("Defaulting the output format to %s" % output_format, file=error_file, flush=True)

        if output_format == cls.OUTPUT_FORMAT_CSV:
            column_separator = "," # What a cheat!
                
        if output_column_names is None:
            output_column_names = column_names
        else:
            # Rename all output columns.
            if len(output_column_names) != len(column_names):
                raise ValueError("%s: %d column names but %d output column names" % (who, len(column_names), len(output_column_names)))

        if old_column_names is not None or new_column_names is not None:
            # Rename selected output columns:
            if old_column_names is None or new_column_names is None:
                raise ValueError("%s: old/new column name mismatch" % who)
            if len(old_column_names) != len(new_column_names):
                raise ValueError("%s: old/new column name length mismatch: %d != %d" % (who, len(old_column_names), len(new_column_names)))

            # Rename columns in place.  Start by copyin the output column name
            # list so the changes don't inadvertantly propogate.
            output_column_names = output_column_names.copy()
            column_name: str
            idx: int
            for idx, column_name in enumerate(old_column_names):
                if column_name not in output_column_names:
                    raise ValueError("%s: old column names %s not in the output column names." % (who, column_name))
                output_column_names[output_column_names.index(column_name)] = new_column_names[idx]
                

        # Build a map from column name to column index.  This is used for
        # self.writemap(...)  and self.build_shuffle_list(...)
        column_name_map: typing.Mapping[str, int] = cls.build_column_name_map(column_names,
                                                                              header_line=column_separator.join(column_names),
                                                                              who=who,
                                                                              error_action=header_error_action,
                                                                              error_file=error_file)

        # Build a header line for error feedback:
        header: str = column_separator.join(output_column_names)

        # Build a map from output column name to column index.
        output_column_name_map: typing.Mapping[str, int] = cls.build_column_name_map(output_column_names,
                                                                                     header_line=header,
                                                                                     who=who,
                                                                                     error_action=header_error_action,
                                                                                     error_file=error_file)

        # Should we automatically determine if this is an edge file or a node file?
        is_edge_file: bool = False
        is_node_file: bool = False
        if mode is KgtkWriterMode.AUTO:
            # If we have a node1 (or alias) column, then this must be an edge file. Otherwise, assume it is a node file.
            node1_idx: int = cls.get_column_idx(cls.NODE1_COLUMN_NAMES, output_column_name_map,
                                                header_line=header,
                                                who=who,
                                                error_action=header_error_action,
                                                error_file=error_file,
                                                is_optional=True)
            is_edge_file = node1_idx >= 0
            is_node_file = not is_edge_file
        elif mode is KgtkWriterMode.EDGE:
            is_edge_file = True
        elif mode is KgtkWriterMode.NODE:
            is_node_file = True
        elif mode is KgtkWriterMode.NONE:
            pass
        
        # Validate that we have the proper columns for an edge or node file,
        # ignoring the result.
        cls.get_special_columns(output_column_name_map,
                                header_line=header,
                                who=who,
                                error_action=header_error_action,
                                error_file=error_file,
                                is_edge_file=is_edge_file,
                                is_node_file=is_node_file)

        gzip_thread: typing.Optional[GzipProcess] = None
        if gzip_in_parallel:
            if verbose:
                print("Starting the gzip process.", file=error_file, flush=True)
            gzip_thread = GzipProcess(file_out, Queue(gzip_queue_size))
            gzip_thread.start()

        kw: KgtkWriter = cls(file_path=file_path,
                             file_out=file_out,
                             column_separator=column_separator,
                             column_names=column_names,
                             column_name_map=column_name_map,
                             column_count=len(column_names),
                             require_all_columns=require_all_columns,
                             prohibit_extra_columns=prohibit_extra_columns,
                             fill_missing_columns=fill_missing_columns,
                             error_file=error_file,
                             header_error_action=header_error_action,
                             gzip_in_parallel=gzip_in_parallel,
                             gzip_thread=gzip_thread,
                             gzip_queue_size=gzip_queue_size,
                             output_format=output_format,
                             output_column_names=output_column_names,
                             line_count=1,
                             verbose=verbose,
                             very_verbose=very_verbose,
        )
        kw.write_header()
        return kw


    def join_csv(self, values: typing.List[str])->str:
        line: str = ""
        value: str
        for value in values:
            if '"' in value or ',' in value:
                value = '"' + '""'.join(value.split('"')) + '"'
            if len(line) > 0:
                line += ","
            line += value
        return line

    def join_md(self, values: typing.List[str])->str:
        line: str = "|"
        value: str
        for value in values:
            value = "\\|".join(value.split("|"))
            line += " " + value + " |"
        return line

    def json_map(self, values: typing.List[str], compact: bool = False)->typing.Mapping[str, str]:
        result: typing.MutableMapping[str, str] = { }
        idx: int
        value: str
        for idx, value in enumerate(values):
            if len(value) > 0 or not compact:
                result[self.output_column_names[idx]] = value
        return result

    def write_header(self):
        header: str
        header2: typing.Optional[str] = None

        # Contemplate a last-second rename of the columns
        column_names: typing.List[str]
        if self.output_column_names is not None:
            column_names = self.output_column_names
        else:
            column_names = self.column_names

        if self.output_format == self.OUTPUT_FORMAT_JSON:
            self.writeline("[")
            header = json.dumps(column_names, indent=None, separators=(',', ':')) + ","
        elif self.output_format == self.OUTPUT_FORMAT_JSON_MAP:
            self.writeline("[")
            return
        elif self.output_format == self.OUTPUT_FORMAT_JSON_MAP_COMPACT:
            self.writeline("[")
            return
        elif self.output_format == self.OUTPUT_FORMAT_JSONL:
            header = json.dumps(column_names, indent=None, separators=(',', ':'))
        elif self.output_format == self.OUTPUT_FORMAT_JSONL_MAP:
            return
        elif self.output_format == self.OUTPUT_FORMAT_JSONL_MAP_COMPACT:
            return
        elif self.output_format == self.OUTPUT_FORMAT_MD:
            header = "|"
            header2 = "|"
            col: str
            for col in column_names:
                col = "\\|".join(col.split("|"))
                header += " " + col + " |"
                header2 += " -- |"
            
        elif self.output_format in [self.OUTPUT_FORMAT_KGTK, self.OUTPUT_FORMAT_CSV]:
            header = self.column_separator.join(column_names)
        else:
            raise ValueError("KgtkWriter: header: Unrecognized output format '%s'." % self.output_format)

        # Write the column names to the first line.
        if self.verbose:
            print("header: %s" % header, file=self.error_file, flush=True)
        self.writeline(header)
        if header2 is not None:
            self.writeline(header2)

    def writeline(self, line: str):
        if self.gzip_thread is not None:
            self.gzip_thread.write(line + "\n") # Todo: use system end-of-line sequence?
        else:
            self.file_out.write(line + "\n") # Todo: use system end-of-line sequence?

    # Write the next list of edge values as a list of strings.
    # TODO: Convert integers, coordinates, etc. from Python types
    def write(self, values: typing.List[str],
              shuffle_list: typing.Optional[typing.List[int]]= None):

        if shuffle_list is not None:
            if len(shuffle_list) != len(values):
                # TODO: throw a better exception
                raise ValueError("The shuffle list is %d long but the values are %d long" % (len(shuffle_list), len(values)))

            shuffled_values: typing.List[str] = [""] * self.column_count
            idx: int
            for idx in range(len(shuffle_list)):
                shuffle_idx: int = shuffle_list[idx]
                if shuffle_idx >= 0:
                    shuffled_values[shuffle_idx] = values[idx]
            values = shuffled_values

        # Optionally fill missing trailing columns with empty values:
        if self.fill_missing_columns and len(values) < self.column_count:
            while len(values) < self.column_count:
                values.append("")

        # Optionally validate that the line contained the right number of columns:
        #
        # When we report line numbers in error messages, line 1 is the first line after the header line.
        line: str
        if self.require_all_columns and len(values) < self.column_count:
            line = self.column_separator.join(values)
            raise ValueError("Required %d columns in input line %d, saw %d: '%s'" % (self.column_count, self.line_count, len(values), line))
        if self.prohibit_extra_columns and len(values) > self.column_count:
            line = self.column_separator.join(values)
            raise ValueError("Required %d columns in input line %d, saw %d (%d extra): '%s'" % (self.column_count, self.line_count, len(values),
                                                                                                len(values) - self.column_count, line))
        if self.output_format == self.OUTPUT_FORMAT_KGTK:
            self.writeline(self.column_separator.join(values))
        elif self.output_format == self.OUTPUT_FORMAT_CSV:
            self.writeline(self.join_csv(values))
        elif self.output_format == self.OUTPUT_FORMAT_MD:
            self.writeline(self.join_md(values))
        elif self.output_format == self.OUTPUT_FORMAT_JSON:
            self.writeline(json.dumps(values, indent=None, separators=(',', ':')) + ",")
        elif self.output_format == self.OUTPUT_FORMAT_JSON_MAP:
            self.writeline(json.dumps(self.json_map(values), indent=None, separators=(',', ':')) + ",")
        elif self.output_format == self.OUTPUT_FORMAT_JSON_MAP_COMPACT:
            self.writeline(json.dumps(self.json_map(values, compact=True), indent=None, separators=(',', ':')) + ",")
        elif self.output_format == self.OUTPUT_FORMAT_JSONL:
            self.writeline(json.dumps(values, indent=None, separators=(',', ':')))
        elif self.output_format == self.OUTPUT_FORMAT_JSONL_MAP:
            self.writeline(json.dumps(self.json_map(values), indent=None, separators=(',', ':')))
        elif self.output_format == self.OUTPUT_FORMAT_JSONL_MAP_COMPACT:
            self.writeline(json.dumps(self.json_map(values, compact=True), indent=None, separators=(',', ':')))
        else:
            raise ValueError("Unrecognized output format '%s'." % self.output_format)

        self.line_count += 1
        if self.very_verbose:
            sys.stdout.write(".")
            sys.stdout.flush()

    def flush(self):
        if self.gzip_thread is None:
            self.file_out.flush()

    def close(self):
        if self.output_format == "json":
            if self.verbose:
                print("Closing the JSON list.", file=self.error_file, flush=True)
            self.writeline("]")

        if self.gzip_thread is not None:
            self.gzip_thread.close()
        else:
            self.file_out.close()


    def writemap(self, value_map: typing.Mapping[str, str]):
        """
        Write a map of values to the output file.
        """
        column_name: str

        # Optionally check for unexpected column names:
        if self.prohibit_extra_columns:
            for column_name in value_map.keys():
                if column_name not in self.column_name_map:
                    raise ValueError("Unexpected column name %s at data record %d" % (column_name, self.line_count))

        values: typing.List[str] = [ ]
        for column_name in self.column_names:
            if column_name in value_map:
                values.append(value_map[column_name])
            elif self.require_all_columns:
                # TODO: throw a better exception.
                raise ValueError("Missing column %s at data record %d" % (column_name, self.line_count))
            else:
                values.append("")
                
        self.write(values)

    def build_shuffle_list(self,
                           other_column_names: typing.List[str],
                           fail_on_unknown_column: bool = False)->typing.List[int]:
        results: typing.List[int] = [ ]
        column_name: str
        for column_name in other_column_names:
            if column_name in self.column_name_map:
                results.append(self.column_name_map[column_name])
            elif fail_on_unknown_column:
                # TODO: throw a better exception
                raise ValueError("Unknown column name %s when building shuffle list" % column_name)
            else:
                results.append(-1) # Means skip this column.
        return results
    
def main():
    """
    Test the KGTK edge file writer.

    TODO: full reader options.

    TODO:  --show-options
    """
    parser = ArgumentParser()
    parser.add_argument(dest="input_kgtk_file", help="The KGTK file to read", type=Path, nargs="?")
    parser.add_argument(dest="output_kgtk_file", help="The KGTK file to write", type=Path, nargs="?")
    parser.add_argument(      "--header-error-action", dest="header_error_action",
                              help="The action to take when a header error is detected  Only ERROR or EXIT are supported.",
                              type=ValidationAction, action=EnumNameAction, default=ValidationAction.EXIT)
    parser.add_argument(      "--gzip-in-parallel", dest="gzip_in_parallel", help="Execute gzip in a subthread.", action='store_true')
    parser.add_argument(      "--input-mode", dest="input_mode",
                              help="Determine the input KGTK file mode.", type=KgtkReaderMode, action=EnumNameAction, default=KgtkReaderMode.AUTO)
    parser.add_argument(      "--output-mode", dest="output_mode",
                              help="Determine the output KGTK file mode.", type=KgtkWriterMode, action=EnumNameAction, default=KgtkWriterMode.AUTO)
    parser.add_argument(      "--output-format", dest="output_format", help="The file format (default=kgtk)", type=str)
    parser.add_argument(      "--output-columns", dest="output_column_names", help="Rename all output columns. (default=%(default)s)", type=str, nargs='+')
    parser.add_argument(      "--old-columns", dest="old_column_names", help="Rename seleted output columns: old names. (default=%(default)s)", type=str, nargs='+')
    parser.add_argument(      "--new-columns", dest="new_column_names", help="Rename seleted output columns: new names. (default=%(default)s)", type=str, nargs='+')
    parser.add_argument("-v", "--verbose", dest="verbose", help="Print additional progress messages.", action='store_true')
    parser.add_argument(      "--very-verbose", dest="very_verbose", help="Print additional progress messages.", action='store_true')
    args = parser.parse_args()

    error_file: typing.TextIO = sys.stdout if args.errors_to_stdout else sys.stderr

    kr: KgtkReader = KgtkReader.open(args.input_kgtk_file,
                                     error_file=error_file,
                                     header_error_action=args.header_error_action,
                                     gzip_in_parallel=args.gzip_in_parallel,
                                     mode=args.input_mode,
                                     verbose=args.verbose, very_verbose=args.very_verbose)

    kw: KgtkWriter = KgtkWriter.open(kr.column_names,
                                     args.output_kgtk_file,
                                     error_file=error_file,
                                     gzip_in_parallel=args.gzip_in_parallel,
                                     header_error_action=args.header_error_action,
                                     mode=args.output_mode,
                                     output_format=args.output_format,
                                     output_column_names=args.output_column_names,
                                     old_column_names=args.old_column_names,
                                     new_column_names=args.new_column_names,
                                     verbose=args.verbose, very_verbose=args.very_verbose)

    line_count: int = 0
    row: typing.List[str]
    for row in kr:
        kw.write(row)
        line_count += 1
    kw.close()
    if args.verbose:
        print("Copied %d lines" % line_count, file=error_file, flush=True)


if __name__ == "__main__":
    main()
