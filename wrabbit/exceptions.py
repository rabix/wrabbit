"""
Exception
    WrabbitError
        ParseError
            SampleSheetError
            MalformedSchemaException
            MalformedConfigException
        SbTypeError
            UnrecognizedSbTypeError
            MissingSbTypeError
            SbTypeMissmatch
"""


class WrabbitError(Exception):
    pass


class ParserError(WrabbitError):
    pass


class SampleSheetError(ParserError):
    pass


class MalformedSchemaException(ParserError):
    pass


class MalformedConfigException(ParserError):
    pass


class SbTypeError(WrabbitError, TypeError):
    pass


class UnrecognizedSbTypeError(SbTypeError):
    pass


class MissingSbTypeError(SbTypeError):
    pass


class SbTypeMissmatch(SbTypeError):
    pass


class ErrorMessages:
    SCHEMA_JSON_INVALID = (
        'Failed parsing nextflow_schema.json, make sure that the file is '
        'properly formatted'
    )
    CONFIG_FILE_INVALID = (
        'nextflow.config file malformed or missing referenced '
        'configuration files'
    )
    UNKNOWN_SAMPLE_SHEET_FORMAT = (
        'Unrecognized sample sheet format. '
        'Please specify one of "tsv" or "csv" in the '
        'sample sheet schema file.'
    )
