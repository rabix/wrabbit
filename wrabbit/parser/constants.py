from enum import Enum
from wrabbit.parser import read_js_template


# ############################## Generic Bits ############################### #
# keep track of what extensions are applicable for processing

class EXTENSIONS:
    yaml = 'yaml'
    yml = 'yml'
    json = 'json'
    cwl = 'cwl'

    yaml_all = [yaml, yml, cwl]
    json_all = [json]
    all_ = [yaml, yml, json, cwl]


# ############################ CWL Standard Bits ############################ #
# A generic SB input array of files that should be available on the
# instance but are not explicitly provided to the execution as wdl params.

# This looks best on the platform
MAX_APP_DESCRIPTION_IMAGE_WIDTH = 1000

SAMPLE_SHEET_FUNCTION = read_js_template("sample_sheet_generator.js")
SAMPLE_SHEET_SWITCH = read_js_template("sample_sheet_switch.js")
SB_SAMPLES_SCHEMA_DEFAULT_NAME = "samplesheet_schema.yaml"

GENERIC_FILE_ARRAY_INPUT = {
    "id": 'auxiliary_files',
    "type": [
        'null',
        {
            'type': 'array',
            'items': 'File'
        }
    ],
    "label": 'Auxiliary files',
    "doc": 'List of files not added as explicit workflow inputs but '
           'required for workflow execution.'
}

NF_PARAMS_FILE_INPUT = {
    "id": 'params_file',
    "type": [
        'null',
        'File'
    ],
    "label": 'Params files',
    "inputBinding": {
        "prefix": '-params-file'
    },
    "doc": 'Provide parameters through a JSON format input file.',
    "sbg:fileTypes": "JSON"
}

SAMPLE_SHEET_FILE_ARRAY_INPUT = {
    "id": "file_input",
    "type": [
        'null',
        {
            'type': 'array',
            'items': 'File'
        }
    ],
    "label": "Input files",
    "doc": "List of files that will be used to autogenerate the sample sheet "
           "that is required for workflow execution."
}

GENERIC_NF_OUTPUT_DIRECTORY = {
    "id": "nf_publishdir",
    "type": [
        'null',
        {
            'type': 'array',
            'items': 'File'
        }
    ],
    "label": "Publish Directory",
    "doc": "This is a template output. "
           "You can modify the glob pattern to make outputs more specific.",
    "outputBinding": {
        'glob': "*"
    }
}

GENERIC_WDL_OUTPUT_DIRECTORY = {
    "id": "output_txt",
    "type": {
        'type': 'array',
        'items': 'File'
    },
    "doc": "This is a template output. "
           "Please change glob to directories specified in "
           "publishDir in the workflow.",
    "outputBinding": {
        'glob': "*.txt"
    }
}

# Requirements for sb wrapper
INLINE_JS_REQUIREMENT = {
    'class': "InlineJavascriptRequirement"
}
LOAD_LISTING_REQUIREMENT = {
    'class': "LoadListingRequirement"
}
AUX_FILES_REQUIREMENT = {
    'class': "InitialWorkDirRequirement",
    'listing': [
        "$(inputs.auxiliary_files)"
    ],
}


def sample_sheet(
        file_name, sample_sheet_input, format_, input_source, header, rows,
        defaults, group_by):
    basename = ".".join(file_name.split(".")[:-1])
    ext = file_name.split(".")[-1]
    new_name = f"{basename}.new.{ext}"

    return {
        'class': "InitialWorkDirRequirement",
        'listing': [
            {
                "entryname": f"${{ return {sample_sheet_input} ? "
                             f"{sample_sheet_input}.nameroot + '.new' + "
                             f"{sample_sheet_input}.nameext : "
                             f"'{file_name}' }}",
                "entry": SAMPLE_SHEET_FUNCTION.format_map(locals()),
                "writable": False
            }
        ]
    }


# ############################## Nextflow Bits ############################## #
# Keys that should be skipped when parsing nextflow tower yaml file

REGEX_NF_VERSION_PIL = r"\[Nextflow]\([^(]+(%E2%89%A5|%E2%89%A4|=|>|<)(\d{2}\.\d+\.\d+)[^)]+\)"
REGEX_NF_VERSION_NUM = r"((?:[!><=]+|))(\d{2}\.\d+\.\d+)((?:\+|))"

NF_CONFIG_DEFAULT_NAME = 'nextflow.config'
NF_SCHEMA_DEFAULT_NAME = 'nextflow_schema.json'
SB_SCHEMA_DEFAULT_NAME = 'sb_nextflow_schema'
README_DEFAULT_NAME = 'README.md'
MINIMUM_SUPPORTED_NF_VERSION = "21.10.0"
NFCORE_OUTPUT_DIRECTORY_ID = 'outdir'

# Mappings of nextflow input fields to SB input fields
#  nextflow_key: cwl_key mapping
NF_TO_CWL_PORT_MAP = {
    'default': 'sbg:toolDefaultValue',
    'description': 'label',
    'help_text': 'doc',
    'mimetype': 'format',
    'fa_icon': 'sbg:icon',
    'pattern': 'sbg:pattern',
    'hidden': 'sbg:hidden',
}

# Mappings of nextflow definition fields to SB category fields
#  nextflow_key: cwl_key mapping
NF_TO_CWL_CATEGORY_MAP = {
    'title': 'sbg:title',
    'description': 'sbg:doc',
    'fa_icon': 'sbg:icon',
}

# What keys to skip from the tower.yml file
SKIP_NEXTFLOW_TOWER_KEYS = [
    'tower',
    'mail',
]


class ExecMode(Enum):
    single = 'single-instance'
    multi = 'multi-instance'

    def __str__(self):
        return self.value

# ############################ Image generation ############################# #
# This part contains constants related to image generation for Markdown

# Find Markdown images
REGEX_MD_IMAGE = r'((?:!|)\[([^\[\]]+)]\(([^\[\]\(\)]+\.((?:jpe?g|png)))({opt}#gh-{image_mode}-mode-only|)((?:#sbg_.*|))\))'
# Find <p><img> in HTML
REGEX_HTML_IMAGE = r'(?:<p(?:[^>]+)>.*?)<img[^>]+>(?:.*?</p>)'
# Find <h><picture> in HTML
REGEX_HTML_PICTURE = r'(?:<h\d>).*?<picture>.*?</picture>.*?(?:</h\d>)'


class ImageMode(Enum):
    light = 'light'
    dark = 'dark'

    @property
    def opposite(self):
        return self.light if self.value == self.dark else self.dark

    def __str__(self):
        return self.value
