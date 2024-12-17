import os
import logging
import re
import yaml

from typing import (
    Union,
    TextIO,
)

from wrabbit.specification.node import (
    CompositeType,
    EnumType,
    ArrayType,
    StringType,
    FileType,
    DirectoryType,
    IntegerType,
    FloatType,
    BooleanType,
    RecordType,
    InputPort,
    Binding,
)
from wrabbit.specification.sbg import (
    ExecutorVersion,
)

from wrabbit.parser.constants import (
    NF_TO_CWL_PORT_MAP,
    SB_SCHEMA_DEFAULT_NAME,
    SB_SAMPLES_SCHEMA_DEFAULT_NAME,
    NF_SCHEMA_DEFAULT_NAME,
    README_DEFAULT_NAME,
    SKIP_NEXTFLOW_TOWER_KEYS,
    MAX_APP_DESCRIPTION_IMAGE_WIDTH,
    REGEX_MD_IMAGE,
    REGEX_HTML_IMAGE,
    REGEX_HTML_PICTURE,
    REGEX_NF_VERSION_PIL,
    REGEX_NF_VERSION_NUM,
    ImageMode,
)

from PIL import Image
from io import BytesIO
from bs4 import BeautifulSoup

import base64

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def nf_to_sb_input_mapper(port_id, port_data, category=None, required=False):
    """
    Convert a single input from Nextflow schema to SB schema
    """
    sb_input = InputPort(id_=port_id)
    sb_input.type_ = type_mapper(port_data, port_id)

    sb_input.binding = Binding(prefix=f'--{port_id}')

    if not required:
        sb_input.type_.to_optional()

    if category:
        sb_input.set_property('sbg:category', category)

    for nf_field, sb_field in NF_TO_CWL_PORT_MAP.items():
        if nf_field in port_data:
            value = port_data[nf_field]
            if value == ":" and nf_field == 'default':
                # Bug prevents running a task if an input's
                #  default value is exactly ":". This bug will likely be
                #  fixed at the time of release of this version.
                value = " :"
            sb_input.set_property(sb_field, value)

    return sb_input


def type_mapper(port_data: Union[str, dict], name=None):
    type_ = ""
    format_ = ""

    if isinstance(port_data, str):
        type_ = port_data

    if isinstance(port_data, dict):
        type_ = port_data.get('type', 'string')
        format_ = port_data.get('format', '')
        enum_symbols = port_data.get('enum', [])

        if enum_symbols:
            return EnumType(
                name=name,
                symbols=enum_symbols,
            )

    if isinstance(type_, str):
        if type_ == 'string' and 'path' in format_:
            if format_ == 'file-path':
                return FileType()
            if format_ == 'directory-path':
                return DirectoryType()
            if format_ == 'path':
                return FileType()
        if type_ == 'string':
            return StringType()
        if type_ == 'integer':
            return IntegerType()
        if type_ == 'number' or type_ == 'float':
            return FloatType()
        if type_ == 'boolean':
            return BooleanType()
        if type_ == 'array':
            item_data = port_data.get('items', {})
            return ArrayType(items=type_mapper(item_data))
        if type_ == 'object':
            # These are maps
            # They can only be provided through the -params-file
            # Skip these
            return StringType()
        return StringType()

    if isinstance(type_, list):
        temp_type_list = CompositeType()
        for m in type_:
            temp_type_list.extend(type_mapper(m))
        return temp_type_list


def subfolder_search(func, path, prefix, *args):
    # Check inner directories for a possible path
    list_of_paths = os.listdir(path)
    full_paths = [(d, os.path.join(path, d)) for d in list_of_paths]
    dirs = [(pfx, d) for pfx, d in full_paths if os.path.isdir(d)]
    if len(dirs) == 1:
        # If the contents of a directory is a single directory,
        # look through it
        for pfx, d in dirs:
            p = func(
                d, prefix=(*prefix, pfx), search_subfolders=True, *args
            )
            if p:
                return p
    return None


def subfolder_search_single(func, string):
    # Check inner directories for a possible path
    list_of_paths = os.listdir(string)
    full_paths = [os.path.join(string, d) for d in list_of_paths]
    dirs = [d for d in full_paths if os.path.isdir(d)]
    if len(dirs) == 1:
        # If the contents of a directory is a single directory,
        # look through it
        for d in dirs:
            p = func(
                d, search_subfolders=True
            )
            if p is not None:
                return p
    return None


def create_profile_enum(profiles: list):
    """
    If profiles are defined in the config file, this input stores the profiles
    They are added to the commandline as -profile foo,bar,foobar
    :param profiles: list of profiles
    :return: Profiles enum array input
    """
    return InputPort(
        id_="profile",
        type_=ArrayType(
            items=[EnumType(name="profile", symbols=profiles)], optional=True
        ),
        label="Profiles",
        doc="Select which profile(s) you want to use for task execution.",
        binding=Binding(
            prefix="-profile",
            item_separator=",",
            shell_quote=False,
        )
    )


def get_dict_depth(dict_, level=0):
    """
    Find the depth of the dictionary. Example:
    {'a': 1} - returns 0;
    {'a': {'b': 2}} - returns 1...

    :param dict_: input dictionary
    :param level: depth of the outer dict
    :return: int
    """
    n = level
    for k, v in dict_.items():
        if type(v) is dict:
            lv = get_dict_depth(v, level + 1)
            if lv > n:
                n = lv
    return n


def get_readme(path, prefix=(), search_subfolders=False):
    """
    Find readme file is there is one in the path folder
    """
    for file in os.listdir(path):
        if file.lower() == 'readme.md':
            return os.path.join(path, file)

    if search_subfolders:
        return subfolder_search(get_readme, path, prefix)
    return None


def get_tower_yml(path, prefix=(), search_subfolders=False):
    """
    Find tower.yml file is there is one in the path folder
    """
    list_of_paths = os.listdir(path)
    for file in list_of_paths:
        if file.lower() == 'tower.yml':
            return os.path.join(path, file)

    if search_subfolders:
        return subfolder_search(get_tower_yml, path, prefix)
    return None


def get_entrypoint(
        path, search_for='main.nf', search_subfolders=False, prefix=()
):
    """
    Auto find main.nf or similar file is there is one in the path folder.
    """
    possible_paths = []
    list_of_paths = os.listdir(path)

    for file in list_of_paths:
        if file.lower() == search_for.lower():
            return os.path.join(*prefix, file)

        if file.lower().endswith('.nf'):
            possible_paths.append(os.path.join(*prefix, file))

    if len(possible_paths) > 1:
        raise FileExistsError(
            'Detected more than 1 nextflow file in the root of the '
            'workflow-path. Please use `--entrypoint` to specify which script '
            'you want to use as the workflow entrypoint')
    elif len(possible_paths) == 1:
        return possible_paths[0]

    if search_subfolders and len(possible_paths) == 0:
        return subfolder_search(get_entrypoint, path, prefix, search_for)
    else:
        return None


def get_latest_sb_schema(path, prefix=(), search_subfolders=False):
    """
    Auto find sb_nextflow_schema file.
    """
    possible_paths = []
    list_of_paths = os.listdir(path)
    for file in list_of_paths:
        result = re.findall(
            fr"(.*{SB_SCHEMA_DEFAULT_NAME}\.?(\d+)?\.(ya?ml|json))", file
        )
        if result:
            result = result.pop(0)
            if not result[1]:
                prep = 0, result[0]
            else:
                prep = int(result[1]), result[0]
            possible_paths.append(prep)

    if possible_paths:
        latest = sorted(possible_paths).pop()[1]
        sb_schema_path = os.path.join(path, latest)
        logger.info(f"Located latest sb_nextflow_schema at <{sb_schema_path}>")
        return sb_schema_path
    else:
        if search_subfolders:
            return subfolder_search(get_latest_sb_schema, path, prefix)
        return None


def get_nf_schema(string, search_subfolders=False):
    path = os.path.join(string, NF_SCHEMA_DEFAULT_NAME)

    if os.path.exists(path):
        return path

    if search_subfolders:
        return subfolder_search_single(get_nf_schema, string)
    return None


def get_docs_file(path, prefix=(), search_subfolders=False):
    """
    Auto find readme file.
    """
    list_of_paths = os.listdir(path)
    for file in list_of_paths:
        if file.lower() == README_DEFAULT_NAME.lower():
            return os.path.join(path, file)
    else:
        if search_subfolders:
            return subfolder_search(get_docs_file, path, prefix)
        return None


def verify_version(sign: str, version: str, plus: str = ""):
    if plus == "+":
        sign = ">="

    if sign in ["%E2%89%A5", ">=", "!>="]:
        sign = ">="
    elif sign in ["!>", ">"]:
        sign = ">"
    elif sign in ["%E2%89%A4", "<=", "!<="]:
        sign = "<="
    elif sign in ["!<", "<"]:
        sign = "<"
    elif not sign:
        sign = "="
    print(
        f"Identified nextflow executor version requirement "
        f"'{sign} {version}'"
    )
    return sign, version


def get_executor_version(string: str):
    # from description
    result = re.findall(
        REGEX_NF_VERSION_PIL,
        string
    )

    if result:
        return verify_version(*result.pop(0))

    result = re.findall(
        REGEX_NF_VERSION_NUM,
        string
    )

    # from
    f_version = None, None
    if result:
        f_version = verify_version(*result[0])

    # to
    t_version = None, None
    if len(result) > 1:
        t_version = verify_version(*result[1])

    return ExecutorVersion(*f_version, *t_version)


def get_sample_sheet_schema(path, search_subfolders=False):
    ss_path = os.path.join(path, SB_SAMPLES_SCHEMA_DEFAULT_NAME)

    if os.path.exists(ss_path):
        logger.info(f"Located latest sample sheet schema at <{ss_path}>")
        return ss_path
    else:

        if search_subfolders:
            subfolder_search_single(get_sample_sheet_schema, path)
        return None


def get_config_files(path: str, search_subfolders=False) -> list:
    """
    Auto find config files.
    """
    paths = list_config_files(path, search_subfolders=search_subfolders)
    all_paths = paths
    include_pattern = re.compile(
        r'^includeConfig\s+[\'\"]([a-zA-Z_.\\/]+)[\'\"]'
    )

    for config_path in paths:
        with open(config_path, 'r') as config_file:
            for line in config_file.readlines():
                if include_path := re.findall(include_pattern, line):
                    chk = os.path.join(path, include_path[0])
                    if os.path.exists(chk):
                        all_paths.append(chk)

    return all_paths


def list_config_files(path: str, prefix=(), search_subfolders=False) -> list:
    """
    Auto find config files.
    """
    paths = []
    list_of_paths = os.listdir(path)
    for file in list_of_paths:
        if file.lower().endswith('.config'):
            paths.append(os.path.join(path, file))
    if paths:
        return paths

    if search_subfolders:
        return subfolder_search(list_config_files, path, prefix) or paths
    else:
        return paths


def find_config_section(file_path: str, section: str) -> str:
    section_text = ""
    found_section = False
    brackets = 0

    with open(file_path, 'r') as file:
        for line in file.readlines():
            if found_section:
                section_text += line
                brackets += line.count("{") - line.count("}")

            if brackets < 0:
                break

            if re.findall(section + r'(?:\s+|)\{', line):
                section_text += "{\n"
                found_section = True

                # Handle "section {}"
                if line.count("{") == line.count("}"):
                    section_text += "}"
                    break

    return section_text


def find_publish_params(file_path: str) -> set:
    sections = []
    section_text = ""
    brackets = 0
    found_section = False

    with open(file_path, 'r') as file:
        for line in file.readlines():
            if found_section:
                section_text += line
                brackets += line.count("[") - line.count("]")

            if brackets < 0:
                sections.append(section_text)
                brackets = 0
                section_text = ""
                found_section = False

            if re.findall(r'publishDir(?:\s|)=(?:\s+|)\[', line):
                section_text += "[\n"
                found_section = True

                # Handle "section []"
                if line.count("[") == line.count("]"):
                    section_text += line[line.index("[")+1:line.index("]")]
                    section_text += "]"
                    sections.append(section_text)
                    brackets = 0
                    section_text = ""
                    found_section = False

    params = set()

    for s in sections:
        if param := re.findall(r"path:[^$]+\$\{(?:\s+|)params\.(\w+)", s):
            params = params.union(set(param))

    return params


def parse_manifest(file_path: str) -> dict:
    manifest_text = find_config_section(file_path, 'manifest')

    key_val_pattern = re.compile(
        r'([a-zA-Z._]+)(?:\s+|)=(?:\s+|)(.+)'
    )

    manifest = dict(re.findall(key_val_pattern, manifest_text))
    for key, value in manifest.items():
        if value.startswith("'") and value.endswith("'"):
            manifest[key] = value.strip("'")
        elif value.startswith('"') and value.endswith('"'):
            manifest[key] = value.strip('"')

    return manifest


def parse_config_file(file_path: str) -> dict:
    profiles_text = find_config_section(file_path, 'profiles')

    # Extract profiles using regex
    profiles = {}
    block_pattern = re.compile(
        r'\s*(\w+)\s*{([^}]+)}', re.MULTILINE | re.DOTALL
    )
    key_val_pattern = re.compile(
        r'([a-zA-Z._]+)(?:\s+|)=(?:\s+|)([^\s]+)'
    )
    include_pattern = re.compile(
        r'includeConfig\s+[\'\"]([a-zA-Z_.\\/]+)[\'\"]'
    )

    blocks = re.findall(block_pattern, profiles_text)
    for name, content in blocks:
        settings = dict(re.findall(key_val_pattern, content))
        profiles[name] = settings
        include_path = re.findall(include_pattern, content)
        if include_path:
            profiles[name]['includeConfig'] = include_path
            include_path = include_path.pop()
            additional_path = os.path.join(
                os.path.dirname(file_path), include_path)
            params_text = find_config_section(additional_path, 'params')
            params = dict(re.findall(key_val_pattern, params_text))
            for param, val in params.items():
                profiles[name][f"params.{param}"] = val

    # return currently returns includeConfig and settings, which are not used
    # but could be used in the future versions of sbpack
    return profiles


def parse_sbg_image_notes(notes: str) -> dict:
    """
    Markdown notes for the sbg platform. They must start with "#sbg_".
    Acceptable notes are:
        width=X%
        (More to be added)

    Example: #sbg_align=center_width=30%
    :param notes: string
    :return: dictionary with parsed notes
    """
    sbg_notes = list(r.split("=") for r in notes.split("_")[1:])
    sbg_notes = dict(sbg_notes)
    if 'width' in sbg_notes:
        width = sbg_notes['width']
        if width.endswith("%"):
            width = float(width[:-1]) / 100
        else:
            width = float(width)
        sbg_notes['width'] = width
    return sbg_notes


def convert_images_to_md(path_to_docs: str, image_mode: ImageMode) -> str:
    """
    Converts html images and pictures, and markdown images to base64.
    base64 images can be used independent of the image source on the platform.

    :param path_to_docs: path to the Readme file that contains html,
        or MD images
    :param image_mode: ImageMode object
    :return: modified string with base64 images instead of html and MD
    """
    directory = os.path.dirname(path_to_docs)
    with open(path_to_docs, 'r') as docs:
        docs = docs.read()

    # replace html with md
    regex_string = re.compile(
        REGEX_HTML_PICTURE,
        re.DOTALL
    )
    reg_find = re.findall(regex_string, docs)

    for to_replace in reg_find:
        # Parse the HTML
        sbg_elements = []
        soup = BeautifulSoup(to_replace, 'html.parser')

        # Extract the <source> tag and its attributes
        source_srcset = ""
        img_title = 'picture'
        source_tag = soup.find('source')
        if source_tag is not None:
            source_media = source_tag.get('media')
            if source_media == f"(prefers-color-scheme: {image_mode.value})":
                source_srcset = source_tag.get('srcset')
            else:
                # Extract the <img> tag and its attributes
                img_tag = soup.find('img')
                if img_tag:
                    img_title = img_tag.get('title') or 'picture'
                    source_srcset = img_tag.get('src')
                    width = img_tag.get('width')
                    if width:
                        sbg_elements.append(f'width={width}')
        sbg_string = ""
        if sbg_elements:
            sbg_string = "#sbg_" + "_".join(sbg_elements)
        new_string = f"![{img_title}]({source_srcset}{sbg_string})"
        docs = docs.replace(to_replace, new_string)

    # convert <p><img> to md
    regex_string = re.compile(
        REGEX_HTML_IMAGE,
        re.DOTALL
    )
    reg_find = re.findall(regex_string, docs)

    for to_replace in reg_find:
        # Parse the HTML
        sbg_elements = []
        soup = BeautifulSoup(to_replace, 'html.parser')

        # Extract the <source> tag and its attributes
        img_src = ""
        img_title = 'image'
        img_tag = soup.find('img')
        if img_tag:
            img_title = img_tag.get('title') or 'image'
            img_src = img_tag.get('src')
            width = img_tag.get('width')
            if width:
                sbg_elements.append(f'width={width}')

        sbg_string = ""
        if sbg_elements:
            sbg_string = "#sbg_" + "_".join(sbg_elements)
        new_string = f"![{img_title}]({img_src}{sbg_string})"
        docs = docs.replace(to_replace, new_string)

    # Replace MD images with inline base64
    regex_string = REGEX_MD_IMAGE.format(
        opt="?:",
        image_mode=image_mode.value
    )
    reg_find = re.findall(regex_string, docs)
    for to_replace, hover, relative_path, ext, notes in reg_find:
        notes = parse_sbg_image_notes(notes)
        width = notes.get('width') or 1

        abs_path = os.path.join(directory, relative_path)
        if not os.path.exists(abs_path):
            continue
        # Open, resize, and encode the image
        with Image.open(abs_path) as img:
            original_width, original_height = img.size

            if original_width > MAX_APP_DESCRIPTION_IMAGE_WIDTH * width:
                new_height = int((MAX_APP_DESCRIPTION_IMAGE_WIDTH * width / original_width) * original_height)
                new_size = (
                    int(MAX_APP_DESCRIPTION_IMAGE_WIDTH * width), new_height
                )
                img = img.resize(
                    new_size, Image.Resampling.LANCZOS)

            buffer = BytesIO()
            # Save the resized image to a buffer
            img.save(buffer, format=ext.upper())
            buffer.seek(0)
            # Convert to Base64
            encoded_image = base64.b64encode(
                buffer.read()).decode("utf-8")

        # Wrap the Base64 string in Markdown syntax
        replacement = \
            f"![{hover}](data:image/{ext};base64,{encoded_image})"
        docs = docs.replace(to_replace, replacement)

    # Remove dark-mode images
    regex_string = REGEX_MD_IMAGE.format(
        opt="",
        image_mode=image_mode.opposite
    )
    reg_find = re.findall(regex_string, docs)
    for to_replace in reg_find:
        docs = docs.replace(to_replace, "")
    return docs


def make_output_type(key, output_dict, is_record=False) -> dict:
    """
    This creates an output of specific type based on information provided
    through output_dict.

    :param key:
    :param output_dict:
    :param is_record:
    :return:
    """

    converted_cwl_output = dict()

    file_pattern = re.compile(r'.*\.(\w+)$')
    folder_pattern = re.compile(r'[^.]+$')
    id_key = 'id'

    if is_record:
        id_key = 'name'

    name = key
    if 'display' in output_dict:
        name = output_dict['display']

    clean_id = re.sub(r'[^a-zA-Z0-9_]', "", name.replace(
        " ", "_")).lower()

    # Case 1: Output is a Record-type
    if get_dict_depth(output_dict) > 0:
        # this is a record, go through the dict_ recursively
        fields = [make_output_type(key, val, is_record=True)
                  for key, val in output_dict.items()]

        used_field_ids = set()

        for field in fields:
            base_field_id = field.get('name', 'Output')

            # Since name fields can be the same for multiple inputs,
            # correct the name if it has already been used.
            chk_id = base_field_id
            i = 1
            if chk_id in used_field_ids:
                chk_id = f"{base_field_id}_{i}"
                i += 1
            used_field_ids.add(chk_id)

            field['name'] = chk_id

        converted_cwl_output = {
            id_key: clean_id,
            "label": name,
            "type": RecordType(fields=fields, name=clean_id, optional=True)
        }

    # Case 2: Output is a File type
    elif re.fullmatch(file_pattern, key):
        # create a list of files output
        converted_cwl_output = {
            id_key: clean_id,
            "label": name,
            "type": ArrayType(items=[FileType()], optional=True),
            "outputBinding": {
                "glob": key
            }
        }

    # Case 3: Output is a folder type
    elif re.fullmatch(folder_pattern, key):
        # create a list of directories output
        converted_cwl_output = {
            id_key: clean_id,
            "label": name,
            "type": DirectoryType(optional=True),
            "outputBinding": {
                "glob": key,
                "loadListing": "deep_listing"
            }
        }
    return converted_cwl_output


def parse_output_yml(yml_file: TextIO) -> list:
    """
    Extracts output information from a YAML file, usually in tower.yml
    format.

    :param yml_file: path to YAML file.
    :return: list of outputs in CWL format.
    """
    outputs = list()
    yml_schema = yaml.safe_load(yml_file)

    for key, value in yml_schema.items():
        # Tower yml file can use "tower" key in the yml file to designate
        # some configurations tower uses. Since these are not output
        # definitions, we skip these.
        if key in SKIP_NEXTFLOW_TOWER_KEYS:
            continue

        outputs.append(
            make_output_type(key, value)
        )

    return outputs
