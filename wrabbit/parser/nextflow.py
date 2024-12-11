from typing import (
    Union,
    Optional,
)

from wrabbit.parser.utils import (
    get_config_files,
    get_nf_schema,
    parse_config_file,
    parse_manifest,
    get_executor_version,
    create_profile_enum,
    nf_to_sb_input_mapper,
    get_tower_yml,
    parse_output_yml,
    get_entrypoint,
    get_docs_file,
    get_sample_sheet_schema,
    find_publish_params,
    convert_images_to_md,
)

from wrabbit.parser.constants import (
    sample_sheet,
    ExecMode,
    ImageMode,
    SB_SCHEMA_DEFAULT_NAME,
    EXTENSIONS,
    NF_TO_CWL_CATEGORY_MAP,
    GENERIC_FILE_ARRAY_INPUT,
    NF_PARAMS_FILE_INPUT,
    AUX_FILES_REQUIREMENT,
    INLINE_JS_REQUIREMENT,
    GENERIC_NF_OUTPUT_DIRECTORY,
    SAMPLE_SHEET_FILE_ARRAY_INPUT,
    SAMPLE_SHEET_SWITCH,
    LOAD_LISTING_REQUIREMENT,
    NFCORE_OUTPUT_DIRECTORY_ID,
)

from wrabbit.specification.hints import (
    NextflowExecutionMode,
)

from wrabbit.specification.sbg import (
    ExecutorVersion
)

from wrabbit.exceptions import (
    SampleSheetError, MalformedConfigException, ErrorMessages,
    MalformedSchemaException
)

import yaml
import os
import json

from wrabbit.wrapper.wrapper import SbWrapper


class NextflowParser:
    nf_config_files: list
    nf_schema_path: Optional[str]
    readme_path: Optional[str]
    sb_samplesheet_schema: Optional[str]

    def __init__(
            self, workflow_path: str,
            sb_doc: Optional[str] = None,
            label: Optional[str] = None,
            entrypoint: Optional[str] = None,
            executor_version: Optional[str] = None,
            sb_package_id: Optional[str] = None,
            readme_path: Optional[str] = None,
            search_subfolders: Optional[bool] = False,
            image_mode: Optional[ImageMode] = ImageMode.light
    ):
        self.search_subfolders = search_subfolders
        self.sb_wrapper = SbWrapper(label)
        self.workflow_path = workflow_path

        # Locate nextflow files in the package if possible
        self.init_config_files()
        self.nf_schema_path = get_nf_schema(
            self.workflow_path, search_subfolders=self.search_subfolders
        )

        if readme_path:
            self.readme_path = readme_path
        else:
            self.readme_path = get_docs_file(
                self.workflow_path, search_subfolders=self.search_subfolders
            )

        self.sb_samplesheet_schema = get_sample_sheet_schema(
            self.workflow_path, search_subfolders=self.search_subfolders
        )

        self.sb_doc = sb_doc
        self.image_mode = image_mode

        # app contents
        self.entrypoint = entrypoint
        self.executor_version = executor_version
        self.sb_package_id = sb_package_id

    def init_config_files(self):
        """
        Config may be initialized multiple times while working with a code
        package in case a new config file is generated with nf-core lib.
        """
        self.nf_config_files = get_config_files(
            self.workflow_path, search_subfolders=self.search_subfolders
        ) or []

    def generate_sb_inputs(self, execution_mode=None):
        """
        Generate SB inputs schema
        """
        if execution_mode:
            if isinstance(execution_mode, ExecMode):
                execution_mode = execution_mode.value

        # ## Add profiles to the input ## #

        profiles = dict()

        for path in self.nf_config_files:
            try:
                profiles.update(parse_config_file(path))
            except Exception:
                raise MalformedConfigException(
                    ErrorMessages.CONFIG_FILE_INVALID
                )

        profiles_choices = sorted(list(set(profiles.keys())))

        if profiles:
            self.sb_wrapper.safe_add_input(
                create_profile_enum(profiles_choices)
            )

        # Optional inputs due to profiles
        # optional_inputs = []
        # for profile_id, profile_contents in profiles.items():
        #     for key in profile_contents.keys():
        #         if 'params.' in key:
        #             input_ = key.rsplit('params.', 0)
        #             optional_inputs.extend(input_)
        # optional_inputs = set(optional_inputs)

        # ## Add inputs ## #
        if self.nf_schema_path:
            with open(self.nf_schema_path, 'r') as f:
                try:
                    nf_schema = yaml.safe_load(f)
                except Exception:
                    raise MalformedSchemaException(
                        ErrorMessages.SCHEMA_JSON_INVALID
                    )

            for p_key, p_value in nf_schema.get('properties', {}).items():
                self.sb_wrapper.safe_add_input(
                    nf_to_sb_input_mapper(p_key, p_value))

            definitions_dict = nf_schema.get(
                'definitions', {})
            definitions_dict.update(nf_schema.get(
                '$defs', {}
            ))
            for def_name, definition in definitions_dict.items():
                # Nextflow inputs schema contains multiple definitions where
                # each definition contains multiple properties
                category = dict()

                for nf_field, sb_field in NF_TO_CWL_CATEGORY_MAP.items():
                    if nf_field in definition:
                        category[sb_field] = definition[nf_field]

                input_category = 'Inputs'
                if 'title' in definition:
                    input_category = category['sbg:title']

                for port_id, port_data in definition.get(
                        'properties', {}).items():
                    req = False
                    # if port_id in definition.get('required', []) and \
                    #         port_id not in optional_inputs:
                    #     req = True

                    self.sb_wrapper.safe_add_input(nf_to_sb_input_mapper(
                        port_id,
                        port_data,
                        category=input_category,
                        required=req,
                    ))

        # Remap publishDir to string type
        found_publish_dir = False
        if temp := self.sb_wrapper.get_input(NFCORE_OUTPUT_DIRECTORY_ID):
            print(f"Detected publishDir input --{temp.id_}. Remapping to "
                  f"string type input.")
            temp.set_property('type', 'string')
            found_publish_dir = True

        if not found_publish_dir:
            # Look for publish dir in config files
            pub_dir_params = set()
            for config_file in self.nf_config_files:
                pub_dir_params = pub_dir_params.union(
                    find_publish_params(config_file)
                )

            pub_dir_params = list(pub_dir_params)

            if pub_dir_params:
                if len(pub_dir_params) == 1:
                    id_ = pub_dir_params.pop()
                    print(f"Detected publishDir input --{id_}. Remapping to "
                          f"string type input.")
                    if temp := self.sb_wrapper.get_input(id_):
                        temp.set_property('type', 'string')
                    else:
                        print(f"Input --{id_} not found.")

                if len(pub_dir_params) > 1:
                    print(f"Detected multiple possible publishDir inputs. "
                          f"Skipping remapping.")

        # Add the generic file array input - auxiliary files
        self.sb_wrapper.safe_add_input(GENERIC_FILE_ARRAY_INPUT)
        self.sb_wrapper.safe_add_input(NF_PARAMS_FILE_INPUT)

        if execution_mode == ExecMode.single:
            self.sb_wrapper.add_requirement(AUX_FILES_REQUIREMENT)

        self.sb_wrapper.add_requirement(INLINE_JS_REQUIREMENT)

    def generate_sb_outputs(self):
        """
        Generate SB output schema
        """
        if get_tower_yml(self.workflow_path):
            for output in parse_output_yml(
                    open(get_tower_yml(
                        self.workflow_path, search_subfolders=True
                    ))
            ):
                self.sb_wrapper.safe_add_output(output)

        # if the only output is reports, or there are no outputs, add generic
        if len(self.sb_wrapper.outputs) == 0 or \
                (len(self.sb_wrapper.outputs) == 1 and
                 'reports' in self.sb_wrapper.outputs):
            self.sb_wrapper.safe_add_output(GENERIC_NF_OUTPUT_DIRECTORY)

    def generate_app_data(self):

        for file in self.nf_config_files:
            manifest_data = parse_manifest(file)

            self.entrypoint = self.entrypoint or manifest_data.get(
                'mainScript', None)

            if not self.executor_version:
                executor_version = manifest_data.get('nextflowVersion', None)
                if executor_version:
                    executor_version = get_executor_version(executor_version)
                self.executor_version = executor_version
            else:
                print(f"Using provided version {self.executor_version}")

            tk_author = manifest_data.get('author', None)
            if not self.sb_wrapper.toolkit_author:
                self.sb_wrapper.add_toolkit_author(tk_author)

            home_page = manifest_data.get('homePage', None)
            if home_page:
                pl = {
                    'id': home_page,
                    'label': "Home Page",
                }
                self.sb_wrapper.add_link(pl)

            if manifest_data:
                # Stop searching if manifest is found
                break

        if not self.entrypoint:
            self.entrypoint = get_entrypoint(
                self.workflow_path, search_subfolders=self.search_subfolders
            )

        if not self.executor_version and self.sb_doc:
            self.executor_version = get_executor_version(self.sb_doc)

        if self.executor_version and \
                isinstance(self.executor_version, ExecutorVersion):
            # Confirm if the executor version is valid.
            # If it is below 22.10.1 it is not supported.
            self.executor_version.correct_version()

        # step2: add links

        links = []  # sbg:links
        # {
        #     "id": "https://github.com/alexdobin/STAR",
        #     "label": "STAR"
        # },
        # manifest.homePage

    def nf_schema_build(self):
        """
        To be replaced before used.
        """
        pass

    def generate_sb_app(
            self, sb_entrypoint: Optional[str] = None,
            executor_version: Optional[str] = None,
            sb_package_id: Optional[str] = None,
            execution_mode: Optional[Union[str, ExecMode]] = None,
            sample_sheet_schema: Optional[str] = None,
    ):
        """
        Generate an SB app for a nextflow workflow, OR edit the one created and
        defined by the user
        """

        self.sb_wrapper.cwl_version = 'None'
        self.sb_wrapper.class_ = 'nextflow'

        self.generate_app_data()
        self.nf_schema_build()
        self.generate_sb_inputs(execution_mode)
        self.generate_sb_outputs()

        if sample_sheet_schema or self.sb_samplesheet_schema:
            self.parse_sample_sheet_schema(open(
                sample_sheet_schema or self.sb_samplesheet_schema))

        self.sb_wrapper.set_app_content(
            code_package=sb_package_id or self.sb_package_id,
            entrypoint=sb_entrypoint or self.entrypoint,
            executor_version=executor_version or self.executor_version,
        )

        if execution_mode:
            self.sb_wrapper.add_hint(NextflowExecutionMode(
                execution_mode=execution_mode
            ))

        if self.sb_doc:
            self.sb_wrapper.add_docs(self.sb_doc)
        elif self.readme_path:
            docs = convert_images_to_md(self.readme_path, self.image_mode)
            self.sb_wrapper.add_docs(docs)

    def parse_sample_sheet_schema(self, path):
        """
        Example sample sheet:
        sample_sheet_input: input_sample_sheet  # taken from app wrapper
        sample_sheet_name: samplesheet.csv
        header:
          - sample_id
          - fastq1
          - fastq2
        rows:
          - sample_id
          - path
          - path
        defaults:
          - ""
          - ""
          - ""
        group_by: sample_id
        format_: csv

        """
        schema = yaml.safe_load(path)

        sample_sheet_input = schema.get('sample_sheet_input')
        sample_sheet_name = schema.get('sample_sheet_name', 'samplesheet')
        header = schema.get('header', 'null')

        # fix rows
        rows = schema.get('rows')
        for i, r in enumerate(rows):
            if "." not in r:
                if r == 'path':
                    n = 0
                    new_r = f'files[{n}].path'
                    while new_r in rows:
                        n += 1
                        new_r = f'files[{n}].path'
                    rows[i] = new_r
                else:
                    rows[i] = f'files[0].metadata.{r}'

        defaults = schema.get('defaults', 'null')

        # fix group by
        group_by = schema.get('group_by')
        if type(group_by) is str:
            group_by = [group_by]
        for i, gb in enumerate(group_by):

            if "." not in gb:
                if gb in ['file', 'none']:
                    group_by[i] = 'file.path'
                else:
                    group_by[i] = f'file.metadata.{gb}'

        format_ = schema.get('format_', None)

        if format_ and not sample_sheet_name.endswith(format_):
            sample_sheet_name += f".{format_}".lower()

        if not format_ and not sample_sheet_name.endswith(['.tsv', '.csv']):
            raise SampleSheetError(ErrorMessages.UNKNOWN_SAMPLE_SHEET_FORMAT)

        if not format_ and sample_sheet_name.endswith(['.tsv', '.csv']):
            format_ = sample_sheet_name.split('.').pop().lower()

        if format_.lower() not in ['tsv', 'csv']:
            raise SampleSheetError(ErrorMessages.UNKNOWN_SAMPLE_SHEET_FORMAT)

        # Step 1:
        # add a new input to the pipeline
        #    - new input must not clash with other inputs by ID
        # Ensure that the new input is unique

        # Create the sample sheet file array input
        file_input = self.sb_wrapper.safe_add_input(
            SAMPLE_SHEET_FILE_ARRAY_INPUT
        )
        file_input_id = file_input.id_

        # Step 2:
        # add argument for sample sheet
        #    - requires: sample sheet input (sample_sheet_input),
        #                file input (ss_file_input)
        #    - if the sample sheet is provided on input,
        #      do not generate a new ss
        ss_input = self.sb_wrapper.get_input(
            sample_sheet_input
        )
        ss_input.set_property('loadContents', True)
        prefix = ss_input.binding.prefix
        ss_input.unbind()

        self.sb_wrapper.add_argument(
            {
                "prefix": prefix,
                "shellQuote": False,
                "valueFrom": SAMPLE_SHEET_SWITCH.format(
                    file_input=f"inputs.{file_input_id}",
                    sample_sheet=f"inputs.{sample_sheet_input}",
                    sample_sheet_name=sample_sheet_name,
                )
            }
        )

        # Step 3:
        # add file requirement
        #    - requires: sample sheet schema
        #    - add InitialWorkDirRequirement if there are none
        #    - if there are, append the entry to listing
        ss = sample_sheet(
            file_name=sample_sheet_name,
            sample_sheet_input=f"inputs.{sample_sheet_input}",
            format_=format_,
            input_source=f"inputs.{file_input_id}",
            header=header,
            rows=rows,
            defaults=defaults,
            group_by=group_by,
        )

        self.sb_wrapper.add_requirement(ss)
        self.sb_wrapper.add_requirement(INLINE_JS_REQUIREMENT)
        self.sb_wrapper.add_requirement(LOAD_LISTING_REQUIREMENT)

    def dump_sb_wrapper(self, out_format=EXTENSIONS.yaml):
        """
        Dump SB wrapper for nextflow workflow to a file
        """
        # self.generate_sb_app()
        print('Writing sb nextflow schema file...')
        basename = SB_SCHEMA_DEFAULT_NAME
        counter = 0
        sb_wrapper_path = os.path.join(
            self.workflow_path,
            f'{basename}.{out_format}'
        )

        while os.path.exists(sb_wrapper_path):
            counter += 1
            sb_wrapper_path = os.path.join(
                self.workflow_path,
                f'{basename}.{counter}.{out_format}'
            )

        print(f"Schema written to file <{sb_wrapper_path}>")

        if out_format in EXTENSIONS.yaml_all:
            with open(sb_wrapper_path, 'w') as f:
                yaml.dump(self.sb_wrapper.dump(), f, indent=4, sort_keys=True)
        elif out_format in EXTENSIONS.json_all:
            with open(sb_wrapper_path, 'w') as f:
                json.dump(self.sb_wrapper.dump(), f, indent=4, sort_keys=True)
