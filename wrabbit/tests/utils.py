from typing import Optional, Union

from wrabbit.parser.nextflow import NextflowParser
from wrabbit.parser.constants import ExecMode
import yaml


def process_nextflow_app(path, execution_mode: Optional[Union[str, ExecMode]]=None):
    np = NextflowParser(
        workflow_path=path,
    )

    np.generate_sb_app(
        execution_mode=execution_mode
    )

    # print(np.sb_wrapper.dump())

    with open("test.yaml", 'w') as write_file:
        yaml.dump(np.sb_wrapper.dump(), write_file, indent=4, sort_keys=True)
