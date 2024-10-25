from typing import Union, Optional

from wrabbit.specification.node import (
    InputPort, OutputPort,
)
from wrabbit.specification.requirements import (
    Requirement,
)
from wrabbit.specification.sbg import (
    Link,
)

import logging

from wrabbit.wrapper.utils import recursive_serialize


class SbWrapper:
    label = None
    inputs = dict()
    outputs = dict()
    app_content = dict()
    class_ = None
    cwl_version = None
    arguments = None
    requirements = None
    hints = None
    doc = None
    revision_note = None
    links = None
    toolkit_author = None
    wrapper_author = None
    licence = None
    other_keys = None

    def __init__(self, label: Optional[str] = None):
        self.label = label

    def get_input(self, id_) -> Optional[InputPort]:
        if id_ not in self.inputs:
            logging.warning(f'Input with id <{id_}> not found.')
            return None

        return self.inputs[id_]

    def add_input(self, inp: Union[dict, InputPort]):
        inp = InputPort.deserialize(inp)

        if inp.id_ in self.inputs:
            logging.warning(f'Input with id <{inp.id_}> already exists. '
                            f'Skipping...')

        self.inputs[inp.id_] = inp

    def safe_add_input(self, inp: Union[dict, InputPort]) -> InputPort:
        inp = InputPort.deserialize(inp)

        temp_id = inp.id_
        i = 0
        while temp_id in self.inputs:
            i += 1
            temp_id = f"{inp.id_}_{i}"

        inp.set_property('id', temp_id)
        self.add_input(inp)

        return inp

    def update_input(self, inp: Union[dict, InputPort]):
        if isinstance(inp, InputPort):
            inp = inp.serialize()
        id_ = inp.get('id')

        if self.get_input(id_):
            self.get_input(id_).update(inp)

    def remove_input(self, id_):
        if self.get_input(id_):
            self.inputs.pop(id_)

    def get_output(self, id_) -> Optional[OutputPort]:
        if id_ not in self.outputs:
            logging.warning(f'Output with id <{id_}> not found.')
            return None

        return self.outputs[id_]

    def add_output(self, out: Union[dict, OutputPort]):
        out = OutputPort.deserialize(out)

        if out.id_ in self.outputs:
            logging.warning(f'Output with id <{out.id_}> already exists. '
                            f'Skipping...')

        self.outputs[out.id_] = out

    def safe_add_output(self, out: Union[dict, OutputPort]) -> OutputPort:
        out = OutputPort.deserialize(out)

        temp_id = out.id_
        i = 0
        while temp_id in self.outputs:
            i += 1
            temp_id = f"{out.id_}_{i}"

        out.set_property('id', temp_id)
        self.add_output(out)

        return out

    def update_output(self, out: Union[dict, OutputPort]):
        if isinstance(out, OutputPort):
            out = out.serialize()
        id_ = out.get('id')

        if self.get_output(id_):
            self.get_output(id_).update(out)

    def remove_output(self, id_):
        if self.get_output(id_):
            self.outputs.pop(id_)

    def add_requirement(self, requirement: [dict, Requirement]):
        requirement = Requirement.deserialize(requirement)

        if not self.requirements:
            self.requirements = list()

        for req in self.requirements:
            if req.class_ == requirement.class_:
                req.update(requirement)
                break
        else:
            # add new class
            self.requirements.append(requirement)

    def set_app_content(
            self, code_package=None, entrypoint=None, executor_version=None,
            **kwargs
    ):
        payload = dict()

        if hasattr(executor_version, 'serialize'):
            executor_version = executor_version.serialize()

        if code_package:
            payload['code_package'] = code_package
        if entrypoint:
            payload['entrypoint'] = entrypoint
        if executor_version:
            payload['executor_version'] = executor_version

        payload.update(kwargs)

        self.app_content.update(payload)

    def add_argument(self, arg):
        if not self.arguments:
            self.arguments = list()
        self.arguments.append(arg)

    def add_hint(self, hint):
        if not self.hints:
            self.hints = list()
        self.hints.append(hint)

    def add_docs(self, doc):
        self.doc = doc

    def add_revision_note(self, note):
        self.revision_note = note

    def add_link(self, link: Union[Link, dict]):
        link = Link.deserialize(link)

        if not self.links:
            self.links = dict()

        if link in self.links:
            logging.warning(f'Link with id <{link}> already exists. '
                            f'Skipping...')

        self.links[link.id_] = link

    def add_toolkit_author(self, author):
        self.toolkit_author = author

    def add_wrapper_author(self, author):
        self.wrapper_author = author

    def add_licence(self, licence):
        self.licence = licence

    def add_other_keys(self, other_keys):
        self.other_keys = other_keys

    def load(self, schema):
        keys_to_remove = []
        self.label = schema.get('label', None)
        keys_to_remove.append('label')

        s_inputs = schema.get('inputs', [])
        for input_ in s_inputs:
            self.add_input(input_)
        keys_to_remove.append('inputs')

        s_outputs = schema.get('outputs', [])
        for output in s_outputs:
            self.add_output(output)
        keys_to_remove.append('outputs')

        s_app_content = schema.get('app_content', dict())
        self.set_app_content(**s_app_content)
        keys_to_remove.append('app_content')

        self.class_ = schema.get('class', None)
        keys_to_remove.append('class')
        self.cwl_version = schema.get('cwlVersion', None)
        keys_to_remove.append('cwlVersion')

        s_arguments = schema.get('arguments', [])
        for argument in s_arguments:
            self.add_argument(argument)
        keys_to_remove.append('arguments')

        s_requirements = schema.get('requirements', [])
        for requirement in s_requirements:
            self.add_requirement(requirement)
        keys_to_remove.append('requirements')

        s_hints = schema.get('hints', [])
        for hint in s_hints:
            self.add_hint(hint)
        keys_to_remove.append('hints')

        s_doc = schema.get('doc', None)
        if s_doc:
            self.add_docs(s_doc)
        keys_to_remove.append('doc')

        s_tk_author = schema.get('sbg:toolAuthor', None)
        if s_tk_author:
            self.add_toolkit_author(s_tk_author)
        keys_to_remove.append('sbg:toolAuthor')

        s_w_author = schema.get('sbg:wrapperAuthor', None)
        if s_w_author:
            self.add_wrapper_author(s_w_author)
        keys_to_remove.append('sbg:wrapperAuthor')

        s_links = schema.get('sbg:links', None)
        if s_links:
            for link in s_links:
                self.add_link(link)
        keys_to_remove.append('sbg:links')

        s_licence = schema.get('sbg:license', None)
        if s_licence:
            self.add_licence(s_licence)
        keys_to_remove.append('sbg:license')

        s_revision_note = schema.get('sbg:revisionNote', None)
        if s_revision_note:
            self.add_revision_note(s_revision_note)
        keys_to_remove.append('sbg:revisionNote')

        other_keys = {
            key: value for key, value in schema.items() if
            key not in keys_to_remove
        }
        if other_keys:
            self.add_other_keys(other_keys)

    def dump(self):
        wrapper = dict()

        if self.label:
            wrapper['label'] = self.label

        if self.app_content:
            wrapper['app_content'] = self.app_content

        if self.doc:
            wrapper['doc'] = self.doc

        wrapper['inputs'] = list()
        for key, value in self.inputs.items():
            new_val = value.serialize()
            new_val['id'] = key
            wrapper['inputs'].append(new_val)

        wrapper['outputs'] = list()
        for key, value in self.outputs.items():
            new_val = value.serialize()
            new_val['id'] = key
            wrapper['outputs'].append(new_val)

        if self.arguments:
            wrapper['arguments'] = self.arguments

        if self.class_:
            wrapper['class'] = self.class_

        if self.cwl_version:
            wrapper['cwlVersion'] = self.cwl_version

        if self.requirements:
            wrapper['requirements'] = self.requirements

        if self.hints:
            wrapper['hints'] = self.hints

        if self.toolkit_author:
            wrapper['sbg:toolAuthor'] = self.toolkit_author

        if self.wrapper_author:
            wrapper['sbg:wrapperAuthor'] = self.wrapper_author

        if self.links:
            wrapper['sbg:links'] = [v for k, v in self.links.items()]

        if self.licence:
            wrapper['sbg:license'] = self.licence

        if self.revision_note:
            wrapper['sbg:revisionNotes'] = self.revision_note

        if self.other_keys:
            wrapper.update(self.other_keys)

        return recursive_serialize(wrapper)
