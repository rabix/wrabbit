import copy
from typing import Union, Optional, Any

from wrabbit.specification.node_types import (
    NodeType,
    convert_to_type,
)

from wrabbit.specification.binding import (
    Binding,
    convert_to_binding,
)


class Port:
    _port_type = ''

    def __init__(
            self, id_: Optional[str] = None,
            type_: Union[str, dict, list, NodeType] = None,
            label: Optional[str] = None,
            doc: Optional[str] = None,
            binding: Union[dict, Binding] = None,
            **kwargs
    ):
        self.id_ = id_ or kwargs.pop('id', None)
        self.type_ = type_ or kwargs.pop('type', None)
        if self.type_:
            self.type_ = convert_to_type(self.type_)

        self.label = label
        self.doc = doc

        self.binding = binding or None
        if self.binding:
            self.binding = convert_to_binding(binding)
        self._custom_properties = kwargs or dict()

    def set_property(self, key: str, value: Any):
        if key in ('id', 'id_'):
            self.id_ = value
        elif key in ('type', 'type_'):
            self.type_ = convert_to_type(value)
        elif key == 'binding':
            self.binding = convert_to_binding(value)
        elif key == 'doc':
            self.doc = value
        elif key == 'label':
            self.label = value
        else:
            self._custom_properties.update({
                key: value
            })

    def serialize(self):
        skip_keys = ('__len__', '_custom_properties')
        dict_ = copy.deepcopy(self.__dict__)

        temp = {
            'id': dict_.pop('id_'),
        }

        if self.binding:
            temp[f'{self._port_type}Binding'] = dict_.pop(
                'binding'
            ).serialize()
        if self.type_:
            temp['type'] = dict_.pop('type_').serialize()

        for key, value in dict_.items():
            if key in skip_keys:
                continue

            if hasattr(value, 'serialize'):
                temp[key] = value.serialize()
            else:
                temp[key] = value

        for key, value in sorted(self._custom_properties.items()):
            if key in skip_keys:
                continue

            if hasattr(value, 'serialize'):
                temp[key] = value.serialize()
            else:
                temp[key] = value

        return temp

    def update(self, inp: Union[dict]):
        for key, value in inp.items():
            self.set_property(key, value)


class InputPort(Port):
    _port_type = 'input'


class OutputPort(Port):
    _port_type = 'output'
