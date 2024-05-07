from typing import Optional, Union, Any

from wrabbit.exceptions import (
    UnrecognizedSbTypeError, MissingSbTypeError
)

import copy

from wrabbit.specification.binding import (
    Binding,
)


class NodeType:
    type_ = None

    def __init__(
            self, optional: Optional[bool] = False, **kwargs):
        self.optional = optional

    def is_optional(self):
        return self.optional

    def to_optional(self):
        self.optional = True

    def to_required(self):
        self.optional = False

    def serialize(self):
        if self.optional:
            return [
                "null",
                self._dict
            ]
        else:
            return self._dict

    @property
    def _dict(self):
        return self.type_


class CompositeType(list, NodeType):
    def serialize(self):
        # validate node types
        for t in self._dict:
            if not isinstance(t, NodeType):
                raise UnrecognizedSbTypeError(
                    f"Unable to recognize node type of {t}"
                )

        if self.optional:
            return [
                "null",
                *[t.serialize() for t in self._dict]
            ]
        else:
            return self._dict

    @property
    def _dict(self):
        return self


class EnumType(NodeType):
    type_ = "enum"

    def __init__(
            self, name: Optional[str] = None,
            symbols: Optional[list] = None,
            **kwargs
    ):
        """
        :param symbols: Symbols will be converted to string.
        """
        self.name = name
        self.symbols = [str(s) for s in symbols]
        super().__init__(**kwargs)

    @property
    def _dict(self):
        return {
            "type": self.type_,
            "name": self.name,
            "symbols": self.symbols
        }


class ArrayType(NodeType):
    type_ = "array"

    def __init__(self, items: Union[list, NodeType], **kwargs):
        self.items = items
        super().__init__(**kwargs)

    @property
    def _dict(self):
        temp = {
            "type": self.type_,
        }
        if isinstance(self.items, NodeType):
            temp["items"] = self.items.serialize()
        elif isinstance(self.items, list):
            temp["items"] = [i.serialize() for i in self.items]
            if len(temp["items"]) == 1:
                temp["items"] = temp["items"][0]
        return temp

    def serialize(self):
        if self.optional:
            return [
                "null",
                self._dict
            ]
        else:
            return self._dict


class RecordType(NodeType):
    type_ = "record"

    def __init__(self, fields: list, name: str, **kwargs):
        self.fields = [Field.deserialize(f) for f in fields]
        self.name = name
        super().__init__(**kwargs)

    @property
    def _dict(self):
        return {
            "type": self.type_,
            "name": self.name,
            "fields": self.fields,
        }


class StringType(NodeType):
    type_ = "string"


class FileType(NodeType):
    type_ = "File"


class DirectoryType(NodeType):
    type_ = "Directory"


class IntegerType(NodeType):
    type_ = 'int'


class FloatType(NodeType):
    type_ = 'float'


class BooleanType(NodeType):
    type_ = 'boolean'


def convert_to_type(type_: Union[str, dict, list, NodeType]) -> NodeType:
    if isinstance(type_, NodeType):
        return type_

    is_array = False
    is_optional = False
    of_type = None

    if isinstance(type_, str):
        if '[]' in type_:
            is_array = True
        if '?' in type_:
            is_optional = True

        if type_.startswith('File'):
            of_type = FileType()
        elif type_.startswith('Directory'):
            of_type = DirectoryType()
        elif type_.startswith('int'):
            of_type = IntegerType()
        elif type_.startswith('string'):
            of_type = StringType()
        elif type_.startswith('float'):
            of_type = FloatType()
        elif type_.startswith('bool'):
            of_type = BooleanType()
        else:
            raise UnrecognizedSbTypeError(
                f"Unable to map string type {type_} to an SbNodeType"
            )
    elif isinstance(type_, dict):
        t = type_.get('type', None)
        if not t:
            raise MissingSbTypeError

        if t == 'array':
            of_type = ArrayType(items=convert_to_type(type_['items']))
        elif t == 'enum':
            name = type_.get('name', None)
            symbols = type_.get('symbols', [])
            of_type = EnumType(name=name, symbols=symbols)
        elif t == 'record':
            name = type_.get('name', None)
            fields = type_.get('fields', [])
            of_type = RecordType(name=name, fields=fields)
        else:
            raise UnrecognizedSbTypeError(
                f"Unable to map dict type {type_} to an SbNodeType"
            )

    elif isinstance(type_, list):
        if 'null' in type_:
            is_optional = True
            type_.remove('null')

        types_in_the_list = []
        for member in type_:
            types_in_the_list.append(convert_to_type(member))

        if len(types_in_the_list) > 1:
            of_type = CompositeType(*types_in_the_list)
        else:
            of_type = types_in_the_list.pop()

    if of_type is None:
        raise MissingSbTypeError(
            f"Unable to map type {type_} to a SbNodeType"
        )

    if is_array:
        of_type = ArrayType(items=of_type)

    if is_optional:
        of_type.to_optional()

    return of_type


class BasePort:
    _port_type = ''

    def __init__(
            self,
            type_: Union[str, dict, list, NodeType] = None,
            label: Optional[str] = None,
            doc: Optional[str] = None,
            binding: Union[dict, Binding] = None,
            **kwargs
    ):
        self.type_ = type_ or kwargs.pop('type', None)
        if self.type_:
            self.type_ = convert_to_type(self.type_)

        self.label = label
        self.doc = doc

        self.binding = binding or None
        if self.binding:
            self.binding = Binding.deserialize(binding)
        self._custom_properties = dict()
        for key, value in kwargs.items():
            self.set_property(key, value)

    def set_property(self, key: str, value: Any):
        if key in ('type', 'type_'):
            self.type_ = convert_to_type(value)
        elif key in ('binding', f'{self._port_type}Binding'):
            self.binding = Binding.deserialize(value)
        elif key == 'doc':
            self.doc = value
        elif key == 'label':
            self.label = value
        else:
            self._custom_properties.update({
                key: value
            })

    def update(self, inp: Union[dict]):
        for key, value in inp.items():
            self.set_property(key, value)


class Port(BasePort):
    def __init__(self, id_: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.id_ = id_ or kwargs.pop('id', None)

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
            if value is None:
                continue
            if key in skip_keys:
                continue

            if hasattr(value, 'serialize'):
                temp[key] = value.serialize()
            else:
                temp[key] = value

        for key, value in sorted(self._custom_properties.items()):
            if value is None:
                continue
            if key in skip_keys:
                continue

            if hasattr(value, 'serialize'):
                temp[key] = value.serialize()
            else:
                temp[key] = value

        return temp

    def set_property(self, key: str, value: Any):
        if key in ('id', 'id_'):
            self.id_ = value
        else:
            super().set_property(key, value)

    @staticmethod
    def deserialize(port):
        if isinstance(port, Port):
            return port

        binding = port.get('inputBinding', None)
        if binding:
            return InputPort(
                binding=binding,
                **port
            )

        binding = port.get('outputBinding', None)
        if binding:
            return OutputPort(
                binding=binding,
                **port
            )

        return Port(
            **port
        )


class Field(BasePort):
    def __init__(self, name: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.name = name or kwargs.pop('name', None)

    def serialize(self):
        skip_keys = ('__len__', '_custom_properties')
        dict_ = copy.deepcopy(self.__dict__)

        temp = {
            'name': dict_.pop('name'),
        }

        if self.binding:
            temp[f'{self._port_type}Binding'] = dict_.pop(
                'binding'
            ).serialize()
        if self.type_:
            temp['type'] = dict_.pop('type_').serialize()

        for key, value in dict_.items():
            if value is None:
                continue
            if key in skip_keys:
                continue

            if hasattr(value, 'serialize'):
                temp[key] = value.serialize()
            else:
                temp[key] = value

        for key, value in sorted(self._custom_properties.items()):
            if value is None:
                continue
            if key in skip_keys:
                continue

            if hasattr(value, 'serialize'):
                temp[key] = value.serialize()
            else:
                temp[key] = value

        return temp

    def set_property(self, key: str, value: Any):
        if key == 'name':
            self.name = value
        else:
            super().set_property(key, value)

    @staticmethod
    def deserialize(field):
        if isinstance(field, Field):
            return field

        binding = field.get('inputBinding', None)
        if binding:
            return InputField(
                binding=binding,
                **field
            )

        binding = field.get('outputBinding', None)
        if binding:
            return OutputField(
                binding=binding,
                **field
            )

        return Field(
            **field
        )


class InputPort(Port):
    _port_type = 'input'


class OutputPort(Port):
    _port_type = 'output'


class InputField(Field):
    _port_type = 'input'


class OutputField(Field):
    _port_type = 'output'
