from typing import Optional, Union

from wrabbit.exceptions import (
    UnrecognizedSbTypeError, MissingSbTypeError
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
        self.name = name
        self.symbols = symbols
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
        self.fields = fields
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
