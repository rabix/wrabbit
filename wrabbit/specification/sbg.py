import copy
from typing import Optional
from packaging.version import Version
from wrabbit.parser.constants import MINIMUM_SUPPORTED_NF_VERSION


class Link:
    def __init__(
            self,
            id_: Optional[str],
            label: Optional[str] = None,
    ):
        self.id_ = id_
        self.label = label

    def serialize(self):
        temp = dict()
        if self.id_:
            temp['id'] = self.id_
        if self.label:
            temp['label'] = self.label
        return temp

    @staticmethod
    def deserialize(link):
        if isinstance(link, Link):
            return link
        l = copy.deepcopy(link)
        id_ = None
        if "id" in l:
            id_ = l.pop('id')
        elif "id_" in l:
            id_ = l.pop("id_")

        return Link(id_=id_, **l)


class ExecutorVersion:
    def __init__(
            self,
            from_sign: Optional[str] = None,
            from_version: Optional[str] = None,
            to_sign: Optional[str] = None,
            to_version: Optional[str] = None,
    ):
        self.from_sign = from_sign
        self.from_version = from_version
        self._from_version = None

        # Edge versions are pre-release, same as rc.
        #  Some nf executor versions use: edge, some rc#
        if from_version:
            self._from_version = Version(from_version.replace("edge", "rc1"))

        self.to_sign = to_sign
        self.to_version = to_version
        self._to_version = None
        if to_version:
            self._to_version = Version(to_version.replace("edge", "rc1"))

    def correct_version(self):
        if not self.from_version:
            return
        if self._from_version >= Version(MINIMUM_SUPPORTED_NF_VERSION):
            return
        if self.from_sign in ['<', '=', '<=']:
            raise ValueError(
                f"Executor version '{self.from_sign} {self.from_version}' is "
                f"not compatible with Sevenbridges/Velsera powered platforms."
            )
        if self.to_sign in ['<'] and \
                self._to_version < Version(MINIMUM_SUPPORTED_NF_VERSION):
            raise ValueError(
                f"Executor version '{self.from_sign} {self.from_version}', "
                f"'{self.to_sign} {self.to_version}' is "
                f"not compatible with Sevenbridges/Velsera powered platforms."
            )

        print(
            "Executor version set to the minimum supported version for "
            "Sevenbridges/Velsera powered platforms."
        )
        self.from_version = MINIMUM_SUPPORTED_NF_VERSION
        self._from_version = Version(MINIMUM_SUPPORTED_NF_VERSION)

    def serialize(self):
        return self.from_version
