import copy
import logging
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
            sign: Optional[str] = None,
            version: Optional[str] = None,
    ):
        self.sign = sign
        self.version = None
        if version:
            self.version = Version(version.replace("edge", "rc1"))
        # Edge versions are pre-release, same as rc.
        #  Some nf executor versions use: edge, some rc#

    def correct_version(self):
        if not self.version:
            raise ValueError(
                "Executor Version was not set."
            )
        if self.version >= Version(MINIMUM_SUPPORTED_NF_VERSION):
            return
        if self.sign in ['<', '=', '<=']:
            raise ValueError(
                f"Executor version {self.sign}{self.version.base_version} is "
                f"not compatible with Sevenbridges/Velsera powered platforms."
            )
        logging.info(
            "Executor version set to the minimum supported version for "
            "Sevenbridges/Velsera powered platforms."
        )
        self.version = Version(MINIMUM_SUPPORTED_NF_VERSION)

    def serialize(self):
        if self.version:
            return self.version.base_version
        return None
