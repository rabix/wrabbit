import copy
from typing import Optional, Union

from wrabbit.specification.listing import (
    Listing,
)

from wrabbit.exceptions import (
    SbTypeMissmatch
)


class Requirement:
    class_ = None

    def __init__(
            self, class_: Optional[str] = None,
            **kwargs):
        self.listing = None
        if 'listing' in kwargs:
            listing = kwargs.pop('listing', [])
            self.listing = [Listing.deserialize(a) for a in listing]
        self.class_ = class_ or kwargs.pop('class', None)

    def update(self, obj):
        if not isinstance(obj, type(self)) and obj.class_ == self.class_:
            raise SbTypeMissmatch(
                f"Type missmatch. Cannot update {self} using {obj}"
            )
        if hasattr(obj, 'listing'):
            for o_l in obj.listing:
                self.add_listing(o_l)

    def add_listing(self, obj: Union[dict, str, Listing]):
        obj = Listing.deserialize(obj)

        if not self.listing:
            self.listing = list()

        if obj not in self.listing:
            self.listing.append(obj)

    def serialize(self):
        temp = {
            'class': self.class_,
        }
        if self.listing:
            temp['listing'] = [a.serialize() for a in self.listing]

        return temp

    @staticmethod
    def deserialize(requirement):
        if isinstance(requirement, Requirement):
            return requirement
        req = copy.deepcopy(requirement)
        class_ = None
        if "class" in req:
            class_ = req.pop('class')
        elif "class_" in req:
            class_ = req.pop("class_")

        return Requirement(class_=class_, **req)
