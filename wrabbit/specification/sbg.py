import copy
from typing import Optional


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
