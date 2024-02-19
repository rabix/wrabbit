from typing import Optional, Union


class Listing:
    def __init__(
            self,
            entry: Optional[str],
            entryname: Optional[str] = None,
            writable: Optional[bool] = None,
    ):
        self.entry = entry
        self.entryname = entryname
        self.writable = writable or False

    def is_writable(self):
        return self.writable

    def serialize(self):
        if self.entry and not self.entryname and not self.writable:
            return self.entry
        else:
            temp = dict()
            if self.entry:
                temp['entry'] = self.entry
            if self.entryname:
                temp['entryname'] = self.entryname
            if self.writable:
                temp['writable'] = self.writable
            return temp


def convert_to_listing(listing: Union[str, dict, Listing]) -> Listing:
    if isinstance(listing, Listing):
        return listing
    elif isinstance(listing, str):
        return Listing(listing)
    return Listing(**listing)
