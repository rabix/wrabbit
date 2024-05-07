from typing import Optional


class Binding:
    def __init__(
            self, prefix: Optional[str] = None,
            item_separator: Optional[str] = None,
            shell_quote: Optional[bool] = None,
            glob: Optional[str] = None,
            **kwargs
    ):
        self.prefix = prefix
        self.item_separator = item_separator
        self.shell_quote = shell_quote
        self.glob = glob

    def serialize(self):
        temp = dict()
        if self.prefix is not None:
            temp['prefix'] = self.prefix
        if self.item_separator is not None:
            temp['itemSeparator'] = self.item_separator
        if self.shell_quote is not None:
            temp['shellQuote'] = self.shell_quote
        if self.glob is not None:
            temp['glob'] = self.glob

        return temp

    @staticmethod
    def deserialize(binding):
        if isinstance(binding, Binding):
            return binding
        return Binding(**binding)
