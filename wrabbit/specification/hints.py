from wrabbit.parser.constants import ExecMode
from typing import Union


class Hint:
    class_ = None

    def __init__(self):
        pass

    def update(self, obj):
        pass

    def serialize(self):
        return {
            'class': self.class_
        }


class NextflowExecutionMode(Hint):
    class_ = "sbg:NextflowExecutionMode"

    def __init__(self, execution_mode: Union[str, ExecMode]):
        super().__init__()
        e_m = execution_mode
        if isinstance(execution_mode, ExecMode):
            e_m = ExecMode.value
        self.value = e_m

    def serialize(self):
        return {
            'class': self.class_,
            'value': self.value
        }

    @staticmethod
    def deserialize(hint):
        if isinstance(hint, Hint):
            return hint
        return NextflowExecutionMode(hint['value'])
