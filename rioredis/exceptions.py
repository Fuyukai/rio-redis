from typing import Union


class RedisError(Exception):
    """
    Raised when there is an error in a command.
    """
    def __init__(self, err_text: str, command: Union[str, bytes]):
        self.error = err_text
        self.command = command

    def __repr__(self):
        return f"Redis error for command {self.command!r}: {str(self.error)}"

    def __str__(self):
        return self.__repr__()
