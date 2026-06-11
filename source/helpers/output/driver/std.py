"""Standard-output driver — prints to stdout."""

from helpers.output.driver import OutputDriver


class StdOutputDriver(OutputDriver):
    """Print output to stdout."""

    name = "std"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def put(self, output: str, **kwargs):
        print(output)

    def close(self):
        pass
