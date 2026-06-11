"""Input driver factory."""

from helpers.input.driver.std import StdInputDriver


class InputDriverFactory:
    """Factory that creates the correct InputDriver."""

    @staticmethod
    def create_input_driver(*args, **kwargs):
        # Currently only STD is supported; extensible for beanstalk, etc.
        return InputDriverFactory.create_std_input_driver(*args, **kwargs)

    @staticmethod
    def create_std_input_driver(*args, **kwargs):
        return StdInputDriver(kwargs.pop("input", None), *args, **kwargs)
