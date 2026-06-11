"""Output driver factory — creates the right driver for the destination."""

from exception.exception import OutputDriverNotRecognizeException
from helpers.output.driver.kafka import KafkaOutputDriver
from helpers.output.driver.elasticsearch import ElasticsearchOutputDriver
from helpers.output.driver.file import FileOutputDriver
from helpers.output.driver.std import StdOutputDriver

# Registry: destination string → driver class
_DRIVERS: dict[str, type] = {
    "kafka": KafkaOutputDriver,
    "elasticsearch": ElasticsearchOutputDriver,
    "file": FileOutputDriver,
    "std": StdOutputDriver,
}


class OutputDriverFactory:
    """Factory that instantiates the correct OutputDriver based on kwargs."""

    @staticmethod
    def create_output_driver(*args, **kwargs):
        destination = kwargs.get("destination")
        if not destination:
            raise OutputDriverNotRecognizeException("Destination is required (-d kafka|elasticsearch|file|std)")

        driver_cls = _DRIVERS.get(destination)
        if driver_cls is None:
            raise OutputDriverNotRecognizeException(
                f"Unknown destination '{destination}'. Use: {', '.join(_DRIVERS)}"
            )

        # Copy kwargs so we don't mutate the caller's dict
        driver_kwargs = dict(kwargs)

        # Pop destination-specific keys from the copy
        if destination == "kafka":
            return KafkaOutputDriver(
                topic=driver_kwargs.pop("output", "tiktok.posts.raw"),
                bootstrap_servers=driver_kwargs.pop("bootstrap_servers", "localhost:9092"),
                *args,
                **driver_kwargs,
            )
        elif destination == "elasticsearch":
            hosts = driver_kwargs.pop("elasticsearch_hosts", ["http://localhost:9200"])
            if isinstance(hosts, str):
                hosts = [hosts]
            from library.config import settings
            return ElasticsearchOutputDriver(
                index_name=driver_kwargs.pop("output", settings.elasticsearch.index_name),
                hosts=hosts,
                request_timeout=driver_kwargs.pop("request_timeout", settings.elasticsearch.request_timeout),
                max_retries=driver_kwargs.pop("max_retries", settings.elasticsearch.max_retries),
                *args,
                **driver_kwargs,
            )
        elif destination == "file":
            return FileOutputDriver(
                path=driver_kwargs.pop("output", None),
                *args,
                **driver_kwargs,
            )
        elif destination == "std":
            return StdOutputDriver(*args, **driver_kwargs)