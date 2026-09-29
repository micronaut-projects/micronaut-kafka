# tag::imports[]
from micronaut.context import ApplicationContextBuilder, ApplicationContextConfigurer
from micronaut.context.annotation import ContextConfigurer
# end::imports[]


# tag::clazz[]
@ContextConfigurer
class RuntimeBootstrapServers(ApplicationContextConfigurer):

    def configure(self, builder: ApplicationContextBuilder) -> None:
        builder.properties({"kafka.bootstrap.servers": self.resolve_bootstrap_servers()})

    @staticmethod
    def resolve_bootstrap_servers() -> str:
        return "localhost:9092"
# end::clazz[]
