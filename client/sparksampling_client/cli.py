import click
from sparksampling_client.config import reset_config
from sparksampling_client.session import Session


@click.command()
def init():
    assert Session()
    print("Pyspark sampling client initialized!")


@click.command()
def reset():
    reset_config()


@click.command()
@click.argument("json_file")
def apply(json_file):
    session = Session()
    session.apply_file(json_file)


@click.group()
def cli():
    pass


cli.add_command(init)
cli.add_command(reset)
cli.add_command(apply)

if __name__ == "__main__":
    cli()
