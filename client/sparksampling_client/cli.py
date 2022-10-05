import click

from sparksampling_client.session import Session


@click.command()
def echo():
    print('Pyspark sampling client initialized!')


@click.command()
@click.argument('json_file')
def apply(json_file):
    session = Session()


@click.group()
def cli():
    pass


cli.add_command(echo)
cli.add_command(apply)

if __name__ == '__main__':
    cli()
