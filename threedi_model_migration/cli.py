"""Console script for threedi_model_migration."""
import click
import sys


@click.command()
def main(args=None):
    """Console script for threedi_model_migration."""
    click.echo(
        "Replace this message by putting your code into "
        "threedi_model_migration.cli.main"
    )
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
