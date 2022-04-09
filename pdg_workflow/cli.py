"""Console script for pdg_workflow."""
import sys
import click
from pdg_workflow import init_parsl
from pdg_workflow import run_pdg_workflow

@click.command()
def main(args=None):
    """Console script for pdg_workflow."""
    click.echo("Replace this message by putting your code into "
               "pdg_workflow.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    
    init_parsl()
    #run_pdg_workflow()

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
