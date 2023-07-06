import click
from .core import DbmlDocs
import subprocess


@click.command()
@click.argument('schema_path', type=str)
@click.argument('catalog_path', type=str)
@click.argument('dbml_path', type=str)
@click.argument('docs_path', type=str)
@click.argument('project_name', type=str)
@click.argument('visualize', type=bool)
def cli(schema_path, catalog_path, dbml_path, docs_path, project_name, visualize):
    """"Generate a DBML file from a dbt project and visualize it with dbdocs.io"""
    
    subprocess.run(f"dbt docs generate", text=True, shell=True)

    dbml_docs = DbmlDocs(schema_path, catalog_path, docs_path, dbml_path)
    dbml_docs.GenerateDbml()

    if visualize == "launch-dbdocs":
        subprocess.run(f"dbdocs build {dbml_path} --project {project_name}", text=True, shell=True)
    else:
        subprocess.run(f"dbml-renderer -i {dbml_path} -o ERD.svg", text=True, shell=True)
        
    
