import click
from .core import genereatedbml
import subprocess


@click.command()
@click.argument('schema_path', type=str)
@click.argument('catalog_path', type=str)
@click.argument('erd_path', type=str)
@click.argument('docs_path', type=str)
@click.argument('project_name', type=str)
@click.argument('visualize', type=bool)
def cli(schema_path, catalog_path, erd_path, docs_path, project_name, visualize):
    """"Generate a DBML file from a dbt project and visualize it with dbdocs.io"""
    try:
        genereatedbml(schema_path, catalog_path, erd_path, docs_path)
        if visualize:
            subprocess.run(f"dbdocs build {erd_path} --project {project_name}", text=True, shell=True)
    
    except Exception as e:
        print(f"Unexpected error: {e}")
        
    
