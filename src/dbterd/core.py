import json
import yaml
import re
import os

def loadModel(catalog_path, schema_path):
    """Loads the dbt catalog and schema. The schema selected is the one that will be used to generate the ERD diagram.

    Args:
        catalog_path (Path): Path to dbt catalog
        schema_path (Path): Path to dbt schema

    Returns:
        dict, dict: Return schema and catalog dicts.  
    """    
    with open(catalog_path) as f:
        catalog = json.load(f)


    with open(schema_path, 'r') as f:
        schema = yaml.safe_load(f)

    return catalog, schema


def createTable(dbml_path, model, schema, docs_path):
    """Create a table in the dbml file. 

    Args:
        dbml_path (dbml file): The file where to store the table
        model (dbt model): The dbt model to extract the table and columns from
    """    
    name = model["metadata"]["name"]
    columns = list(model["columns"].keys())
    start = "{"
    end = "}"

    # Create a dictionary from the docs.md file
    docs_dict = {}

    if os.path.exists(docs_path):
        # Loop through all files in the docs folder
        for filename in os.listdir(docs_path):
            # Check if the file is a markdown file
            if filename.endswith(".md"):
                # Load the content of the markdown file
                with open(os.path.join(docs_path, filename), "r") as docs_file:
                    docs_content = docs_file.read()

                docs_pattern = re.compile(r"{% docs (.*?) %}(.*?){% enddocs %}", re.DOTALL)

                for match in docs_pattern.finditer(docs_content):
                    key = match.group(1).strip()
                    value = match.group(2).strip()
                    docs_dict[key] = value.replace("'", "")

    # Replace jinja variables in .yml with values in the docs.md files
    def replace_jinja_variables(text, docs_dict):
        jinja_variable_pattern = re.compile(r'\'?{{\s*doc\(\s*\"(\w+)\"\s*\)\s*}}\'?\n?')


        def replace_variable(match):
            variable_name = match.group(1)
            return docs_dict.get(variable_name, match.group(0))

        return jinja_variable_pattern.sub(replace_variable, text)

    # Find the model in the schema YAML file
    schema_model = None
    for m in schema['models']:
        if m['name'] == name.lower():
            schema_model = m
            break
    
    model_description = ""
    if "description" in schema_model:
        if "{{" in schema_model["description"]:
            model_description = "Note: '" + replace_jinja_variables(schema_model["description"], docs_dict) + "'"
        else:
            model_description = "Note: '" + schema_model["description"].replace("'","") + "'"

    dbml_path.write(f"Table {name} {start} \n")

    for column_name in columns:
        column = model["columns"][column_name] 
        name = column["name"]
        dtype = column["type"]

        # Set default values
        schema_column = None
        column_tests_and_description = ""
        
        # Find the column in the schema_model
        for c in schema_model["columns"]:
            if c["name"] == name.lower():
                schema_column = c
                break

        if schema_column != None:
            column_docs_list = []
            if "tests" in schema_column:
                schema_tests = schema_column["tests"]
                for t in schema_tests:
                    if t == "not_null":
                        column_docs_list.append("not null")
                    elif t == "unique":
                        column_docs_list.append("unique, pk")

            if "description" in schema_column:
                if "{{" in schema_column["description"]:
                    column_docs_list.append("note: '" + replace_jinja_variables(schema_column["description"], docs_dict) + "'")
                else:
                    column_docs_list.append("note: '" + schema_column["description"].replace("'","") + "'")

            if column_docs_list:
                column_tests_and_description = '[' + ', '.join(column_docs_list) + ']'
            
        dbml_path.write(f"{name} {dtype} {column_tests_and_description} \n")
    dbml_path.write(f"{model_description} \n{end} \n")
    
    
def createRelatonship(dbml_path, schema):
    """Create a relationship in the dbml file. Loops over all columns to find relationship tests and saves them to the dbml file

    Args:
        dbml_path (dbml file): The file where to store the table
        schema (dbt schema): The dbt schema to extract relationships from 
    """    
    for model in schema["models"]:
        for column in model["columns"]:
            if "tests" in column:
                tests = column["tests"]
                for test in tests:
                    if isinstance(test, dict): 
                        if "relationships" in test:
                            relationship = test["relationships"]
                            # errors here if relationship test is not in the right format in the dbt yml
                            r1 = relationship["to"].upper()
                            r1 = re.findall(r"('.*?')", r1, re.DOTALL)[0].replace("'", "")
                            r1_field = relationship["field"].upper()
                            
                            r2 = model["name"].upper()
                            r2_field = column["name"].upper()
                            dbml_path.write(f"Ref: {r1}.{r1_field} > {r2}.{r2_field} \n")
                            


def genereatedbml(schema_path, catalog_path, dbml_path, docs_path):
    """Create dbml file for a dbt schema

    Args:
        catalog_path (Path): Path to dbt catalog (ex. target/catalog.json)
        schema_path (Path): Path to dbt schema (ex. models/core.yml)
        dbml_path (Path): Pat to save dbml file 
    """    
    catalog, schema = loadModel(catalog_path, schema_path)
    model_names = catalog["nodes"]
    
    tables = [model["name"].upper() for model in schema["models"]]
    
    with open(dbml_path, "w") as dbml_file:
        for model_name in model_names:
            model = catalog["nodes"][model_name]
            if model["metadata"]["name"] in tables: 
                createTable(dbml_file, model, schema, docs_path)
        createRelatonship(dbml_file, schema)