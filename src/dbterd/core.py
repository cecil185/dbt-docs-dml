import json
import yaml
import re
import os
from pathlib import Path

class DbmlDocs:
    def __init__(self, schema_path, catalog_path, docs_path, dbml_path):
        """
        This class creates a DBML (Database Markup Language) file from other files typically found in a dbt project.

        Attributes:
            schema (dict): The loaded dbt YAML file used to parse column names, tests, and descriptions.
            catalog (dict): The loaded dbt catalog.json file used to parse table names.
            docs_dict (dict): A dictionary created from parsed markdown files in the docs_path.
            dbml_path (str): The path to the output DBML file.
        
        Args:
            schema_path (str or Path): Path to the dbt YAML file.
            catalog_path (str or Path): Path to the dbt catalog.json file.
            docs_path (str or Path): Path to the directory containing docs markdown files.
            dbml_path (str or Path): Path to the output DBML file.
        """
        
        self.schema = self.LoadSchema(schema_path)
        self.catalog = self.LoadCatalog(catalog_path)
        self.docs_dict = self.ParseDocsMarkdownFiles(docs_path)
        self.dbml_path = dbml_path

    def LoadSchema(self, schema_path):
        """Loads the dbt schema. The schema selected is the one that will be used to generate the ERD diagram.

        Args:
            schema_path (Path): Path to dbt schema

        Returns:
            schema (dict): Schema dict
        """    
        try:
            with open(schema_path, 'r') as f:
                schema = yaml.safe_load(f)
        except (FileNotFoundError, yaml.YAMLError) as e:
            print(f"Failed to load schema: {e}")
            return None

        return schema

    def LoadCatalog(self, catalog_path):
        """Loads the dbt catalog.

        Args:
            catalog_path (Path): Path to dbt catalog

        Returns:
            catalog (dict): Catalog dict 
        """    
        try:
            with open(catalog_path, 'r') as f:
                catalog = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Failed to load catalog: {e}")
            return None

        return catalog
    
    # def ReplaceJinjaVariables(self, text):
    #     jinja_variable_pattern = re.compile(r'\'?{{\s*doc\(\s*\"(\w+)\"\s*\)\s*}}\'?\n?')

    #     replace_variable = self.docs_dict.get(match.group(1), match.group(0))

    #     return jinja_variable_pattern.sub(replace_variable, text)
    
    def ReplaceJinjaVariables(self, text):
        jinja_variable_pattern = re.compile(r'\'?{{\s*doc\(\s*\"(\w+)\"\s*\)\s*}}\'?\n?')
        return jinja_variable_pattern.sub(lambda match: self.docs_dict.get(match.group(1), ""), text)

    
    def ParseDocsMarkdownFiles(self, docs_path):
        # Create a dictionary from the docs.md file
        docs_dict = {}

        docs_pattern = re.compile(r"{% docs (.*?) %}(.*?){% enddocs %}", re.DOTALL)

        docs_path = Path(docs_path)

        if os.path.exists(docs_path):
            # Loop through all files in the docs folder
            for filename in docs_path.iterdir():
                # Check if the file is a markdown file
                if filename.suffix == ".md":
                    try:
                        # Load the content of the markdown file
                        with open(filename, "r", encoding="utf-8") as docs_file:
                            docs_content = docs_file.read()

                        for match in docs_pattern.finditer(docs_content):
                            key = match.group(1).strip()
                            value = match.group(2).strip()
                            docs_dict[key] = value.replace("'", "")
                    except IOError as e:
                        print(f"Error reading file {filename}: {e}")
                    except UnicodeDecodeError as e:
                        print(f"Error decoding file {filename}: {e}")

        return docs_dict
    
    def ParseDescription(self, entity):
        if "description" not in entity:
            return ""
            
        description = entity["description"]
        if "{{" in description:
            return "Note: '" + self.ReplaceJinjaVariables(description) + "'"
        else:
            return "Note: '" + description.replace("'","") + "'"

    def WriteTable(self, dbml_file, model):
        """Create a table in the dbml file. 

        Args:
            model: JSON object from catalog representing the dbt model
            docs_path: Path to docs folder containing docs.md files
        """    
        name = model["metadata"]["name"]
        columns = list(model["columns"].keys())
        start = "{"
        end = "}"

        # Find the model in the schema YAML file
        schema_model = None
        for m in self.schema['models']:
            if m['name'] == name.lower():
                schema_model = m
                break
        
        model_description = self.ParseDescription(schema_model)

        dbml_file.write(f"Table {name} {start} \n")

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

                column_description = self.ParseDescription(schema_column)
                if column_description:
                    column_docs_list.append(column_description)

                if column_docs_list:
                    column_tests_and_description = '[' + ', '.join(column_docs_list) + ']'
                
            dbml_file.write(f"{name} {dtype} {column_tests_and_description} \n")
        dbml_file.write(f"{model_description} \n{end} \n")
        

    def WriteRelationship(self, dbml_file):
        """Create a relationship in the dbml file. Loops over all columns to find relationship tests and saves them to the dbml file
        """    
        for model in self.schema["models"]:
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
                                dbml_file.write(f"Ref: {r1}.{r1_field} > {r2}.{r2_field} \n")
                                

    def GenerateDbml(self):
        """Create dbml file
        """
        
        model_names = self.catalog["nodes"]
        tables = [model["name"].upper() for model in self.schema["models"]]
        
        with open(self.dbml_path, "w") as dbml_file:
            for model_name in model_names:
                model = self.catalog["nodes"][model_name]
                if model["metadata"]["name"] in tables: 
                    self.WriteTable(dbml_file, model)
            self.WriteRelationship(dbml_file)

