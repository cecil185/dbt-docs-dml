from setuptools import setup, find_packages

setup(
    name='dbterd',
    version='0.1.1',    
    description='A simple package that can generate dbml file and erd diagrams for dbt',
    url='https://github.com/cecil185/dbt-docs-to-dbml',
    author='Cecil Ash, Oliver Rise Thomsen, Anders Boje Hertz',
    author_email='cecil.ash.4@gmail.com',
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['pyyaml', 'Click'],
    
    entry_points='''
        [console_scripts]
        dbterd=dbterd.terminal:cli
    ''',
)
