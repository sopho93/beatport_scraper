from setuptools import setup
from setuptools import find_packages

setup(
    name='beatscraper', ## This will be the name your package will be published with
    version='0.0.2', 
    description='Scraper package that scrapes the Beatport website',
    #url='https://github.com/IvanYingX/project_structure_pypi.git', # Add the URL of your github repo if published 
                                                                   # in GitHub
    author='Sophocles Sophocleous', 
    #license='MIT',
    packages=find_packages(), # This one is important to explain. See the notebook for a detailed explanation
    install_requires=['webdriver_manager', 'selenium', 'sqlalchemy', 'boto3'], # For this project we are using two external libraries
                                                     # Make sure to include all external libraries in this argument
)
