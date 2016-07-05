import os
import re
import ast
from setuptools import setup, find_packages
from setuptools.command.build_ext import build_ext as _build_ext

package_name = "ipype"


# version parsing from __init__ pulled from scikit-bio
# https://github.com/biocore/scikit-bio/blob/master/setup.py
# which is itself based off Flask's setup.py https://github.com/mitsuhiko/flask/blob/master/setup.py
_version_re = re.compile(r'__version__\s+=\s+(.*)')



with open('ipype/__init__.py', 'rb') as f:
    hit = _version_re.search(f.read().decode('utf-8')).group(1)
    version = str(ast.literal_eval(hit))


here = os.path.abspath(os.path.dirname(__file__))

README = open(os.path.join(here, 'README.md')).read()
CHANGES = open(os.path.join(here, 'CHANGES.md')).read()

try:
    import pypandoc
    long_description = pypandoc.convert(README + '\n\n' +  CHANGES, 'rst', format='md')
except ImportError:
    long_description= README + '\n\n' + CHANGES


setup_requires = [
    ]


install_requires = []
with open(os.path.join(here, 'requirements.txt')) as req:
    for line in req:
        install_requires.append(line)

    

setup(name=package_name,
      version=version,
      license='BSD',
      description="Run self-documenting Jupyter notebook pipelines.",
      long_description=long_description,
      classifiers=[
        "Programming Language :: Python",
        "License :: OSI Approved :: BSD License",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        ],
      cmdclass={},
      author='Ahmed Bassiouni',
      author_email='ahmedbassi@gmail.com',
      maintainer="Ahmed Bassiouni",
      maintainer_email="ahmedbassi@gmail.com",
      url='https://github.com/bassio/ipype',
      download_url = 'https://github.com/bassio/ipype/tarball/' + version,
      keywords='bioinformatics',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite='ipype.tests',
      install_requires=install_requires,
      setup_requires=setup_requires,
      entry_points="""\
      """,
      )
