ipype
=====

**ipype** is an open-source, Python 3 only, BSD-licensed library that allows you to run self-documenting Jupyter notebook pipelines.


In order to integrate well with the rest of the Jupyter ecosystem, the package uses their traitlets-based configuration and depends on other packages from the Jupyter and IPython ecosystem packages of Jupyter and IPython (mainly nbconvert).

One of the goals of the package is to be straightforward and easy to use, because the philosophy is that complex code should be self-documenting in the notebooks,
rather than in the pipeline-running-tool itself.
It puts the value in the "Literate Programming" paradigm allowed by the Jupyter notebooks.
The package does not aim to replace complex workflow/pipeline/build software, but it could integrate with them.


## Example: Command-line interface

    ipype --pipeline notebook.ipynb --output ./output_dir
    
    #OR (shorter version)
    ipype -p notebook.ipynb -o ./output_dir
    
    #Zip files containing multiple notebook files are supported!
    ipype -p pipeline_notebooks.zip -o ./output_dir
    
    #Note: R Kernel notebooks work, Jupyter is awesome!
    ipype -p R_kernel_nb_test.ipynb -o ./output_dir
    

## Example: Python API interface


    TODO when API is more stable.


## Current Workflow

The current workflow includes the following steps:

1. Create output folder and subfolders.
These currently include: 'data','exec_notebooks','html','logs','pipeline','results','tmp'.

2. Setup logging functionality, typically involving files placed into the output folder.

3. Copy/extract the pipeline notebooks into the target folder (in particular, into the *pipeline* subfolder).

4. Execute the notebooks one by one (sorted in alphabetical order).
Write the executed notebooks with filename.exec.ipynb in the *exec_notebooks* subfolder.

5. Export an html version for each of the executed notebooks into the *html* subfolder.


## Installation

Just clone the git repository.
Hopefully very shortly, the package will be uploaded to PyPi and will be pip-installable.

## Dependencies

Current ipype dependencies:

- traitlets
- nbconvert
- nbformat
- click

## License

This package is released as open-source, under a BSD License. Please see LICENSE.txt.

## Documentation

To be available soon.

## Roadmap

Some things to aim for:

- Allowing a series of files as input on the commandline to allow the pipelining of multiple pipelines *in sequence*.
For example:

        ipype --pipeline pipeline1.zip,pipeline2.ipynb,pipeline4.zip --output ./output_dir


- Allowing to point to a github folder of notebooks (or other git repo) for automatic downloading of pipelines from online repositories.
For example:

        ipype --github https://github.com/user/repository/folder_with_notebooks --output ./output_dir

- Notification (think email etc) when the processing of a long-running pipeline finishes.

- Closer integration with the rest of the Jupyter ecosystem.

- Reach a convention on how to set up initial pipeline configuration and how the pipeline signals its required inputs. Investigate how to reuse the traitlets system as much as possible for that.

- Provide documentation and testing.


## Acknowledgements

I would like to thank the great Python community who continues to publish excellent software as open-source.
