from pathlib import Path
import zipfile
from collections import namedtuple
import nbformat
from nbformat.reader import reads as reader_reads
from nbformat.validator import validate
from nbconvert.exporters import HTMLExporter
from nbconvert.preprocessors import ExecutePreprocessor

ZipFileTuple = namedtuple('ZipFileTuple', ['zipfile_path','member_info'])

def export_notebook(notebook_pth, preprocessor_instance=ExecutePreprocessor(timeout=-1), metadata_path_str=""):
    notebook_pth = Path(notebook_pth).absolute()
    
    notebook = str(notebook_pth)

    nb = None
    
    with open(notebook) as f:
        nb = nbformat.read(f, as_version=nbformat.current_nbformat)
    
    resources = {'metadata': {'path': metadata_path_str}}
    preprocessor_instance.preprocess(nb, resources)
    
    return nb, resources    
    
    
def execute_notebook(notebook_pth, notebook_out_pth, preprocessor_instance=ExecutePreprocessor(timeout=-1), metadata_path_str=""):
    notebook_pth = Path(notebook_pth).absolute()
    notebook_out_pth = Path(notebook_out_pth).absolute()
    
    notebook = str(notebook_pth)
    notebook_out = str(notebook_out_pth)
    notebook_out_folder = str(notebook_out_pth.parent)

    nb = None
    
    with open(notebook) as f:
        nb = nbformat.read(f, as_version=nbformat.current_nbformat)
    
    preprocessor_instance.preprocess(nb, {'metadata': {'path': metadata_path_str}})
    
    with open(notebook_out, 'wt') as f:
        nbformat.write(nb, f)
    
    

def notebook_to_html(notebook_pth, notebook_out_pth):
    notebook_pth = Path(notebook_pth).absolute()
    notebook_out_pth = Path(notebook_out_pth).absolute()
    
    notebook = str(notebook_pth)
    notebook_out = str(notebook_out_pth)
    
    nb = None

    html_exporter = HTMLExporter()
    #html_exporter.template_file = 'basic'
    
    with open(notebook) as f:
        nb = nbformat.read(f, as_version=4)
        
    body, resources = html_exporter.from_notebook_node(nb)
    
    with open(notebook_out, 'w') as f:
        print(body, file=f)
    

def get_notebooks_in_zip(zip_file, notebook_ext="ipynb"):
    notebooks = []
    
    zip_file_pth = Path(zip_file)
    
    with zipfile.ZipFile(zip_file, 'r') as zipped:
        if notebook_ext is not None:
            return [ZipFileTuple(zip_file_pth, zi) for zi in zipped.infolist() \
                    if zi.filename.endswith(notebook_ext)]
        else:
            return [ZipFileTuple(zip_file_pth, zi) for zi in zipped.infolist()]


def extract_notebook_from_zip(zip_file, notebook_filename, output_dir):
    
    output_path = Path(output_dir)
    if not output_path.exists():
        output_path.mkdir()
    
    with zipfile.ZipFile(zip_file, 'r') as zipped:
        zipped.extract(notebook_filename, str(output_path))
         
def is_valid_notebook(notebook_file):
    with open(notebook_file) as myfile:
        data = "".join(line for line in myfile)
    
    nb = reader_reads(data)
    
    try:
        validate(nb)
        return True
    except ValidationError as e:
        return False
        