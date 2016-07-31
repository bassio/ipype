from pathlib import Path
from functools import reduce
import zipfile
import hashlib
from collections import namedtuple
import nbformat
from nbformat.reader import reads as reader_reads, NotJSONError
from nbformat.validator import validate, ValidationError
from ipype.exporters import HTMLExporter
from nbconvert.preprocessors import ExecutePreprocessor

ZipFileTuple = namedtuple('ZipFileTuple', ['zipfile_path','member_info'])

def open_notebook(notebook):

    with open(str(notebook)) as f:
        nb = nbformat.read(f, as_version=nbformat.current_nbformat)
    
    return nb
    

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
    
    try:
        nb = reader_reads(data)
        validate(nb)
        return True
    except ValidationError as e:
        return False
    except NotJSONError as e:
        return False
        

def get_notebook_pipeline_info(notebook_filename):
    
    nb = open_notebook(notebook_filename)
    
    return dict(nb.metadata.pipeline_info)


def md5sum(filename):
    
    md5hash = hashlib.md5()
    
    with open(str(filename), 'rb') as f:
        while True:
            chunk = f.read(1024)  
            if not chunk:
                break
            
            md5hash.update(chunk)
    
    try:
        f.close()
    except:
        pass
    
    return md5hash.hexdigest()


def calculate_notebook_node_hash(notebook_node):
    
    output_hash_dict = {}
    
    nb_dict_keys = sorted(dict(notebook_node).keys())
    
    md5 = hashlib.md5()
    
    for k in nb_dict_keys:
        
        v = notebook_node[k]
        
        if isinstance(v, str):
            values = [v]
        elif isinstance(v, (list, tuple)):
            values = v
        
        for val in values:
            p = Path(val)
            try:
                val_to_byte = md5sum(p).encode()
            except IsADirectoryError as e:
                val_to_byte = str(p).encode()
            except FileNotFoundError as e:
                val_to_byte = val.encode()
            
            md5.update(val_to_byte)
        
    return md5.hexdigest()
    
    
def return_notebook_node_modified_times(notebook_node):
    
    nb_dict_keys = sorted(dict(notebook_node).keys())
    
    mtimes = []
    
    for k in nb_dict_keys:
        v = notebook_node[k]
        
        if isinstance(v, str):
            values = [v]
        elif isinstance(v, (list, tuple)):
            values = v
            
        for val in values:
            p = Path(val)
            try:
                mtimes.append(p.stat().st_mtime)
            except FileNotFoundError as e:
                pass
        
    return mtimes
    
        
def calculate_notebook_hash(notebook_filename):
    nb_pipeline_info = get_notebook_pipeline_info(notebook_filename)
    
    inputs = nb_pipeline_info['inputs']
    outputs = nb_pipeline_info['outputs']
    
    md5 = hashlib.md5()
    
    notebook_digest = md5sum(notebook_filename)
    inputs_hash = calculate_notebook_node_hash(inputs)
    outputs_hash = calculate_notebook_node_hash(outputs)
    
    md5.update(notebook_digest.encode())
    md5.update(inputs_hash.encode())
    md5.update(outputs_hash.encode())
    
    return md5.hexdigest()

    
def is_notebook_modified_since_run(notebook_filename):
    nb = open_notebook(notebook_filename)
    
    nb_mtime = float(nb.metadata.pipeline_info.notebook_finished_timestamp)
    inputs_mtimes = return_notebook_node_modified_times(nb.metadata.pipeline_info.inputs)
    outputs_mtimes = return_notebook_node_modified_times(nb.metadata.pipeline_info.outputs)
    
    if max(inputs_mtimes + [nb_mtime]) > nb_mtime:
        return True
    
    if max(outputs_mtimes + [nb_mtime]) > nb_mtime:
        return True
    
    return False
    
    