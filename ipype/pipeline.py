import logging
from pathlib import Path
from zipfile import is_zipfile
import shutil
import io
import copy

import traitlets
from traitlets.config import Configurable, Application
import nbformat
from nbconvert.exporters import Exporter

from ipype.notebook import export_notebook, execute_notebook, notebook_to_html, get_notebooks_in_zip, extract_notebook_from_zip, ZipFileTuple
from ipype.preprocessors import IPypeExecutePreprocessor

#class Pipeline(Configurable):
class Pipeline(Exporter):
    requires = traitlets.List()
    path = traitlets.Unicode().tag(config=True)
    output_dir = traitlets.Unicode().tag(config=True)
    notebook_pattern = traitlets.Unicode("*.ipynb")
    
    output_subdirs = traitlets.List(['data','exec_notebooks','html','logs','pipeline', 'results','tmp'])
    
    _preprocessors = traitlets.List(['ipype.preprocessors.IPypeExecutePreprocessor'])

    def initialize(self):
        self._path = Path(self.path).absolute()
        self._output = Path(self.output_dir).absolute()

        if self._path.is_dir():
            self._notebooks = self._path.glob(self.notebook_pattern)
        elif is_zipfile(str(self._path)):
            self._notebooks = get_notebooks_in_zip(str(self._path))
        
        self.init_preprocessor()
        
        
    def init_preprocessor(self):
        preprocessor = IPypeExecutePreprocessor(timeout=-1)
        preprocessor.log = self.parent.log
        self.preprocessor = preprocessor
    
            
    def _output_subdir(self, subdir):
        return (self._output / subdir)
    
    def _make_output_dir(self):
        if not self._output.exists():
            self._output.mkdir()

    def _make_output_subdirs(self):
        for subdir in self.output_subdirs:
            subdir_pth = self._output / subdir
            subdir_pth.mkdir()
    
    def _setup_logging(self):
        try:
            self.logger = self.log = self.parent.log
        except:
            print("error setting up logging")
            self.logger = self.log = logging.getLogger(__name__)
        
        self.logger.setLevel(logging.INFO)

        log_file_handler = logging.FileHandler(str(self._output / 'pipeline.log'))
        log_file_handler.setLevel(logging.INFO)
        self.logger.addHandler(log_file_handler)

        timestamp_log_handler = logging.FileHandler(str(self._output_subdir('logs') / 'timestamps.log'))
        timestamp_formatter = logging.Formatter('%(asctime)s - %(message)s')
        timestamp_log_handler.setFormatter(timestamp_formatter)
        timestamp_log_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(timestamp_log_handler)
        
    
    def init_notebooks(self):
        #copy "unexecuted" notebooks (to pipeline subdir)
        self.extracted_notebooks = []
        
        for notebook_file in self._notebooks:
            if isinstance(notebook_file, ZipFileTuple):
                zipfiletuple = notebook_file
                extract_notebook_from_zip(str(zipfiletuple.zipfile_path), zipfiletuple.member_info.filename, self._output_subdir('pipeline'))
                dst_notebook_pth = self._output_subdir('pipeline') / zipfiletuple.member_info.filename
                self.extracted_notebooks.append(dst_notebook_pth)

            elif isinstance(notebook_file, Path):
                dst_notebook_pth = self._output_subdir('pipeline') / notebook_file.name
                shutil.copy(str(notebook_file), str(dst_notebook_pth))
                self.extracted_notebooks.append(dst_notebook_pth)
                

        #list and set the notebooks (all extracted notebooks)
        #############
        #self.notebooks = self._output_subdir('pipeline').glob(self.notebook_pattern)
        self.notebooks = self.extracted_notebooks
        #TODO:decide which is the better line to set the notebooks
        

    
    def export_single_notebook(self, notebook_filename, resources=None, input_buffer=None):
        notebook_filename = Path(notebook_filename)
        notebook_exec_name = notebook_filename.with_suffix('.exec.ipynb').name
        notebook_exec_pth = self._output_subdir('exec_notebooks') / notebook_exec_name
        nb, resources = export_notebook(notebook_filename, self.preprocessor, metadata_path_str=str(self._output))
        return nb, resources
        
    def convert_single_notebook(self, notebook_filename, input_buffer=None):
        
        notebook_filename = Path(notebook_filename)
        notebook_exec_name = notebook_filename.with_suffix('.exec.ipynb').name
        notebook_exec_pth = self._output_subdir('exec_notebooks') / notebook_exec_name
        
        self.logger.info("Starting to execute {}".format(str(notebook_filename)))
        nb, resources = self.export_single_notebook(notebook_filename)
        self.logger.info("Finished executing {}".format(str(notebook_filename)))
    

        with io.open(str(notebook_exec_pth), 'wt', encoding='utf-8') as f:
            nbformat.write(nb, f)
        
        self.exec_notebooks.append(notebook_exec_pth)
    
    
    def _convert_executed_notebooks_to_html(self, executed_notebooks):
        for exec_notebook in self.exec_notebooks:
            html_notebook_name = exec_notebook.name.split('.exec.ipynb')[0] + ".html"
            html_notebook_pth = self._output_subdir('html') / html_notebook_name
            notebook_to_html(exec_notebook, html_notebook_pth)
    
    def convert_notebooks(self):
        self.exec_notebooks = []
        
        for notebook_filename in self.notebooks:
            self.convert_single_notebook(notebook_filename)
        
        #export notebooks (to html)
        self._convert_executed_notebooks_to_html(self.exec_notebooks)
        
    def run(self):
        output_path = self._output
   
        #make sure output directory exists
        self._make_output_dir()
        
        #make subdirs
        self._make_output_subdirs()

        #setup logging
        self._setup_logging()        
        
        #copy "unexecuted" notebooks (to pipeline subdir)
        self.init_notebooks()
        
        #execute notebooks
        self.convert_notebooks()
        
    
    def start(self):
        """Run start after initialization process has completed"""
        self.run()
    
    
    
class IPypeApp(Application):
    name = 'ipype'
    description = "IPype Application"
    
    classes = traitlets.List([Pipeline])
    
    aliases = {'pipeline': 'Pipeline.path',
               'output': 'Pipeline.output_dir'}
    
    flags = {}
    
    def init_pipeline(self):
         self.pipeline = Pipeline(config=self.config, parent=self)
         self.pipeline.initialize()
         
    def initialize(self):
        #if self.config_file: self.load_config_file(self.config_file)
        self.init_pipeline()
        
            