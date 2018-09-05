import logging
import zipfile
import shutil
import io
import copy
from zipfile import is_zipfile
from datetime import datetime
from pathlib import Path

import traitlets
from traitlets.config import Configurable, Application
from traitlets.config.loader import KeyValueConfigLoader
from traitlets.config.manager import BaseJSONConfigManager

import nbformat
from nbconvert.exporters import Exporter


from ipype.preprocessors import IPypeExecutePreprocessor
from ipype.config import ZippedPipelineConfigLoader, DirPipelineConfigLoader
from ipype.notebook import export_notebook, execute_notebook, notebook_to_html, \
get_notebooks_in_zip, extract_notebook_from_zip, ZipFileTuple, is_valid_notebook



#class Pipeline(Configurable):
class Pipeline(Exporter):
    requires = traitlets.List()
    path = traitlets.Unicode().tag(config=True)
    output_dir = traitlets.Unicode().tag(config=True)
    cmdline_args = traitlets.Tuple().tag(config=True)
    notebook_pattern = traitlets.Unicode("*.ipynb")
    
    output_subdirs = traitlets.List(['data','exec_notebooks','html','logs','pipeline', 'results','tmp'])
    
    _preprocessors = traitlets.List(['ipype.preprocessors.IPypeExecutePreprocessor'])

    def initialize(self):
        self._path = Path(self.path).absolute()
        self._output = Path(self.output_dir).absolute()

        if self._path.is_dir():
            self._notebooks = sorted(self._path.glob(self.notebook_pattern))
        elif is_zipfile(str(self._path)):
            self._notebooks = get_notebooks_in_zip(str(self._path))
        elif self._path.is_file():
            if is_valid_notebook(str(self._path)):
                self._notebooks = [self._path] # list with one notebook
            else:
                raise Exception("Could not validate notebook")

        self.init_configloader()
        
        self.init_preprocessor()
        
        
    def init_configloader(self):
        if self._path.is_dir():
            self.configloader = DirPipelineConfigLoader(str(self._path))
            pipeline_config = self.configloader.load_config()
        elif is_zipfile(str(self._path)):
            self.configloader = ZippedPipelineConfigLoader(str(self._path))
            pipeline_config = self.configloader.load_config()
        else:
            pass
        
        kv_config_loader = KeyValueConfigLoader(self.cmdline_args)
        kv_config_loader.log.setLevel(logging.ERROR) #shut up the useless warnings
        cmdline_args_config = kv_config_loader.load_config()
        
        pipeline_config['Args'].merge(cmdline_args_config)
        
        self.config['Pipeline'].merge(pipeline_config)
        
        #set pipeline_dir
        self.config['Pipeline']['pipeline_dir'] = str(self._output_subdir('pipeline'))
        
        
    def init_preprocessor(self):
        preprocessor = IPypeExecutePreprocessor(timeout=-1, pipeline_config=self.config)
        preprocessor.log = self.parent.log
        self.preprocessor = preprocessor
    
    def _output_subdir(self, subdir):
        return (self._output / subdir)
    
    def _make_output_dir(self):
        self._output.mkdir(exist_ok=True)

    def _make_output_subdirs(self):
        for subdir in self.output_subdirs:
            subdir_pth = self._output / subdir
            subdir_pth.mkdir(exist_ok=True)
    
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
                try:
                    shutil.copy(str(notebook_file), str(dst_notebook_pth))
                except shutil.SameFileError: #when the input pipeline_dir = output_dir
                    pass #happy: do nothing
                
                self.extracted_notebooks.append(dst_notebook_pth)
                

        #list and set the notebooks (all extracted notebooks)
        #############
        self.notebooks = self.extracted_notebooks
        #add it into the Pipeline metadata
        self.config['Pipeline']['pipeline_notebooks'] = [str(nb) for nb in self.notebooks]
        
        
        #copy custom files
        module_path = Path(__loader__.path).absolute().parent
        custom_js = module_path / 'custom' / 'custom.js'
        custom_css = module_path / 'custom' / 'custom.css'
        shutil.copy(str(custom_js), str(self._output_subdir('html')))
        
        
    
    def init_config_json(self):
        #save a config.json into the output dir
        print(print(self.config['Pipeline']))
        configmanager = BaseJSONConfigManager(config_dir=str(self._output))
        configmanager.set('config', self.config['Pipeline'])
        #save a config.json into the "pipeline subdir"
        configmanager = BaseJSONConfigManager(config_dir=str(self._output_subdir('pipeline')))
        configmanager.set('config', self.config['Pipeline'])
    
    
    def export_single_notebook(self, notebook_filename, resources=None, input_buffer=None):
        
        notebook_filename_pth = Path(notebook_filename)
        notebook_exec_name = notebook_filename_pth.with_suffix('.exec.ipynb').name
        notebook_exec_pth = self._output_subdir('exec_notebooks') / notebook_exec_name
        
        nb = None
    
        with open(str(notebook_filename_pth)) as f:
            nb = nbformat.read(f, as_version=nbformat.current_nbformat)

        resources = {'metadata': {'path': str(self._output)}}
        
        pipeline_info_dict = dict(self.config)['Pipeline']
        
        if 'pipeline_info' not in nb['metadata']:
            nb['metadata']['pipeline_info'] = pipeline_info_dict
        else: #assume it is a dict / dict-like
            nb['metadata']['pipeline_info'].update(pipeline_info_dict)
            
        nb['metadata']['pipeline_info']['notebook_filename'] = notebook_exec_name
        nb['metadata']['pipeline_info']['notebook_name'] = notebook_exec_name.split(".exec.ipynb")[0]
        nb['metadata']['pipeline_info']['notebook_path'] = str(notebook_exec_pth)
        nb['metadata']['pipeline_info']['notebook_index'] = notebook_index = self.notebooks.index(notebook_filename_pth)
        
        if notebook_index == 0:
            nb['metadata']['pipeline_info']['previous_notebook'] = None 
            #set inputs to empty dict
            nb['metadata']['pipeline_info']['inputs'] = {}
            
        else:
            nb['metadata']['pipeline_info']['previous_notebook'] = str(self.notebooks[notebook_index - 1])
            #set inputs from previous notebook outputs
            nb['metadata']['pipeline_info']['inputs'] = self.get_last_exec_notebook_outputs()        
        
        notebook_started = datetime.now()
        nb['metadata']['pipeline_info']['notebook_started'] = notebook_started.isoformat()
        nb['metadata']['pipeline_info']['notebook_started_timestamp'] = notebook_started.timestamp()


        self.logger.info("Starting to execute {}".format(str(notebook_filename_pth)))
        
        self.preprocessor.preprocess(nb, resources)
        
        notebook_finished = datetime.now()
        nb['metadata']['pipeline_info']['notebook_finished'] = notebook_finished.isoformat()
        nb['metadata']['pipeline_info']['notebook_finished_timestamp'] = notebook_finished.timestamp()
        self.logger.info("Finished executing {} at {}".format(str(notebook_filename_pth), notebook_finished))
        
        return nb, resources
            
        
    def convert_single_notebook(self, notebook_filename, input_buffer=None):
        
        notebook_filename = Path(notebook_filename)
        notebook_exec_name = notebook_filename.with_suffix('.exec.ipynb').name
        notebook_exec_pth = self._output_subdir('exec_notebooks') / notebook_exec_name
        
        
        nb, resources = self.export_single_notebook(notebook_filename)
        
        with io.open(str(notebook_exec_pth), 'wt', encoding='utf-8') as f:
            nbformat.write(nb, f)
        
    
        self.exec_notebooks.append(notebook_exec_pth)
    
        return nb, resources
    
    
    def get_last_exec_notebook_outputs(self):
        
        notebook_filename_pth = Path(self.exec_notebooks[-1])
        
        nb = None
    
        with open(str(notebook_filename_pth)) as f:
            nb = nbformat.read(f, as_version=nbformat.current_nbformat)

        try:
            return nb['metadata']['pipeline_info']['outputs']
        except:
            return {}
    
    
    def verify_pipeline_integrity(self):
        
        vars_dict = {}
        outputs_prev = set([])
        
        for index, notebook_filename in enumerate(self.notebooks):
                    
            with open(str(notebook_filename)) as f:
                nb = nbformat.read(f, as_version=nbformat.current_nbformat)
            
            exec(nb['cells'][0]['source'], globals(), vars_dict)
            outputs_prev.update(vars_dict.get('__outputs__', []))
            
            if index > 0:
                inputs = vars_dict.get('__inputs__', [])
                for input_ in inputs:
                    if input_ not in outputs_prev:
                        print(input_)
                        raise Exception("Pipeline integrity compromised: "\
                                        "Notebook {} requires the input {}."\
                                        .format(str(notebook_filename), str(input_))
                                        )
            else:
                continue
            
                
        
    def _convert_executed_notebooks_to_html(self, executed_notebooks):
        for exec_notebook in self.exec_notebooks:
            html_notebook_name = exec_notebook.name.split('.exec.ipynb')[0] + ".html"
            html_notebook_pth = self._output_subdir('html') / html_notebook_name
            notebook_to_html(exec_notebook, html_notebook_pth)
    
    def convert_notebooks(self):
        
        self.verify_pipeline_integrity()
        
        self.exec_notebooks = []
        
        for notebook_filename in self.notebooks:
            nb, resources = self.calibrate_single_notebook(notebook_filename)
            nb, resources = self.convert_single_notebook(notebook_filename)
        
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
        
        #save a config.json of current configuration into
        #the output dir
        self.init_config_json()
        
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
        
             
