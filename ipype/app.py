import logging
from pathlib import Path
from io import StringIO

from traitlets.config import Config
from traitlets.config.manager import BaseJSONConfigManager
from traitlets.traitlets import Bool, Unicode, List, Dict, Tuple, default
from nbconvert.nbconvertapp import NbConvertApp
from nbconvert.exporters import export
from nbconvert.writers import FilesWriter



from ipype.config import Pipeline
from ipype.notebook import get_notebooks_in_zip, is_valid_notebook, \
    export_notebook, open_notebook

class IPype(NbConvertApp):
    name = Unicode('ipype')

    description = Unicode('IPype is an open-source, Python 3 only, BSD-licensed library that allows you to run self-documenting Jupyter notebook pipelines.')
    
    # The version string of this application.
    version = Unicode('')
    
    #aliases = {'pipeline': 'Pipeline.path',
    #          'output': 'Pipeline.output_dir'}
    
    @default('version')
    def get_version(self):
        from ipype import __version__
        return __version__
    
    # The usage and example string that goes at the end of the help string.
    #examples = Unicode()

    # A sequence of Configurable subclasses whose config=True attributes will
    # be exposed at the command line.
    classes = List([Pipeline])
    
    config_file = Unicode(u'', config=True, help="Load this config file")
    # config_file is reachable only with --MyApp.config_file=... or --help-all
    
    
    def initialize(self, argv=None):
        super().initialize(argv) #btw this also calls init notebooks
        
        #set pipeline path and output dir
        self._path = Path(self.config.pipeline)
        self._output = Path(self.config.output_dir)
        
        #setup logging
        self._setup_logging()
        
        #set the pipeline Configurable object
        self.Pipeline = Pipeline(config=Config(self.config), log=self.log)
        
        #init writers
        self.init_writers()
        
        
    def init_notebooks(self):
        filenames = []
        
        pipeline_path = Path(self.config.pipeline)
        output_path = Path(self.config.output_dir)

        if pipeline_path.is_dir():
            filenames = sorted(pipeline_path.glob('*.ipynb'))
        elif is_zipfile(str(pipeline_path)):
            filenames = get_notebooks_in_zip(str(pipeline_path))
        elif pipeline_path.is_file():
            if is_valid_notebook(str(pipeline_path)):
                filenames = [pipeline_path] # list with one notebook
            else:
                raise Exception("Could not validate notebook")

        _notebooks = [str(f) for f in filenames]
        
        #copy notebooks to pipeline subfolder
        copied_notebooks = []
        
        for notebook in _notebooks:
            notebook_pth = Path(notebook)
            from nbconvert.exporters import NotebookExporter
            pipeline_writer = FilesWriter(build_directory=str(output_path / 'pipeline'))
            pipeline_output, resources = export(NotebookExporter, notebook, resources={})
            pipeline_writer.write(pipeline_output, resources, notebook_name=notebook_pth.stem)
            copied_notebook_pth = output_path / 'pipeline' / notebook_pth.name
            assert copied_notebook_pth.exists()
            copied_notebooks.append(str(copied_notebook_pth.absolute()))
        
        self.notebooks = copied_notebooks
        
        #add it into the Pipeline metadata
        self.config.pipeline_notebooks = [nb for nb in self.notebooks]
        self.config.executed_notebooks = []
     
    def _output_subdir(self, subdir):
        return str(self._output / subdir)
    
    
    def init_writers(self):
        self.log.debug("Call: IPype.init_writers()") #log
        
        self.writers = Config()
        
        self.writers['pipeline_writer'] = FilesWriter(build_directory=self._output_subdir('pipeline'))
        self.writers['calib_writer'] = FilesWriter(build_directory=self._output_subdir('calib_notebooks'))
        self.writers['exec_writer'] = FilesWriter(build_directory=self._output_subdir('exec_notebooks'))
        self.writers['html_writer'] = FilesWriter(build_directory=self._output_subdir('html'))
        
 
    def _setup_logging(self):
        
        self.log.debug("Call: IPype._setup_logging()") #log
        
        self.log.setLevel(logging.INFO)
        
        logs_subdir = Path(self._output_subdir('logs'))
        logs_subdir.mkdir(exist_ok=True)
        pipeline_log_pth = logs_subdir / 'pipeline.log'

        log_file_handler = logging.FileHandler(str(pipeline_log_pth))
        log_file_handler.setLevel(logging.INFO)
        log_file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        log_file_handler.setFormatter(log_file_formatter)
        self.log.addHandler(log_file_handler)
        
        self.log.info("Application logging was set up.") #log


    def write_json_config(self):
        self.log.debug("Call: IPype.write_json_config().") #log
        #save a config.json into the output dir
        configmanager = BaseJSONConfigManager(config_dir=str(self._output))
        configmanager.set('config', self.config)
        self.log.debug("config.json file saved into the main output dir.") #log
        #save a config.json into the "pipeline subdir"
        configmanager = BaseJSONConfigManager(config_dir=str(self._output_subdir('pipeline')))
        configmanager.set('config', self.Pipeline.config)
        self.log.debug("config.json file saved into the pipeline subdir of the output dir.") #log


    def convert_notebooks(self):
        
        #write json config into output dir
        self.write_json_config()
        
        #initialize notebooks ("that have been executed") as empty list
        self.executed_notebooks = []
        
        #loop over the notebooks
        for notebook in self.notebooks:
            self.log.debug("Call: IPype.convert_single_notebook() for notebook: {}".format(notebook))
            self.convert_single_notebook(notebook)
            
    
    def convert_single_notebook(self, notebook_filename, input_buffer=None):
        
        notebook_pth = Path(notebook_filename)
        
        
        self.log.info("Initializing single notebook resources for notebook: {}".format(notebook_filename)) #log
        resources = self.init_single_notebook_resources(notebook_filename)
        
        #calib
        self.log.info("Calibrating notebook: {}".format(notebook_filename)) #log
        from ipype.exporters import CalibratedNotebookExporter
        resources.update(output_subdir=str(self._output / 'calib_notebooks'))
        calib_output, resources = export(CalibratedNotebookExporter, notebook_filename, resources=resources)
        self.writers['calib_writer'].write(calib_output, resources, notebook_name=notebook_pth.stem)
        ##############################################################
        
        #exec ##########################################################
        self.log.info("Executing notebook: {}".format(notebook_filename)) #log
        results_subdir = Path(self._output / 'results')
        results_subdir.mkdir(exist_ok=True) #create results subdir
        from ipype.exporters import ExecutedNotebookExporter
        exec_subdir = self._output / 'exec_notebooks'
        resources.update(output_subdir=str(exec_subdir))
        exec_output, resources = export(ExecutedNotebookExporter, notebook_filename, resources=resources)
        self.writers['exec_writer'].write(exec_output, resources, notebook_name=notebook_pth.with_suffix('.exec').name)
        executed_notebook_pth = exec_subdir / notebook_pth.with_suffix('.exec.ipynb').name
        self.executed_notebooks.append(str(executed_notebook_pth))
        ##############################################################
        
        #html
        self.log.info("Exporting executed notebook {} to html..".format(str(executed_notebook_pth))) #log
        from ipype.exporters import HTMLExporter
        resources.update(output_subdir=str(self._output / 'html'), notebook_name=notebook_pth.stem)
        exec_output_filelike = StringIO(exec_output)
        html_output, resources = export(HTMLExporter, exec_output_filelike, resources=resources)
        self.writers['html_writer'].write(html_output, resources, notebook_name=notebook_pth.stem)
        ##############################################################
        
        

    def init_single_notebook_resources(self, notebook_filename):
        self.log.debug("Call: IPype.init_single_notebook_resources() for notebook: {}".format(notebook_filename)) #log
        
        notebook_pth = Path(notebook_filename).absolute()
        
        return {
                'config_dir': str(self.config.pipeline),
                'unique_key': notebook_pth.name,
                'output_files_dir': str(self.Pipeline.output_dir),
                'notebook_filename': str(notebook_pth),
                'pipeline_dir': self._output_subdir('pipeline'),
                'pipeline_notebooks': self.notebooks,
                'executed_notebooks': self.executed_notebooks,
                'pipeline_info': self.Pipeline.config,
                }

    def export_single_notebook(self, notebook_filename, resources, input_buffer=None):
        """not used"""
        return output, resources

    def write_single_notebook(self, output, resources):
        """not used"""
        return write_results #return type: file

    def postprocess_single_notebook(self, write_results):
        """not used"""
        pass
 


##############################################################

main = launch_new_instance = IPype.launch_instance


if __name__ == "__main__":
    main()
    
