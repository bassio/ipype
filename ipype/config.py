from pathlib import Path
import traitlets
from traitlets.config import Config, Configurable
from traitlets.config.loader import PyFileConfigLoader, JSONFileConfigLoader, KeyValueConfigLoader
from traitlets.config.manager import BaseJSONConfigManager



class ZippedPipelineConfigLoader(PyFileConfigLoader):
    def _read_file_as_dict(self):
        
        def get_config():
            return self.config
        
        zip_file = self.full_filename
        zip_file_pth = Path(zip_file)
        
        with zipfile.ZipFile(zip_file, 'r') as zipped:
            #try python config first
            try:
                zipinfo = zipped.getinfo("config.py")
                config_type = 'python'
            except KeyError:
                try:
                    zipinfo = zipped.getinfo("config.json")
                    config_type = 'json'
                except KeyError:
                    return #return silently
                        
            if config_type == 'python':
                with zipped.open(zipinfo, 'r') as f:
                    namespace = dict(
                    c=self.config,
                    load_subconfig=self.load_subconfig,
                    get_config=get_config,
                    __file__=self.full_filename,
                    )
                    exec(compile(f.read(), zip_file_pth.name, 'exec'), namespace)
            else:
                with zipped.open(zipinfo, 'r') as f:
                    return json.load(f)



class DirPipelineConfigLoader(PyFileConfigLoader):
    def __new__(self, dir_name_pth):
        dir_name_pth = Path(dir_name_pth)
        
        if (dir_name_pth / 'config.py').exists():
            return PyFileConfigLoader(str(dir_name_pth / 'config.py'))
        elif (dir_name_pth / 'config.json').exists():
            return JSONFileConfigLoader(str(dir_name_pth / 'config.json'))
        else:
            return PyFileConfigLoader(str(dir_name_pth / 'config.py'))
            


class Pipeline(Configurable):
    path = traitlets.Unicode().tag(config=True)
    output_dir = traitlets.Unicode().tag(config=True)
    pipeline_notebooks  = traitlets.List(default_value=[])
    executed_notebooks = traitlets.List(default_value=[])
    
    
    aliases = {'pipeline': 'path',
               'output': 'output_dir'}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._path = Path(self.config.pipeline)
        self._output = Path(self.config.output_dir)
        #set pipeline_dir
        self.config.pipeline_dir = str(self._output / 'pipeline')
        self.init_configloader()
        
    def init_configloader(self):
        
        self.log.debug("Call: Pipeline.init_configloader()") #log
        
        pipeline_config = Config()
        if self._path.is_dir():
            self.configloader = DirPipelineConfigLoader(str(self._path))
            pipeline_config = self.configloader.load_config()
        elif is_zipfile(str(self._path)):
            self.configloader = ZippedPipelineConfigLoader(str(self._path))
            pipeline_config = self.configloader.load_config()
        else:
            pass
        
        for k in dict(pipeline_config)['Args']:
            pipeline_config['Args'][k] = self.config[k]
            del self.config[k]
            
        self.config.merge(pipeline_config)
        
        
    def verify_integrity(self):
        
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
            
    
