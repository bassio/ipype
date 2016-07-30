from pathlib import Path
from traitlets.config.loader import PyFileConfigLoader, JSONFileConfigLoader


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
            
