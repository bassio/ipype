import os
import pprint
import json
import shutil
from datetime import datetime
from pathlib import Path
from queue import Empty

from traitlets.config import Config
from nbconvert.preprocessors import Preprocessor
from nbconvert.preprocessors.execute import ExecutePreprocessor, CellExecutionError
from nbformat.notebooknode import NotebookNode

from .notebook import get_notebook_pipeline_outputs


CELLL_EXEC_ERR_MSG = \
"""
An error occurred while executing the following cell:
------------------
{cell.source}
------------------
{out.ename}: {out.evalue}
"""


CELL_PIPELINE = \
"""
from traitlets.config import Config

pipeline_info = Config(
{}
)

pipeline_info['outputs'] = pipeline_info['inputs']
"""

CELL_PIPELINE_INFO_OUTPUT = \
"""
print(repr(dict(pipeline_info)))
"""


class IPypeExecutePreprocessor(ExecutePreprocessor):
    timeout = -1
    pipeline_config = Config()
    expose_env_variables = False
    
    def preprocess(self, nb, resources):
        
        path = resources.get('metadata', {}).get('path', '')
        if path == '':
            path = None

        from jupyter_client.manager import start_new_kernel
        kernel_name = nb.metadata.get('kernelspec', {}).get('name', 'python')
        if self.kernel_name:
            kernel_name = self.kernel_name
        self.log.debug("Executing notebook with kernel: %s" % kernel_name)
        
        self.km, self.kc = start_new_kernel(
            kernel_name=kernel_name,
            extra_arguments=self.extra_arguments,
            stderr=open(os.devnull, 'w'),
            cwd=path)
        
        self.kc.allow_stdin = False
        
        env = {}
        
        if self.expose_env_variables:
            env = os.environ.copy()
            
        env.update(self.pipeline_config)
        
        pipeline_info_repr = pprint.pformat(dict(nb['metadata']['pipeline_info']), width=1, compact=True)
        
        
        nb.cells.insert(0, NotebookNode({'cell_type': 'code',
                            'source': CELL_PIPELINE.format(pipeline_info_repr),
                            'metadata': {'collapsed':False},
                            }))
        
        env_code_str = 'env={}'.format(repr(env))
        env_pipeline_code_str = 'pipeline = {}'.format(repr(env["Pipeline"]))
        self.kc.execute(env_code_str, silent=True)
        self.kc.execute(env_pipeline_code_str, silent=True)
        
        try:
            nb, resources = super(ExecutePreprocessor, self).preprocess(nb, resources)
        finally:
            self.preprocess_pipeline_outputs(nb, resources)
        
            self.kc.stop_channels()
            self.km.shutdown_kernel(now=True)
        
        
        return nb, resources
    
    def preprocess_pipeline_outputs(self, nb, resources):
        
        self.kc.execute(CELL_PIPELINE_INFO_OUTPUT)
        
        msg_dict = {}
        
        while True:
            try:
                reply = self.kc.get_iopub_msg(timeout=1)
                if reply['msg_type'] == 'stream':
                    msg_dict = reply
                    break
                
            except Empty:
                break

        try:
            pipeline_info = eval(msg_dict['content']['text'])
        except:
            pipeline_info = msg_dict
            
        
        outputs = pipeline_info.get('outputs', {})
        
        nb['metadata']['pipeline_info']['outputs'] = outputs
        #TODO
        #shouldn't it be nb['pipeline_info'] = pipeline_info
        

    def preprocess_cell(self, cell, resources, cell_index):
        """
        Executes a single code cell. See base.py for details.

        To execute all cells see :meth:`preprocess`.
        """
        if cell.cell_type != 'code':
            return cell, resources

        reply, outputs = self.run_cell(cell, cell_index)
        cell.outputs = outputs

        if not self.allow_errors:
            for out in outputs:
                if out.output_type == 'error':
                    raise CellExecutionError.from_cell_and_msg(cell, out)
            if (reply is not None) and reply['content']['status'] == 'error':
                raise CellExecutionError.from_cell_and_msg(cell, reply['content'])
        return cell, resources


    def shutdown(self):
        self.kc.stop_channels()
        self.km.shutdown_kernel(now=True)
        



class CalibratePipelineNotebookPreprocessor(Preprocessor):
    def preprocess(self, nb, resources): 
        notebook_name = resources['metadata']['name'] #resources
        notebook_filename_pth = Path(resources['notebook_filename']) #resources
        notebook_exec_name = notebook_filename_pth.with_suffix('.exec.ipynb').name

        output_subdir = Path(resources['output_subdir']) #resources
        notebook_exec_pth = output_subdir / notebook_exec_name
        
        pipeline_info_dict = resources['pipeline_info'] #resources
        pipeline_notebooks = resources['pipeline_notebooks'] #resources
        
        if 'pipeline_info' not in nb['metadata']:
            nb['metadata']['pipeline_info'] = pipeline_info_dict
        else: #assume it is a dict / dict-like
            nb['metadata']['pipeline_info'].update(pipeline_info_dict)
        
        nb['metadata']['pipeline_info']['notebook_filename'] = notebook_exec_name
        nb['metadata']['pipeline_info']['notebook_name'] = notebook_exec_name.split(".exec.ipynb")[0]
        nb['metadata']['pipeline_info']['notebook_path'] = str(notebook_exec_pth)
        nb['metadata']['pipeline_info']['notebook_index'] = notebook_index = pipeline_notebooks.index(str(notebook_filename_pth))
        
        if notebook_index == 0:
            previous_notebook = None
            nb['metadata']['pipeline_info']['previous_notebook'] = previous_notebook 
            #set inputs to empty dict
            nb['metadata']['pipeline_info']['inputs'] = {}
            
        else:
            previous_notebook = str(pipeline_notebooks[notebook_index - 1])
            nb['metadata']['pipeline_info']['previous_notebook'] = previous_notebook 
            
            nb['metadata']['pipeline_info']['executed_notebooks'] = resources['executed_notebooks']
        
            #set inputs from previous executed notebook output
            def get_last_exec_notebook_outputs():
                last_exec_notebook = nb['metadata']['pipeline_info']['executed_notebooks'][-1] 
                return get_notebook_pipeline_outputs(last_exec_notebook)
                
            nb['metadata']['pipeline_info']['inputs'] = get_last_exec_notebook_outputs()
        
        notebook_started = datetime.now()
        nb['metadata']['pipeline_info']['notebook_started'] = notebook_started.isoformat()
        nb['metadata']['pipeline_info']['notebook_started_timestamp'] = notebook_started.timestamp()
        
        
        #nsert pipeline info into first cell
        pipeline_info_repr = pprint.pformat(dict(nb['metadata']['pipeline_info']), width=1, compact=True)
        
        nb.cells.insert(0, NotebookNode({'cell_type': 'code',
                            'source': CELL_PIPELINE.format(pipeline_info_repr),
                            'metadata': {'collapsed':False},
                            'execution_count': None,
                            'outputs': []
                            }))        
        
        return nb, resources


class ExecutePipelineNotebookPreprocessor(ExecutePreprocessor):
    timeout = -1
    pipeline_config = Config()
    expose_env_variables = False
    
    def preprocess(self, nb, resources):
        
        path = resources.get('metadata', {}).get('path', '')
      
        if path == '':
            path = None

        from jupyter_client.manager import start_new_kernel
        kernel_name = nb.metadata.get('kernelspec', {}).get('name', 'python')
        if self.kernel_name:
            kernel_name = self.kernel_name
        self.log.debug("Executing notebook with kernel: %s" % kernel_name)
        
        self.km, self.kc = start_new_kernel(
            kernel_name=kernel_name,
            extra_arguments=self.extra_arguments,
            stderr=open(os.devnull, 'w'),
            cwd=path)
        
        self.kc.allow_stdin = False
        
        env = {}
        
        if self.expose_env_variables:
            env = os.environ.copy()
            
        env.update(self.pipeline_config)
        
        env_code_str = 'env={}'.format(repr(env))
        env_pipeline_code_str = 'pipeline = {}'.format(repr(self.pipeline_config))
        self.kc.execute(env_code_str, silent=True)
        self.kc.execute(env_pipeline_code_str, silent=True)
        
        try:
            nb, resources = super(ExecutePreprocessor, self).preprocess(nb, resources)
        finally:
            self.preprocess_pipeline_outputs(nb, resources)
                        
            self.kc.stop_channels()
            self.km.shutdown_kernel(now=True)
        
        
        return nb, resources
    
    def preprocess_pipeline_outputs(self, nb, resources):
        
        self.kc.execute(CELL_PIPELINE_INFO_OUTPUT)
        
        msg_dict = {}
        
        while True:
            try:
                reply = self.kc.get_iopub_msg(timeout=1)
                if reply['msg_type'] == 'stream':
                    msg_dict = reply
                    break
                
            except Empty:
                break

        try:
            pipeline_info = eval(msg_dict['content']['text'])
        except:
            pipeline_info = msg_dict
            
        
        outputs = pipeline_info.get('outputs', {})
        
        nb['metadata']['pipeline_info']['outputs'] = outputs
        
        return nb, resources
        

    def preprocess_cell(self, cell, resources, cell_index):
        """
        Executes a single code cell. See base.py for details.

        To execute all cells see :meth:`preprocess`.
        """
        if cell.cell_type != 'code':
            return cell, resources

        reply, outputs = self.run_cell(cell, cell_index)
        cell.outputs = outputs

        if not self.allow_errors:
            for out in outputs:
                if out.output_type == 'error':
                    raise CellExecutionError.from_cell_and_msg(cell, out)
            if (reply is not None) and reply['content']['status'] == 'error':
                raise CellExecutionError.from_cell_and_msg(cell, reply['content'])
        return cell, resources


    def shutdown(self):
        self.kc.stop_channels()
        self.km.shutdown_kernel(now=True)
        


class CustomJsCssPreprocessor(Preprocessor):
    def preprocess(self, nb, resources): 
        output_subdir = Path(resources['output_subdir']) #resources
        
        #copy custom files
        module_path = Path(__loader__.path).absolute().parent
        custom_js = module_path / 'custom' / 'custom.js'
        custom_css = module_path / 'custom' / 'custom.css'
        shutil.copy(str(custom_js), str(output_subdir))
                
        return nb, resources
    
    
