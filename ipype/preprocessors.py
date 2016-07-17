import os
import pprint
import json
from traitlets.config import Config
from nbconvert.preprocessors.execute import ExecutePreprocessor, CellExecutionError
from nbformat.notebooknode import NotebookNode


cell_exec_err_msg = \
"""
An error occurred while executing the following cell:
------------------
{cell.source}
------------------
{out.ename}: {out.evalue}
"""


cell_pipeline = \
"""
from traitlets.config import Config

pipeline_info = Config(\
{}
)
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
                            'source': cell_pipeline.format(pipeline_info_repr),
                            'metadata': {'collapsed':False},
                            }))
        
        env_code_str = 'env={}'.format(repr(env))
        env_pipeline_code_str = 'pipeline = {}'.format(repr(env["Pipeline"]))
        self.kc.execute(env_code_str, silent=True)
        self.kc.execute(env_pipeline_code_str, silent=True)
        
        try:
            nb, resources = super(ExecutePreprocessor, self).preprocess(nb, resources)
        finally:
            pass
            self.kc.stop_channels()
            self.km.shutdown_kernel(now=True)

        return nb, resources
    

    def preprocess_cell(self, cell, resources, cell_index):
        self.log.debug("Executing cell with index number {}".format(str(cell_index)))
        if cell.cell_type != 'code':
            return cell, resources

        outputs = self.run_cell(cell)
        cell.outputs = outputs

        if not self.allow_errors:
            for out in outputs:
                if out.output_type == 'error':
                    msg = cell_exec_err_msg.format(out=out, cell=cell)
                    raise CellExecutionError(msg)
        return cell, resources
    
    def shutdown(self):
        self.kc.stop_channels()
        self.km.shutdown_kernel(now=True)
        
 
