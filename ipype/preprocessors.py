import os
from nbconvert.preprocessors import ExecutePreprocessor


class IPypeExecutePreprocessor(ExecutePreprocessor):
    timeout = -1
    
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
                    pattern = u"""\
                        An error occurred while executing the following cell:
                        ------------------
                        {cell.source}
                        ------------------
                        {out.ename}: {out.evalue}
                        """
                    msg = dedent(pattern).format(out=out, cell=cell)
                    raise CellExecutionError(msg)
        return cell, resources
    
    def shutdown(self):
        self.kc.stop_channels()
        self.km.shutdown_kernel(now=True)
        
 
