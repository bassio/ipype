from pathlib import Path
from traitlets import default
from nbconvert.exporters import HTMLExporter as BaseHTMLExporter
from nbconvert.exporters import NotebookExporter
from ipype.preprocessors import CalibratePipelineNotebookPreprocessor, ExecutePipelineNotebookPreprocessor, CustomJsCssPreprocessor

class HTMLExporter(BaseHTMLExporter):
    preprocessors = [CustomJsCssPreprocessor]
    
    @default('default_template_path')
    def _default_template_path_default(self):
        template_dir = Path(__loader__.path).parent / 'custom'
        return str(template_dir)

    
 
class CalibratedNotebookExporter(NotebookExporter):
    preprocessors = [CalibratePipelineNotebookPreprocessor]

class ExecutedNotebookExporter(NotebookExporter):
    preprocessors = [CalibratePipelineNotebookPreprocessor, ExecutePipelineNotebookPreprocessor]
