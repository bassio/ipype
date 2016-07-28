from pathlib import Path
from traitlets import default
from nbconvert.exporters import HTMLExporter as BaseHTMLExporter


class HTMLExporter(BaseHTMLExporter):
    @default('default_template_path')
    def _default_template_path_default(self):
        template_dir = Path(__loader__.path).parent / 'custom'
        return str(template_dir)
 
