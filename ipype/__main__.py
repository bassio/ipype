import click
from pathlib import Path
from ipype.pipeline import IPypeApp, Pipeline
from traitlets.config import Config

@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option('--pipeline', '-p', type=click.Path(exists=True))
@click.option('--output_dir', '-o', type=click.Path(exists=False))
@click.argument('extra_args', nargs=-1, type=click.UNPROCESSED)
def main(pipeline, output_dir, **extra_args):
    c = Config()
    c.Pipeline.path = pipeline
    c.Pipeline.output_dir = output_dir
    c.Pipeline.cmdline_args = extra_args['extra_args']
    app = IPypeApp(config=c)
    app.initialize()
    app.pipeline.start()
    
    #pipeline = Pipeline(config=c)
    #pipeline.initialize()
    #pipeline.start()
    

if __name__ == "__main__":
    main()
    
