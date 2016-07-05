import click
from pathlib import Path
from ipype.pipeline import IPypeApp, Pipeline
from traitlets.config import Config

@click.command()
@click.option('--pipeline', '-p', type=click.Path(exists=True))
@click.option('--output_dir', '-o', type=click.Path(exists=False))
def main(pipeline, output_dir):
    c = Config()
    c.Pipeline.path = pipeline
    c.Pipeline.output_dir = output_dir
    app = IPypeApp(config=c)
    app.initialize()
    app.pipeline.start()
    
    #pipeline = Pipeline(config=c)
    #pipeline.initialize()
    #pipeline.start()
    

if __name__ == "__main__":
    main()
    