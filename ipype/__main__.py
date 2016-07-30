import click
from pathlib import Path
from ipype.pipeline import IPypeApp, Pipeline
from traitlets.config import Config


@click.group(invoke_without_command=True)
@click.pass_context
def main(ctx, **args):
    if ctx.invoked_subcommand is None:
        rerun()
    else:
        pass


@main.command(context_settings=dict(ignore_unknown_options=True,))
@click.option('--pipeline', '-p', type=click.Path(exists=True))
@click.option('--output_dir', '-o', type=click.Path(exists=False))
@click.argument('cmdline_args', nargs=-1, type=click.UNPROCESSED)
def run(pipeline, output_dir, **cmdline_args):
    c = Config()
    c.Pipeline.path = pipeline
    c.Pipeline.output_dir = output_dir
    c.Pipeline.cmdline_args = cmdline_args['cmdline_args']
    app = IPypeApp(config=c)
    app.initialize()
    app.pipeline.start()
    
    #pipeline = Pipeline(config=c)
    #pipeline.initialize()
    #pipeline.start()
    

@main.command()
def rerun():
    
    print('rerunning')
    
    from ipype.config import DirPipelineConfigLoader

    output_dir = str(Path(".").absolute())
    
    pipeline_config = DirPipelineConfigLoader(output_dir).load_config()
    
    c = Config()
    c.Pipeline.path = pipeline_config['pipeline_dir']
    c.Pipeline.output_dir = output_dir
    c.Pipeline.cmdline_args = pipeline_config['cmdline_args']
    
    app = IPypeApp(config=c)
    app.initialize()
    app.pipeline.start()
        
    
if __name__ == "__main__":
    main()
    pass
    
