from kedro.pipeline import Pipeline, node, pipeline
from .nodes import scrapeQuintoAndar

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([

        node(
            func=scrapeQuintoAndar,
            inputs=None,
            outputs="rawQuintoAndar",
            name="scrapeQuintoAndarNode"
            )
    ])
