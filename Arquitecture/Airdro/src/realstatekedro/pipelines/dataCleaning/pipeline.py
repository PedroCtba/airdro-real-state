from kedro.pipeline import Pipeline, node, pipeline
from .nodes import cleaningQuintoAndar

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([

        node(
            func=cleaningQuintoAndar,
            inputs=["rawQuintoAndar"],
            outputs="cleannedQuintoAndar",
            name="cleanQuintoAndar"
        )
    ])