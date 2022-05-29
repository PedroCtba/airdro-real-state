from typing import Dict
from kedro.pipeline import Pipeline
from realstatekedro.pipelines import dataScrapping, dataCleaning


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    dataScrappingPipeline = dataScrapping.create_pipeline()
    dataCleaningPipeline = dataCleaning.create_pipeline()

    return {
    "__default__": dataScrappingPipeline + dataCleaningPipeline,
    "DataScrappingPipeline": dataScrappingPipeline,
    "DataCleaningPipeline": dataCleaningPipeline
    }