from src.config.configuration import ConfigurationManager
from src.components.data_ingestion import DataIngestion
from src import logger
import asyncio


STAGE_NAME="Data Ingestion Stage"

class DataIngestionTrainingPipeline:
    def __init__(self):
        pass

    async def inititate_data_ingestion(self):
        config = ConfigurationManager()
        data_ingestion_config = config.get_data_ingestion_config()
        data_ingestion = DataIngestion(config=data_ingestion_config)
        output_path = await data_ingestion.initiate_data_ingestion()
        return output_path

if __name__ == '__main__':

    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        obj = DataIngestionTrainingPipeline()
        # Use asyncio.run to call the async function
        asyncio.run(obj.inititate_data_ingestion())
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e