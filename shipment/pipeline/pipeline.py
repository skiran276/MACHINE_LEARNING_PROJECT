from collections import namedtuple
from datetime import datetime
import uuid
from shipment.config.configuration import Configuartion
from shipment.logger import logging
from shipment.exception import ShipmentException
from threading import Thread
from typing import List

from multiprocessing import Process
from shipment.entity.artifact_entity import DataIngestionArtifact
from shipment.entity.config_entity import DataIngestionConfig
from shipment.component.data_ingestion import DataIngestion
import os, sys


config = Configuartion()


class Pipeline():
    

    def __init__(self, config: Configuartion = config) -> None:
        try:
           self.config = config
        except Exception as e:
            raise ShipmentException(e, sys) from e

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise ShipmentException(e, sys) from e


    def run_pipeline(self):
        try:

            data_ingestion_artifact=self.start_data_ingestion()

        except Exception as e:
            raise ShipmentException(e,sys) from e