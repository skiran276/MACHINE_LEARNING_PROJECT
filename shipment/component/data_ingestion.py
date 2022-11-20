from shipment.entity.config_entity import DataIngestionConfig
import sys,os
from shipment.exception import ShipmentException
from shipment.logger import logging
from shipment.entity.artifact_entity import DataIngestionArtifact
import tarfile
import zipfile as z
import numpy as np
from six.moves import urllib
import pandas as pd
from sklearn.model_selection import StratifiedShuffleSplit
from pathlib import Path
from urllib.parse import urlparse, unquote

class DataIngestion:

    def __init__(self,data_ingestion_config:DataIngestionConfig ):
        try:
            logging.info(f"{'>>'*20}Data Ingestion log started.{'<<'*20} ")
            self.data_ingestion_config = data_ingestion_config

        except Exception as e:
            raise ShipmentException(e,sys)
    

    def download_shipment_data(self,) -> str:
        try:
            #extraction remote url to download dataset
            download_url = self.data_ingestion_config.dataset_download_url

            #folder location to download file
            zip_download_dir = self.data_ingestion_config.zip_download_dir
            
            os.makedirs(zip_download_dir,exist_ok=True)

            shipment_file_name = unquote(urlparse(download_url).path.split("/")[-4])

            zip_file_path = os.path.join(zip_download_dir, shipment_file_name)

            logging.info(f"Downloading file from :[{download_url}] into :[{zip_file_path}]")
            urllib.request.urlretrieve(download_url, zip_file_path)
            logging.info(f"File :[{zip_file_path}] has been downloaded successfully.")
            return zip_file_path

        except Exception as e:
            raise ShipmentException(e,sys) from e

    def extract_zip_file(self,zip_file_path:str):
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            if os.path.exists(raw_data_dir):
                os.remove(raw_data_dir)

            os.makedirs(raw_data_dir,exist_ok=True)

            logging.info(f"Extracting zip file: [{zip_file_path}] into dir: [{raw_data_dir}]")
            with tarfile.open(zip_file_path) as shipment_zip_file_obj:
                
                import os
                
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(shipment_zip_file_obj, path=raw_data_dir)
            logging.info(f"Extraction completed")

        except Exception as e:
            raise ShipmentException(e,sys) from e
    
    def split_data_as_train_test(self) -> DataIngestionArtifact:
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            file_name = os.listdir(raw_data_dir)[0]

            shipment_file_path = os.path.join(raw_data_dir,file_name)


            logging.info(f"Reading csv file: [{shipment_file_path}]")
            shipment_data_frame = pd.read_csv(shipment_file_path)

            
            shipment_data_frame['Price'] = pd.cut(
                shipment_data_frame["Freight Cost (USD)"],
                bins=[0.0, 1.5, 3.0, 4.5, 6.0, np.inf],
                labels=[1,2,3,4,5]
            )
            

            logging.info(f"Splitting data into train and test")
            strat_train_set = None
            strat_test_set = None

            split = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)

            for train_index,test_index in split.split(shipment_data_frame, shipment_data_frame["Price"]):
                strat_train_set = shipment_data_frame.loc[train_index].drop(["Price"],axis=1)
                strat_test_set = shipment_data_frame.loc[test_index].drop(["Price"],axis=1)

            train_file_path = os.path.join(self.data_ingestion_config.ingested_train_dir,
                                            file_name)

            test_file_path = os.path.join(self.data_ingestion_config.ingested_test_dir,
                                        file_name)
            
            if strat_train_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_train_dir,exist_ok=True)
                logging.info(f"Exporting training datset to file: [{train_file_path}]")
                strat_train_set.to_csv(train_file_path,index=False)

            if strat_test_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_test_dir, exist_ok= True)
                logging.info(f"Exporting test dataset to file: [{test_file_path}]")
                strat_test_set.to_csv(test_file_path,index=False)
            

            data_ingestion_artifact = DataIngestionArtifact(train_file_path=train_file_path,
                                test_file_path=test_file_path,
                                is_ingested=True,
                                message=f"Data ingestion completed successfully."
                                )
            logging.info(f"Data Ingestion artifact:[{data_ingestion_artifact}]")
            return data_ingestion_artifact

        except Exception as e:
            raise ShipmentException(e,sys) from e

    def initiate_data_ingestion(self)-> DataIngestionArtifact:
        try:
            zip_file_path =  self.download_shipment_data()
            self.extract_zip_file(zip_file_path=zip_file_path)
            return self.split_data_as_train_test()
        except Exception as e:
            raise ShipmentException(e,sys) from e
    


    def __del__(self):
        logging.info(f"{'>>'*20}Data Ingestion log completed.{'<<'*20} \n\n")
