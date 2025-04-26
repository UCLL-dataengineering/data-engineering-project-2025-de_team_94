import os
from azure.storage.blob import BlobServiceClient
import pandas as pd


class Reader:
    def __init__(self, file_path, processed_files_tracker=None):
        self.file_path = file_path
        self.processed_files_tracker = processed_files_tracker
        self.files = [f for f in os.listdir(file_path) if f.endswith('.csv')]
        self.processed_files = set()

        if self.processed_files_tracker and os.path.exists(self.processed_files_tracker):
            with open(self.processed_files_tracker, 'r') as f:
                self.processed_files = set(f.read().splitlines())

    def list_unprocessed_files(self):
        return [f for f in self.files if f not in self.processed_files]

    def read_first_unprocessed(self):
        unprocessed = self.list_unprocessed_files()
        if not unprocessed:
            raise FileNotFoundError("No unprocessed CSV files found.")

        file = unprocessed[0]
        df = pd.read_csv(os.path.join(self.file_path, file))
        return df, file

    def get_length(self):
        return len(self.files)
    

class Writer():
    def __init__(self, dataFrame, fileName, folder_path):
        self.df = dataFrame
        self.fn = fileName
        self.folder_path = folder_path
        self.writeAzureBlobAndCsv()

    # Writes a dataframe to a CSV file
    def writeCsv(self):
        if not isinstance(self.df, pd.DataFrame):
            raise ValueError("Dataframe must be a pandas DataFrame")
        
        os.makedirs(self.folder_path, exist_ok=True)
        
        file_path = os.path.join(self.folder_path, self.fn)
        self.df.to_csv(file_path, index=False)
        print("File saved successfully to CSV")
    
    def writeAzureBlobAndCsv(self):
        self.writeCsv()

        conn_str = "DefaultEndpointsProtocol=https;AccountName=batchprocessing94;AccountKey=aH9cA5Xwbv+fELTDQPG3BZaM1AvUAK7LQTX5A6PDYMHTw6EqcXlSvzlw5Aqfs7i3XdJSzgQQw3OZ+AStf56OCw==;EndpointSuffix=core.windows.net"
        container_name = "realtimeprocessingcontainer"
        blob_name = self.fn

        file_path = os.path.join(self.folder_path, self.fn)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print("File uploaded to Azure Blob Storage successfully")      
    