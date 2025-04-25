import os
from azure.storage.blob import BlobServiceClient
import pandas as pd


class Reader():
    def __init__(self, file_path):
        self.__file_path = file_path
        self.__files = [ f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path,f)) ]
        self.dataFrames = []

    # Returns a list with all the file names in a specific folder
    def listFile(self):
        return self.__files

    # Returns a list of all the dataframes in the folder from csv files
    def DfList(self):
        if not self.dataFrames:
            for file in self.__files:
                if file.endswith(".csv"):
                    self.dataFrames.append(pd.read_csv(os.path.join(self.__file_path, file)))
                else:
                    raise ValueError("File must be a CSV.")
        return self.dataFrames

    # Returns a specific dataframe by index 
    def getDfByIndex(self, index):
        self.DfList()
        index -= 1
        if index < 0 or index >= len(self.dataFrames):
            raise ValueError("Index out of range.")
        return self.dataFrames[index]
    
    # Returns the amount of files in the folder
    def getLength(self):
        return len(self.__files)
    

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
    