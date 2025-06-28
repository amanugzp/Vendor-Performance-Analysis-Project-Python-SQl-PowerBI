import os
import pandas as pd
import gc
import time
import logging
from sqlalchemy import create_engine

logging.basicConfig(
    filename= "C:\Data_Science\Vendor Performance Analysis Project (Python+SQl+PowerBI\logs/ingestion_db.log",
    level= logging.DEBUG,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Setup logging
logging.basicConfig(level=logging.INFO)

folder_path = r'C:\Data_Science\Vendor Performance Analysis Project (Python+SQl+PowerBI\data'
engine = create_engine('mysql+pymysql://root:password@localhost:3306/database_name')

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df,table_name,engine):
    """This function will ingest the dataframe into database table"""
    df.to_sql(table_name,con = engine,if_exists='replace',index= False)

    
def load_raw_data():
    """This function loads large CSVs in chunks and ingests into the DB."""
    start = time.time()
    
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            table_name = file[:-4]
            total_rows = 0
            columns = []

            logging.info(f'Ingesting {file} into DB')

            try:
                for i, chunk in enumerate(pd.read_csv(file_path, chunksize=50000)):
                    chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
                    if i == 0:
                        columns = chunk.columns.tolist()  # Capture columns only once
                    total_rows += chunk.shape[0]
                    del chunk
                    gc.collect()
                
                logging.info(f'{file} | Shape: ({total_rows}, {len(columns)})')

            except MemoryError:
                logging.error(f"MemoryError on {file} even with chunking.")
            except Exception as e:
                logging.error(f"Error while processing {file}: {e}")
    
    end = time.time()
    total_time = (end - start)/60
    logging.info('----------Ingestion Complete---------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')


if __name__ == '__main__':
    load_raw_data()
