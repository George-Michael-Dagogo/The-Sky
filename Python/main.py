import os
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.dialects import registry
from sqlalchemy.orm import sessionmaker
from snowflake.sqlalchemy import URL

registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

path = "../The-Sky/Python/csv_files"

def extract_data_sources():
    if not os.path.exists(path):
        os.makedirs(path)


    for i in os.listdir(path):
        os.remove(path +'/' + i)
    #empties the directory

        
    os.system('wget -P ../The-Sky/Python/csv_files -i ../The-Sky/Python/airport.txt')


def move_to_snowflake():
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/'.format(
            user='GEORGE',
            password='George9042',
            account='QTRNDAX.MY10065',
            warehouse='COMPUTE_WH',

        )
    )
    try:
        connection = engine.connect()
        connection.execute('USE ROLE ACCOUNTADMIN')
        connection.execute('USE DATABASE STAGING')
        connection.execute('USE SCHEMA AIR_STAGING')

        airports = pd.read_csv('/workspace/Airport_Pipeline/Airport_data/airports.csv')
        airports.to_sql('airports', con=connection, if_exists='replace',index = False,chunksize=16000)   

        airport_frequencies = pd.read_csv('/workspace/Airport_Pipeline/Airport_data/airport-frequencies.csv')
        airport_frequencies.to_sql('airport_frequencies', con=connection, if_exists='replace',index = False,chunksize=16000) 

        countries = pd.read_csv('/workspace/Airport_Pipeline/Airport_data/countries.csv')
        countries.to_sql('countries', con=connection, if_exists='replace',index = False,chunksize=16000)

        airport_comments = pd.read_csv('/workspace/Airport_Pipeline/Airport_data/airport-comments.csv')
        airport_comments.to_sql('airport_comments', con=connection, if_exists='replace',index = False,chunksize=16000)

        navaids = pd.read_csv('/workspace/Airport_Pipeline/Airport_data/navaids.csv')
        navaids.to_sql('navaids', con=connection, if_exists='replace',index = False,chunksize=16000)

        regions = pd.read_csv('/workspace/Airport_Pipeline/Airport_data/regions.csv')
        regions.to_sql('regions', con=connection, if_exists='replace',index = False,chunksize=16000)

        runways = pd.read_csv('/workspace/Airport_Pipeline/Airport_data/runways.csv')
        runways.to_sql('runways', con=connection, if_exists='replace',index = False,chunksize=16000)

        
    finally:
        connection.close()
        engine.dispose()
    

move_to_snowflake()
