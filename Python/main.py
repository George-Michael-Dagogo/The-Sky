from sqlalchemy import create_engine
import pandas as pd
import snowflake.connector



path = "../The-Sky/Python/csv_files"

def extract_data_sources():
    if not os.path.exists(path):
        os.makedirs(path)


    for i in os.listdir(path):
        os.remove(path +'/' + i)
    #empties the directory

        
    os.system('wget -P ../The-Sky/Python/csv_files -i ../The-Sky/Python/airport.txt')


def move_to_snowflake():
        # Snowflake connection parameters
    snowflake_account = "tl25793.eu-west-3.aws"
    snowflake_user = "George"
    snowflake_password = "George1234"
    snowflake_database = "STAGING"
    snowflake_schema = "AIR_STAGING"
    snowflake_warehouse = "COMPUTE_WH"

    # Snowflake connection
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )

    engine = create_engine(f'snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}')
    
    airports = pd.read_csv('../The-Sky/Python/csv_files/airports.csv')
    airports.to_sql('airports', engine, if_exists='replace', index=False, chunksize=16000)  

    airport_frequencies = pd.read_csv('../The-Sky/Python/csv_files/airport-frequencies.csv')
    airport_frequencies.to_sql('airport_frequencies', engine, if_exists='replace',index = False,chunksize=16000) 

    countries = pd.read_csv('../The-Sky/Python/csv_files/countries.csv')
    countries.to_sql('countries', engine, if_exists='replace',index = False,chunksize=16000)

    navaids = pd.read_csv('../The-Sky/Python/csv_files/navaids.csv')
    navaids.to_sql('navaids', engine, if_exists='replace',index = False,chunksize=16000)

    regions = pd.read_csv('../The-Sky/Python/csv_files/regions.csv')
    regions.to_sql('regions', engine, if_exists='replace',index = False,chunksize=16000)

    runways = pd.read_csv('../The-Sky/Python/csv_files/runways.csv')
    runways.to_sql('runways', engine, if_exists='replace',index = False,chunksize=16000)

    # Close the Snowflake connection
    conn.close()


