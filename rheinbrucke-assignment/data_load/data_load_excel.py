import io
import glob
import csv
from decimal import Decimal
from string import Template
import datetime as dt
import psycopg2
import psycopg2.extras

from data_load_utils import *

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(funcName)s()] %(levelname)s  %(message)s")

DB_HOST="localhost"
DB_USER="postgres"
DB_PASS="pgadmin"

DATA_FILE_LOCATION = "D:/rheinbrucke/rheinbrucke-assignment/data_load/dataload/"
DROP_FILE = "D:/rheinbrucke/rheinbrucke-assignment/db/ddl/drop_all_tables.sql"
DDL_LOCATION = "D:/rheinbrucke/rheinbrucke-assignment/db/ddl/"

DML_BASE_PATH = DATA_FILE_LOCATION + "dml-scripts/"
COVID_19_DATA = DATA_FILE_LOCATION + "covid_19_data.csv"
WORLD_POPULATION = DATA_FILE_LOCATION + "world_population.csv"

SEPERATOR = "\\"

def get_rheinbrucke_tables_in_db():
    table_name_list = []
    conn = psycopg2.connect( host=DB_HOST,database="rheinbrucke",user=DB_USER,password=DB_PASS)
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute("select table_name from information_schema.tables where table_schema = 'rheinbrucke' ;")
    rows = cur.fetchall()
    for row in rows:
        table_name_list.append(row['table_name'])
    #Done with table names
    logging.info("rheinbrucke Tables[%s] are present.",table_name_list)
    cur.close()
    conn.commit()
    conn.close()
    return table_name_list
#End Function

def delete_tables():
    conn = psycopg2.connect( host=DB_HOST,database="rheinbrucke",user=DB_USER,password=DB_PASS)
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    total_tables = get_rheinbrucke_tables_in_db()
    logging.info("Total [%s] existing tables [%s]",len(total_tables),total_tables)
    if len(total_tables) != 0:
        with io.open(DROP_FILE, "r+") as f:
            delete_query = f.read()
            logging.info("Running delete_query:[\n%s\n]",delete_query)
            cur.execute(delete_query)
            logging.info("Dropped all the tables.")
    else:
        logging.info("No tables to delete.")
    cur.close()
    conn.commit()
    conn.close()
#End Function

def create_tables():
    conn = psycopg2.connect( host=DB_HOST,database="rheinbrucke",user=DB_USER,password=DB_PASS)
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    logging.info("Creating tables")
    SQL_FILE_FILTER = (DDL_LOCATION+"*.sql")
    sql_file_names_list = []
    logging.info(SQL_FILE_FILTER)

    for create_file_path in glob.glob(SQL_FILE_FILTER):
        

        file_name = create_file_path.split(SEPERATOR).pop()
        logging.info(create_file_path)
        logging.info(file_name)

        if "drop_all_tables.sql" != file_name:
            sql_file_names_list.append(file_name)
        #drop file check
    #Finshed files
    
    logging.info("sql_file_names_list[%s]",sql_file_names_list.sort(reverse=False))
    for file_name in sql_file_names_list:
        create_file_path = (DDL_LOCATION + file_name)
        logging.info("[%s]",file_name)
        with io.open(create_file_path, "r+") as f:
            create_table_query = f.read()
            cur.execute(create_table_query)
    #Finshed files
    cur.close()
    conn.commit()
    conn.close()

#End Function


def generate_covid_table_dml():
    covid_19_data_table_dml = []
    file = io.open(COVID_19_DATA)
    sheet = csv.reader(file)
    header = []
    header = next(sheet)
    for value in sheet:
        _id = value[0]
        date=value[1].split(' ')[0]
        if date[len(date)-3] == '/':
            observation_date = dt.datetime.strptime(date,'%m/%d/%y')
        elif date[len(date)-5] == '/':
            observation_date = dt.datetime.strptime(date,'%m/%d/%Y')
        elif date[len(date)-3] == '-':
            observation_date = dt.datetime.strptime(date,'%d-%m-%Y')
        elif date[len(date)-5] == '-':
            observation_date = dt.datetime.strptime(date,'%d-%m-%Y')
        #observation_date = value[1]
        province = value[2].replace("'"," ")
        country = value[3].strip("'',.()")
        date = value[4].split(' ')[0]
        if date[len(date)-3] == '/':
            last_updated_date = dt.datetime.strptime(date,'%m/%d/%y')
        elif date[len(date)-5] == '/':
            last_updated_date = dt.datetime.strptime(date,'%m/%d/%Y')
        elif date[len(date)-3] == '-':
            last_updated_date = dt.datetime.strptime(date,'%d-%m-%Y')
        elif date[len(date)-5] == '-':
            last_updated_date = dt.datetime.strptime(date,'%d-%m-%Y')
        #last_updated_date = value[4]
        confirmed = value[5]
        deaths = value[6]
        recovered = value[7]
        temp_str = ("insert into rheinbrucke.covid_19_data (_id, observation_date,province,country,last_updated_date,confirmed,deaths,recovered) select '$_id', '$observation_date' , '$province','$country','$last_updated_date','$confirmed','$deaths','$recovered';")
        temp_obj = Template(temp_str)
        dml_stmt = temp_obj.substitute(_id=_id, observation_date=observation_date,province=province,country=country,last_updated_date=last_updated_date,confirmed=confirmed,deaths=deaths,recovered=recovered)
        covid_19_data_table_dml.append(dml_stmt)
    #End for
    covid_19_data_table_file = DML_BASE_PATH + "1-covid_19_data.sql"
    logging.info("Writing covid_19_data dml [%s] with [%s] records",covid_19_data_table_file,len(covid_19_data_table_dml))
    write_dml_stmts_file(covid_19_data_table_file , covid_19_data_table_dml)
    save_db_list(covid_19_data_table_dml,1000)
#End Function


def generate_world_population_table_dml():
    world_population_data_table_dml = []
    file = io.open(WORLD_POPULATION)
    sheet = csv.reader(file)
    header = []
    header = next(sheet)
    for value in sheet:
        rank = value[0]
        CCA3 = value[1]
        country = value[2]
        capital = value[3].replace("'"," ")
        continent = value[4]
        population_2022 = value[5]
        population_2020 = value[6]
        population_2015 = value[7]
        population_2010 = value[8]
        population_2000 = value[9]
        population_1990 = value[10]
        population_1980 = value[11]
        population_1970 = value[12]
        area = value[13]
        density = value[14]
        growth_rate = value[15]
        world_population_percentage = value[16]
        temp_str = ("insert into rheinbrucke.world_population (rank, CCA3,country,capital,continent,"+
                    "population_2022,population_2020,population_2015,population_2010,population_2000,"+
                    "population_1990,population_1980,population_1970,area,density,growth_rate,"+
                    "world_population_percentage) "+
                    "select '$rank', '$CCA3','$country','$capital',"+
                    "'$continent','$population_2022','$population_2020','$population_2015','$population_2010',"+
                    "'$population_2000','$population_1990','$population_1980','$population_1970','$area','$density',"+
                    "'$growth_rate','$world_population_percentage';")
        temp_obj = Template(temp_str)
        dml_stmt = temp_obj.substitute(rank=rank, CCA3=CCA3,country=country,capital=capital,continent=continent,population_2022=population_2022,population_2020=population_2020,population_2015=population_2015,population_2010=population_2010,population_2000=population_2000,population_1990=population_1990,population_1980=population_1980,population_1970=population_1970,area=area,density=density,growth_rate=growth_rate,world_population_percentage=world_population_percentage)
        world_population_data_table_dml.append(dml_stmt)
    #End for
    world_population_data_table_file = DML_BASE_PATH + "2-world_population_data.sql"
    logging.info("Writing world_population_data dml [%s] with [%s] records",world_population_data_table_file,len(world_population_data_table_dml))
    write_dml_stmts_file(world_population_data_table_file , world_population_data_table_dml)
    save_db_list(world_population_data_table_dml,100)
#End Function



if __name__ == "__main__":
   logging.info("Running Main.")
   delete_tables()
   create_tables()
   generate_covid_table_dml()
   generate_world_population_table_dml()

# #End of Main