import pandas as pd
from sqlalchemy import create_engine,text
from util import get_database_conn
import mpu


# Create a connection to the Microsoft Access database file
access_db_engine = create_engine("access+pyodbc://@wpidata")
wpi_data_tables = 'WPI Data'

# Function to load data from access database to postgresql database
def load_data_to_db():
    '''
    This function establish connection to the Microsoft access database file and loads each table
    in the database to a postgresql database instance.

    Parameter: Does not accept a parameter
    Return value: Does not return a value. It performs a write operation to the database
    Return type: None
    '''
    postgres_engine = get_database_conn()
    
    table_data = pd.DataFrame(access_db_engine.connect().execute(text('Select * from "WPI Data"')))  
   
    final_list =[]
    ### QUESTION 1 SOLUTION
    new_df=table_data[table_data['Main_port_name'] == 'JURONG ISLAND']
    for index,row in table_data.iterrows():
        row['distance'] = (mpu.haversine_distance((1, 103), (row['Latitude_degrees'], row['Longitude_degrees']))) * 1000
        intermediate_list = [row['distance'],row['Main_port_name'],row['Latitude_degrees'],row['Longitude_degrees'],row['Wpi_country_code']]
        final_list.append(intermediate_list)

    final_df = pd.DataFrame(final_list,columns = ['distance','port_name','latitude','longitude','country_code'])
    table_df = final_df.sort_values('distance',ascending=True).head(5)
    table_df = table_df[['distance','port_name']]
    table_df.to_sql("closest_ports", con= postgres_engine, if_exists='replace',index=False)

    #### QUESTION 2 SOLUTION
    table_2 = table_data[table_data['Load_offload_wharves'] == 'Y']
    table = table_2['Wpi_country_code'].value_counts(ascending = False)
    table_2 = pd.DataFrame(table)
    table_2['count'] = table_2['Wpi_country_code']
    table_2 = table_2['count'].head(1)
    table_2.to_sql("highest_wharves", con= postgres_engine, if_exists='replace',index_label='Country')
    print(table_2.head(1))

    ##### QUESTION 3 SOLUTION
    distress_call_df = table_data[(table_data['Supplies_provisions'] == 'Y') & (table_data['Supplies_water'] == 'Y') & (table_data['Supplies_fuel_oil'] == 'Y') & (table_data['Supplies_diesel_oil'] == 'Y') ]
    distress_call_df = distress_call_df[['Wpi_country_code','Main_port_name','Latitude_degrees','Longitude_degrees']]
    print(distress_call_df.head(5))
    final_list=[]
    for index,row in distress_call_df.iterrows():
        row['distance'] = (mpu.haversine_distance((32.610982,-38.706256), (row['Latitude_degrees'], row['Longitude_degrees']))) * 1000
        intermediate_list = [row['Wpi_country_code'],row['Main_port_name'],row['Latitude_degrees'],row['Longitude_degrees'],row['distance']]
        final_list.append(intermediate_list)
    distress_call_df = pd.DataFrame(final_list,columns = ['Wpi_country_code','Main_port_name','latitude','longitude','distance'])
    table_df_2 = distress_call_df.sort_values('distance',ascending=False).head(1)
    table_df_2 = table_df_2[['Wpi_country_code','Main_port_name','latitude','longitude']]
    table_df_2.to_sql("distress_call_port", con= postgres_engine, if_exists='replace',index=False)





