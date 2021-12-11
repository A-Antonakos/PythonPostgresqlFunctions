# -----------------------------------------------------------------------------
# --------------------------  ------------------ ------------------------------
# -------------------------- --------------------------------------------------
# ----------------------------- September 2020 --- ----------------------------
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# Import to postgresql 12 
# =============================================================================

import sqlalchemy.types 
from sqlalchemy import func
from sqlalchemy import create_engine

f = open('/home/antonis/Projects/PythonProjects/PROJ_G_CRYPTO/init.txt', mode = 'r')
lines = f.readlines()
username = lines[0]
password = lines[1]
database = lines[2]

## Strip new line characters
username = username.strip()
password = password.strip()
database = database.strip()

connection_string = 'postgresql+psycopg2://' + username + ':' + password +'@localhost:5432/' + database

# =============================================================================
# functions that import dataframe to postgresql
# =============================================================================
def f_import_dataframe(df,tablename):
    ## Use sqlalchemy -- Create a connection
    engine = create_engine(connection_string)
    df.to_sql(tablename, engine,chunksize = 2000, if_exists='replace')
    engine.dispose()
    
## Import dataframe to postgresql , explicitly load a column as string    
def f_import_dataframe_col_as_string(df,tablename,colname,index_indicator):
    ## Use sqlalchemy -- Create a connection
    engine = create_engine(connection_string)
    print(colname)
    df.to_sql(tablename, engine,chunksize = 2000,  if_exists='replace',dtype={colname: sqlalchemy.types.String()}, index = index_indicator)
    engine.dispose()
    
## Import dataframe to postgresql , all columns as string    
def f_import_dataframe_all_cols_as_string(df,tablename,index_indicator):
    ## Use sqlalchemy -- Create a connection
    engine = create_engine(connection_string)
    df.to_sql(tablename, engine,chunksize = 2000,  if_exists='replace',dtype={col_name: sqlalchemy.types.String() for col_name in df}, index = index_indicator)
    engine.dispose()

    
## Import dataframe to postgresql , load a list of columns as string    
def f_import_dataframe_list_of_cols_as_string(df,tablename,col_list,index_indicator):
    ## Use sqlalchemy -- Create a connection
    engine = create_engine(connection_string)
    df.to_sql(tablename, engine,chunksize = 2000,  if_exists='replace',dtype={col_name: sqlalchemy.types.String() for col_name in col_list}, index = index_indicator)
    ##df.to_sql(tablename, engine,chunksize = 2000,  if_exists='replace',dtype={col_name: String for col_name in col_list}, index = index_indicator)
    engine.dispose()
    
# =============================================================================
# functions that export a table from postgresql to pandas    
# =============================================================================
def f_export_pgTable_to_dataframe(tablename):   
    ## Use sqlalchemy -- Create a connection
    engine = create_engine(connection_string)
    sqlDF = pd.read_sql_table(tablename, con=engine)
    engine.dispose()
    return sqlDF
    
# =============================================================================
# functions that get an an sql statement and returns results as a dataframe     
# =============================================================================
def f_select_to_dataframe(strSQL):   
    ## Use sqlalchemy -- Create a connection
    engine = create_engine(connection_string)
    sqlDF = pd.read_sql_query(strSQL,con=engine)
    engine.dispose()
    return sqlDF




def f_execute_ps_function(s_func_name, tablename,ind):

    try:
       ps_connection = psycopg2.connect(user    = username,
                                       password = password,
                                       host     = "localhost",
                                       port     ="5432",
                                       database = database )
       
       
    
       
       cursor = ps_connection.cursor()
       
       #call stored procedure
       cursor.callproc(s_func_name,[tablename,ind])
       ps_connection.commit()
         
    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while connecting to PostgreSQL", error)
    
    finally:
        #closing database connection.
        if(ps_connection):
            cursor.close()
            ps_connection.close()
            print("PostgreSQL connection is closed")



def f_insert_record(sql_insert,record):
    try:
       ps_connection = psycopg2.connect(user    = username,
                                       password = password,
                                       host     = "localhost",
                                       port     ="5432",
                                       database = database )
       
       
    
       
       cursor = ps_connection.cursor()
       
       
       
       cursor.execute(sql_insert, record)
       ps_connection.commit()
       row_count = cursor.rowcount
       
       
       print (row_count, "Record inserted successfully into mobile table") 
    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while connecting to PostgreSQL", error)
    
    finally:
        #closing database connection.
        if(ps_connection):
            cursor.close()
            ps_connection.close()
            print("PostgreSQL connection is closed")
    
    
def f_insert_image(sql_insert,file_name):
    try:
       ps_connection = psycopg2.connect(user    = username,
                                       password = password,
                                       host     = "localhost",
                                       port     ="5432",
                                       database = database )
       
       
    
       
       cursor = ps_connection.cursor()
       
       
       
       cursor.execute(sql_insert, 'bytea(' + file_name +')')
       ps_connection.commit()
       row_count = cursor.rowcount
       
       
       print (row_count, "Image inserted successfully into mobile table") 
    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while connecting to PostgreSQL", error)
    
    finally:
        #closing database connection.
        if(ps_connection):
            cursor.close()
            ps_connection.close()
            print("PostgreSQL connection is closed")


 
