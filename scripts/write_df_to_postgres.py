import psycopg2
import pandas as pd
import numpy as np
from create_modify_df import create_base_df, create_creditscore_df, create_exited_age_correlation, create_exited_salary_correlation

import traceback
import os
import logging

postgres_host= os.environ.get("postgres_host")
postgres_database=os.environ.get("postgres_database")
postgres_user=os.environ.get("postgres_user")
postgres_password=os.environ.get("postgres_password")
postgres_port=os.environ.get("postgres_port")

try:
    conn=psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cur=conn.cursor()
    logging.info("Successfuly connected to Postgres")
except Exception as e:
    logging.error(f"Error while connecting to Postgres due to {e}")
    
def create_tables_postgres():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (geography VARCHAR(50), gender VARCHAR(20), avg_credit_score FLOAT, total_exited INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (geography VARCHAR(50), gender VARCHAR(20), exited INTEGER, avg_age FLOAT, avg_salary FLOAT,number_of_exited_or_not INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation (exited INTEGER, is_greater INTEGER, correlation INTEGER)""")
        logging.info("Tables created successfully")
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Failed to create postgres tables due to {e}")
        
def insert_to_creditscore(creditscore_df):
    query="INSERT INTO churn_modelling_creditscore (geography, gender, avg_credit_score, total_exited) VALUES (%s,%s,%s,%s)"
    row_count=0
    for _,row in creditscore_df.iterrows():
        values=(row['geography'],row['gender'],row['avg_credit_score'], row['total_exited'])
        cur.execute(query, values)
        row_count+=1
    logging.info(f"{row_count} rows had been inserted to churn_modelling_credit_score")

def insert_to_churn_modelling_age_correlation(df_age_correlation):
    query="""INSERT INTO churn_modelling_exited_age_correlation (geography, gender, exited, avg_age, avg_salary, number_of_exited_or_not) VALUES (%s,%s,%s,%s,%s,%s)"""
    row_count=0
    for _,row in df_age_correlation.iterrows():
        values=(row['geography'],row['gender'],row['exited'],row['avg_age'],row['estimatedsalary' ],row['number_of_exited_or_not'])
        cur.execute(query, values)
        row_count+=1
    logging.info(f"{row_count} rows had been inserted to churn_modelling_credit_score")    

def insert_to_churn_modelling_salary_correlation(df_salary_correlation):
    query="""INSERT INTO churn_modelling_exited_salary_correlation (exited, is_greater, correlation) VALUES (%s,%s,%s)"""
    row_count=0
    for _, row in df_salary_correlation.iterrows():
        values=(int(row['exited']), int(row['is_greater']), int(row['correlation']))
        cur.execute(query, values)
        row_count+=1
    logging.info(f"{row_count} rows had been inserted to churn_modelling_exited_salary_correlation")  
        
def check_db():
    count_query ="""SELECT * FROM churn_modelling_exited_salary_correlation"""
    cur.execute(count_query)
    result = cur.fetchall()
    
    return result

def write_df_to_postgres_main():
    main_df = create_base_df(cur)
    df_creditscore = create_creditscore_df(main_df)
    df_exited_age_correlation = create_exited_age_correlation(main_df)
    df_exited_salary_correlation = create_exited_salary_correlation(main_df)

    create_tables_postgres()
    insert_to_creditscore(df_creditscore)
    insert_to_churn_modelling_age_correlation(df_exited_age_correlation)
    insert_to_churn_modelling_salary_correlation(df_exited_salary_correlation)

    conn.commit()
    cur.close()
    conn.close()
    
if __name__=='__main__':
    
    main_df=create_base_df(cur)
    df_creditscore=create_creditscore_df(main_df)
    df_exited_age_correlation=create_exited_age_correlation(main_df)
    df_exited_salary_correlation=create_exited_salary_correlation(main_df)
    
    create_tables_postgres()
    insert_to_creditscore(df_creditscore)
    insert_to_churn_modelling_age_correlation(df_exited_age_correlation)
    insert_to_churn_modelling_salary_correlation(df_exited_salary_correlation)
    
    print(check_db())