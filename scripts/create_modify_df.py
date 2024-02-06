"""
Creates a base dataframe out of churn_modelling table and creates 3 separate dataframes out of it.
"""
import os
import logging
import psycopg2
import traceback
import numpy as np
import pandas as pd

postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
    
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")
    
def create_base_df(cur):
    cur.execute("""SELECT * FROM churn_modelling""")
    rows=cur.fetchall()
        
    col_names=[desc[0] for desc in cur.description]
        
    df=pd.DataFrame(rows,columns=col_names)
    print(df)
    df.drop('rownumber', axis=1, inplace=True)
    #Array of 30 (size) generated random ints with max value of 10000
    index_to_be_null=np.random.randint(10000,size=30)
    # Replace values of random indexes with nan
    df.loc[index_to_be_null,['balance','creditscore','geography']]=np.nan
    #value counts count occurence of value in a column and returns an array of value/count. the most occured is the first of the list
    most_occured_country=df['geography'].value_counts().index[0]
    #fill nan values of geography column with the most occured country
    df.fillna({'geography':most_occured_country}, inplace=True)
    #fill nan values of balance column with the median of the balance column
    avg_balance=df['balance'].mean()
    df.fillna({'balance':avg_balance}, inplace=True)
    
    creditscore_mean=df['creditscore'].mean()
    #fill nan values of creditscore column with the median of the creditscore column
    df.fillna({'creditscore':creditscore_mean}, inplace=True)
    
    return df
    
def create_creditscore_df(df):
    df_creditscore=df[['geography','gender','exited','creditscore']].groupby(['geography','gender']).agg({'creditscore':'mean','exited':'sum'})
    df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'}, inplace=True)
    df_creditscore.reset_index(inplace=True)
    
    df_creditscore.sort_values('avg_credit_score', inplace=True)
    
    return df_creditscore

def create_exited_age_correlation(df):
    df_exited_age_correlation=df.groupby(['geography','gender','exited']).agg({
        'age':'mean',
        'estimatedsalary':'mean',
        'exited':'count'
    }).rename(columns={
        'exited':'number_of_exited_or_not',
        'age':'avg_age',
        'estimated_salary':'avg_salary' 
        }).reset_index().sort_values('number_of_exited_or_not')
    
    return df_exited_age_correlation

if __name__=='__main__':
    
    df=create_base_df(cur)

    create_creditscore_df(df)
    
    print(create_exited_age_correlation(df))