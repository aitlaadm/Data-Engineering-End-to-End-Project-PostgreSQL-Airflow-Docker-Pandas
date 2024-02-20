Simple end to end ETL pipeline using airflow, pandas, docker and postgresql DB where we are going to get a CSV file from a remote repo, download it to the local working directory, create a local PostgreSQL table, and write this CSV data to the PostgreSQL table with write_csv_to_postgres.py script.

Then, we will get the data from the table. After some modifications and pandas practices, we will create 3 separate data frames with the create_df_and_modify.py script.

In the end, we will get these 3 data frames, create related tables in the PostgreSQL database, and insert the data frames into these tables with write_df_to_postgres.py

All these scripts will run as Airflow DAG tasks with the DAG script.

Think of this project as a practice of pandas and an alternative way of storing the data in the local machine.

NB: Start the project in a linux environnment, airflow have dependencies issues on windows !

start by installing depndencies in requirements.txt with the command : pip install -r requirements.txt

Then you clone this project, you'll automatically clone @dogukannulu project which is a dockerized airflow image. After cloning the repo, run the following command only once so that all dependencies are configured and ready to use. To run it you must execute : docker-compose -f docker-compose-LocalExecutor.yml

