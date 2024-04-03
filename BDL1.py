import os
import re
import random
import shutil
import zipfile
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

current_working_directory = os.getcwd()

# initialising dag 
with DAG(
     dag_id="BDL1",
     start_date=datetime(2021, 1, 1),
     schedule=None,) as dag:
    root_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'
    year = input("Enter the page's year to download") 

    # Fetching the page using year as input
    page_fetch_task = BashOperator(
        task_id="page_fetch",
        bash_command= f'wget -O {current_working_directory}/{year}-data.html {root_url}{year}/',
        dag=dag
    )   

    # Selecting the list of data files from the given year link
    def select_data_files():
        with open(f'{year}-data.html', 'r') as f:
            file_content = f.read()
        
        pattern = r'<a\s+href="([^"]+\.csv)"'
        files = re.findall(pattern, file_content)
        
        # Print the list of files
        print(f'List of files in the year {year}: {files}')
    
        num_files_needed = int(input(f'Enter the number of files needed (<= {len(files)})'))
        if num_files_needed > len(files):
            print(f'Number should be <= {len(files)}')
        selected_files = random.sample(files, num_files_needed)
        print(f'List of randomly selected files from the year {year}: {selected_files}')

        # Selected_files.txt contains the names of all the selected files of the given year
        with open('Selected_files.txt','w') as f:
            [f.write(f'{file} \n') for file in selected_files]
        return selected_files

    # Python operator to do select file task 
    select_files_task = PythonOperator(
        task_id='selects_files',
        python_callable=select_data_files,
        dag=dag
    )

    # Fetching individual files using the created file 'selected_file.txt' and downloading it and saving in the current working directory
    def individual_files_fetch():
        with open('Selected_files.txt','r') as f:
            for line in f:
                file_name = line.strip()
                subprocess.run(f'wget -O {current_working_directory}/{file_name} {root_url}{year}/{file_name}',shell=True, check=True)
    
    # Python operator to do fetching individual file task
    individual_files_fetch_task = PythonOperator(
        task_id='individual_files_fetch',
        python_callable=individual_files_fetch,
        dag=dag
    )

    # Function to zip all the individual files and named it as individual_files.zip in the current wroking directory
    def zip_individual_files():
        with open('Selected_files.txt','r') as f:
            zip_file_list = [line.strip() for line in f]
        with zipfile.ZipFile('individual_files.zip', 'w') as zf:
            for file in zip_file_list:
                zf.write(file, arcname=file)
                
    # Python operator to do zip the individual files
    zip_individual_files_task = PythonOperator(
        task_id='zip_individual_files',
        python_callable=zip_individual_files,
        dag=dag
    )

    # Function to move the zip file to the user given location
    def move_zipfile():
        source_path = current_working_directory + '/individual_files.zip'
        destination_path = input("Enter the destination path of the file 'individual_files.zip'") + 'individual_files.zip'
        shutil.move(source_path, destination_path)

    # Python operator to move zip file
    move_zipfile_task = PythonOperator(
        task_id='move_zipfile',
        python_callable=move_zipfile,
        dag=dag,
    )

    # Task dependencies
    page_fetch_task >> select_files_task >> individual_files_fetch_task >> zip_individual_files_task >> move_zipfile_task

dag.test()