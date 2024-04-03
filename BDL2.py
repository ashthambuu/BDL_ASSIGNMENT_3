from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import csv
import ast
import pandas as pd
import os
import glob

current_working_directory = os.getcwd()

with DAG(
     dag_id="BDL2",
     start_date=datetime(2021, 1, 1),
     schedule=None,) as dag_:

    # Getting the zip file path
    zipfile_location = input('Enter the complete zip file path (<directory>/individual_files.zip')
    zipfile_directory = os.path.dirname(zipfile_location)

    current_working_directory = os.getcwd()
    
    # Function to check the file exist in that name or not
    def check_file_existance():
        start_time = datetime.now()
        timeout = 5
        file_sensor = FileSensor(
            task_id='file_sensor_task',
            filepath=zipfile_location,
            timeout=timeout,
            poke_interval=5,
            dag=dag_
        )
        file_sensor.execute(context={})  
        elapsed_time = (datetime.now() - start_time).total_seconds()
        if elapsed_time < timeout:
            print('\n Successfully found the zipfile at the desired location \n')

    # Python operator to check the file existance
    check_file_existance_task = PythonOperator(
        task_id='check_file_existance',
        python_callable=check_file_existance,
        dag=dag_
    )

    # Bash operator to check whether the file is zip file or not; if it is a zip file then unzip the file in the same location
    validate_and_unzip_task = BashOperator(
        task_id='validate_and_unzip_task',
        bash_command=f'if unzip -t {zipfile_location} > /dev/null; then unzip -o {zipfile_location} -d /home/dell/Documents; else echo "Not a valid archive"; fi',
        dag=dag_
    )


    # Function which uses beam to process the csv file and to make in the tuple form as given in the question. Finally written in the file with file name <csv_file_name>_task2.3_00000-of
    # -00001.txt which is created in current working directory  
    def process_csv_and_filter(input_file, column_numbers):
        with beam.Pipeline() as p:
            # Read the CSV file
            data = p | 'ReadCSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
    
            parsed_data = (
                data
                | 'ParseCSV' >> beam.Map(lambda line: next(csv.reader([line])))
                | 'ToTuple' >> beam.Map(lambda fields: (
                    (float(fields[2]), float(fields[3])),  # Lat/Lon as a tuple
                    tuple(float(fields[i]) if fields[i] else None for i in column_numbers))
            ))
       
            grouped_data = (
                parsed_data
                | 'GroupByLatLon' >> beam.GroupByKey()
                | 'CollectHourlyData' >> beam.Map(lambda kv: (
                    kv[0],
                    [list(pair) for pair in kv[1]]
                ))
            )
    
            # Print the final tuple structure as per given in the question
            _ = grouped_data | 'PrintResult' >> beam.Map(print)
    
            base_name = os.path.basename(input_file)
            output_file_prefix = base_name.replace('.csv', '_task2.3')
            _ = grouped_data | 'WriteToFile' >> beam.io.WriteToText(output_file_prefix)
        
    
    
    # Function to extract the column number from the required columns using the unziped csv files
    def extract_required_fields():
        column_names_to_find = ['HourlyWindSpeed','HourlyDryBulbTemperature']
        
        directory_path = zipfile_directory + '/'
        all_files = os.listdir(directory_path)
        csv_files = [file for file in all_files if file.endswith('.csv')]
        
        for file in csv_files:
            column_numbers = []
            input_file_path = directory_path + file
            print(f'\n PROCESSING THE FILE {file} \n')
            with open(input_file_path, 'r') as file:
                csv_reader = csv.reader(file)
                headers = next(csv_reader)
                for column_name in column_names_to_find:
                    try:
                        column_number = headers.index(column_name)
                        column_numbers.append(column_number)
                    except ValueError:
                        print(f"Column '{column_name}' not found in the CSV file.")
            
            # Execute the pipeline using Apache Beam
            process_csv_and_filter(input_file_path, column_numbers)


    # Python operator to extract the required field
    extract_required_fields_task = PythonOperator(
        task_id='extract_required_fields',
        python_callable=extract_required_fields,
        dag=dag_
    )

    # Class is defined to compute monthly averages of required fields. 
    # Contains contructor and method 'process' which return the lat,lon and computed monthly averages in the form of tuples and list of arrays
    class ComputeMonthlyAverages(beam.DoFn):
        def __init__(self, input_file):
            self.input_file = input_file   
        def process(self, element):
            print(f'\n PROCESSING THE FILE {self.input_file} \n')
            lat_lon_tuple, monthly_data = element
            csv_file = self.input_file.split('_')[0]
            df = pd.read_csv(csv_file + '.csv')
            column_values = df['DATE'].tolist()
            months = [int(value.split('-')[1]) for value in column_values]
    
           
            monthly_dict = {}
            for i in range(len(months)):
                month = int(months[i])
                if month not in monthly_dict.keys():
                    monthly_dict[month]= []
                monthly_dict[month].append(monthly_data[i])
            
            
            monthly_av_list=[]
            for month in monthly_dict.keys():
                monthly_av_list.append([])
                data_list = monthly_dict[month]
                for i in range(len(data_list[0])):
                    sum=0
                    count=0
                    for j in range(len(data_list)):
                        if data_list[j][i]:
                            count+=1
                            sum+=int(data_list[j][i])
                    av = sum/count
                    monthly_av_list[month-1].append(av)
                
            return (lat_lon_tuple, monthly_av_list)

    # Function which uses beam to compute monthly averages. Creates a file in the current working directory namely '<csvfile_name>_task2.4_00000-of-00001.txt'
    def compute_monthly_average():
        options = PipelineOptions()
        search_term = 'task2.3'
    
        matching_files = [file for file in os.listdir(current_working_directory) if search_term in file and os.path.isfile(current_working_directory + '/' + file)] 
        for file in matching_files:
            input_file = current_working_directory + '/' + file
            with beam.Pipeline(options=options) as pipeline:
                data = pipeline | 'ReadInput' >> beam.io.ReadFromText(input_file)
        
                parsed_data = data | 'ParseInput' >> beam.Map(eval)  # Assuming input is in text format and evaluable
        
                monthly_averages = parsed_data | 'ComputeMonthlyAverages' >> beam.ParDo(ComputeMonthlyAverages(input_file))
                
                _= monthly_averages | 'PrintMonthlyAverages' >> beam.Map(print)
                output_file_name = file.replace('task2.3-00000-of-00001','task2.4')
                monthly_averages | 'WriteOutput' >> beam.io.WriteToText(output_file_name)
                
    
    # Python operator to compute monthly average for the required fields
    compute_monthly_average_task = PythonOperator(
        task_id='compute_monthly_average',
        python_callable=compute_monthly_average,
        dag=dag_
    )

    # Function to delete the csv files which were unzipped in task2.1
    def delete_csv_files():
        pattern = '*.csv'

        csv_files = glob.glob(os.path.join(zipfile_directory, pattern))
        for file in csv_files:
            os.remove(file)
            
    # Python operator to delete csv files
    delete_csv_files_task = PythonOperator(
        task_id='delete_csv_files',
        python_callable=delete_csv_files,
        dag=dag_
    )

    # Task dependencies
    check_file_existance_task >> validate_and_unzip_task >> extract_required_fields_task >> compute_monthly_average_task >> delete_csv_files_task

dag_.test()