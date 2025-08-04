from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import requests
import tarfile

#DAG arguments
default_args = {
'owner': 'owner1',
'start_date': days_ago(0),
'email': ['nussb003@csusm.edu'],
'retries': 1,
'retry_delay': timedelta(minutes=5),
}

#define the DAG(Directed Acyclic Graph)
dag = DAG('ETL_toll_data',
   default_args=default_args,
   description='Apache Airflow Final Assignment PyOp',
   schedule_interval=timedelta(days=1),
)

source_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
destination_path = '/home/project/airflow/dags/python_etl/staging'

def download_dataset():
    response = requests.get(source_url, stream=True)
    if response.status_code == 200:
        with open(destination_path, 'wb') as f:
            f.write(response.content)
        print("Download completed successfully.")
    else:
        print("Failed to download the file.")

def untar_dataset():
    with tarfile.open(destination_path, "r:gz") as tar:
        tar.extractall(path=extract_path)
    print("Extraction completed successfully.")

# define tasks
# task1-extract toll data (Rowid, Timestamp, vehicle number, vehicle type) from csv
def extract_data_from_csv ():
    global destination_path
    input_file = destination_path + '/vehicle-data.csv'
    extracted_file = destination_path +'/csv_data.csv'
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, open(extracted_file, 'w') as outfile:
    #create headers
        outfile.write("Rowid,Timestamp,Anonymized_Vehicle_Number,Vehicle_Type\n")
        next(infile)
        for line in infile:    
            fields = line.split(',')    
            if len(fields) >= 6 : 
                field_1 = fields[0] 
                field_2 = fields[1] 
                field_3 = fields[2]    
                field_4 = fields[3]       
                outfile.write(field_1 + "," + field_2 + "," + field_3 + field_4 + "\n")

# task3-extract (number of axles, tollplaza id, tollplaza code) from tsv
def extract_data_from_tsv():
    global destination_path
    input_file = destination_path + '/tollplaza-data.csv'
    extracted_file = destination_path +'/tsv_data.csv'
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, open(extracted_file, 'w') as outfile:
        #create headers
        outfile.write("Number_of_Axles,Tollplaza_id,Tollplaza_Code\n")
        next(infile)
        for line in infile:    
            fields = line.split('\t')    
            if len(fields) >= 7 : 
                field_1 = fields[4] 
                field_2 = fields[5] 
                field_3 = fields[6]        
                outfile.write(field_1 + "," + field_2 + "," + field_3 + "\n")

# task4-extract (payment code, vehicle code) using fixed width from csv
def extract_data_from_fixed_width():
    global destination_path
    input_file = destination_path + '/payment-data.csv'
    extracted_file = destination_path +'/fixed_width_data.csv'
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, open(extracted_file, 'w') as outfile:
        #create headers
        outfile.write("Payment_Type,Vehicle_Code\n")
        next(infile)
        for line in infile:    
            fields = line[59:67].split(' ')    
            if len(fields) >= 2 :   
                field_1 = fields[0] 
                field_2 = fields[1]         
                outfile.write(field_1 + "," + field_2 + "\n")

# task5-consolidate data into one csv
def consolidate_data():
    global destination_path
    input_file1 = destination_path + '/csv_data.csv'
    input_file2 = destination_path + '/tsv_data.csv'
    input_file3 = destination_path + '/fixed_width_data.csv'
    extracted_file = destination_path +'/extracted_data.csv'
    # Read the contents of the file into a string
    with open(input_file1, 'r') as infile1, open(input_file2, 'r') as infile2, \
        open(input_file3, 'r') as infile3, open(extracted_file, 'w') as outfile:

        for line1, line2, line3 in zip(infile1, infile2, infile3):      
           field_1_4 = line1.split(',')
           field_5_7 = line2.split(',')
           field_8_9 = line3.split(',')       
           outfile.write(','.join(field_1_4 + field_5_7 + field_8_9) + '\n')

# task6-transform (uppercase)
def transform_data():
    global destination_path    
    input_file = destination_path + '/extracted_data.csv' 
    extracted_file = destination_path + 'tansformed_data.csv'
    with open(input_file, 'r') as infile, open(extracted_file, 'w') as outfile:    
        for line in infile:
            field_1_3 = line.split(',')[0:2]
            field_4 = line.split(',')[3].upper()
            field_5_8 = line.split(',')[4:7]
            outfile.write(','.join(field_1_3 + field_4 + field_5_8) + '\n')


# task pipeline
untar_dataset >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
