import requests
import os
import boto3
import configparser
def run_facts_etl():
    url = "https://meowfacts.herokuapp.com/"

    try:
        response = requests.get(url)
        file_path = 'meowfact.txt'

        # check if the request was successful (status code 200)
        if(response.status_code == 200):
            facts = response.json()
            print("A meow fact per day - API start")

            # write json in a file
            if facts:
                write_indexed_text(file_path, facts)

            print("complete writing to a file")

            # upload to a place
            print("pending for upload...")
            print("\n")
            upload_file(file_path)

        else:
            print("Error: ", response.status_code)

    except requests.exceptions.RequestException as e:
        print("Error: ", e)

def write_indexed_text(file_path, facts):
    # write data into file
    if (os.path.isfile(file_path)):
        with open(file_path, 'r+') as file:
            lines = file.readlines()
            if lines:
                last_line = lines[-1].strip()
                try:
                    index = int(last_line.split(",")[0]) + 1
                except ValueError:
                    print("Error: Last line doesn't contain a valid index")
                    index = 1
            else:
                index = 1
            file.seek(0, 2)
            file.write(f'{index}, "{facts["data"][0] }" \n')
    else:
        with open(file_path, 'w') as file:
            file.write(f'1, "{ facts["data"][0] }" \n')

def upload_file(file_name, object_name=None):
    config = configparser.ConfigParser()
    ini_file_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    
    config.read(ini_file_path)
    bucket_name = config['s3']['bucket_name']

    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3 = session.client('s3')

    if object_name is None:
        object_name = file_name

    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print("Upload Successful")
    except Exception as e:
        print(f"An error occurred : {e}")