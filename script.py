from dotenv import load_dotenv
import requests as r
import json
import os

load_dotenv()

# Authentication header for NocoDB API
AUTH_HEADER = {"xc-token": os.getenv("DB_KEY")}

def push_json():
    # Retrieve the list of existing tables
    table_list = r.get(
        f"http://{os.getenv('SERVER_URL')}/api/v2/meta/bases/{os.getenv('BASE_ID')}/tables",
        headers=AUTH_HEADER
    ).json()

    table_list = table_list["list"]

    # Making the user select which table to edit
    print("What table do you want to import the data to?")
    for i in range(len(table_list)):
        print(f"{i+1}) {table_list[i]['table_name']}")

    table = table_list[int(input("\n")) - 1]

    # Making the user select the json file to upload
    file_path = input("What is the file full path? \n").strip()

    with open(file_path, 'r') as file:
        data = json.load(file)

        # Pushing the new entries in the selected table
        request = r.post(
            f"http://{os.getenv('SERVER_URL')}/api/v2/tables/{table['id']}/records",
            headers=AUTH_HEADER,
            json=data
        )
        # Professional debug
        print(request.json())

def main():
    data_insertion_needed = True

    while data_insertion_needed:
        try:
            push_json()

            data_insertion_needed = True if (input("Would you like to upload another file? [y/N] ") == "y") else False

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        exit(0)