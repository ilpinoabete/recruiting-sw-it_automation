from dotenv import load_dotenv
import requests as r
import json
import os

load_dotenv()

# Authentication header for NocoDB API
AUTH_HEADER = {"xc-token": os.getenv("DB_KEY")}

def list_tables() -> dict[str, str]:
    # Retrieve the list of existing tables
    table_list = r.get(
        f"http://{os.getenv('SERVER_URL')}/api/v2/meta/bases/{os.getenv('BASE_ID')}/tables",
        headers=AUTH_HEADER
    )

    if table_list.status_code != 200:
        raise Exception(f"Table list failed with status code {table_list.status_code}\n{table_list.json()}")

    table_list = table_list.json()["list"]

    # Making the user select which table to edit
    print("What table do you want to import the data to?")
    for i in range(len(table_list)):
        print(f"{i + 1}) {table_list[i]['table_name']}")

    return table_list[
        int(
            input("")
        ) - 1
    ]

def push_json(table : dict[str, str]) -> list[dict[str, str]]:
    # Making the user select the json file to upload
    file_path = input("\nWhat is the file full path? \n").strip()

    with open(file_path, 'r') as file:
        data = json.load(file)

        # Remove void fields
        data = [
            {k: v for k, v in item.items() if v}
            for item in data
        ]

        # Pushing the new entries in the selected table
        request = r.post(
            f"http://{os.getenv('SERVER_URL')}/api/v2/tables/{table['id']}/records",
            headers=AUTH_HEADER,
            json=data
        )

        if (request.status_code != 200):
            raise RuntimeError(f"Request failed with status code {request.status_code}\n{request.json()}")

        return request.json()

def update_links(affected_table: dict[str, str], affected_records: list[dict[str, str]]) -> None:
    for record in affected_records:
        # Fetch the full record from NocoDB
        response = r.get(
            f"http://{os.getenv('SERVER_URL')}/api/v2/tables/{affected_table['id']}/records/{record['Id']}",
            headers=AUTH_HEADER
        )
        if response.status_code != 200:
            raise RuntimeError(f"Request failed with status code {response.status_code}\n{response.json()}")

        db_record = response.json()
        update_payload = {}

        # Find link fields (assuming they end with '_id')
        for key, value in db_record.items():
            if key.endswith('_id') and value is None:
                # Find the linked table name (strip '_id')
                linked_table_name = key[:-3]
                # Find the record in the linked table matching the value from the original JSON
                # Here, you need to know which field to match on; assuming 'name' for example
                search_value = record.get(linked_table_name)
                if search_value:
                    linked_table_resp = r.get(
                        f"http://{os.getenv('SERVER_URL')}/api/v2/tables/{linked_table_name}/records",
                        headers=AUTH_HEADER,
                        params={'where': json.dumps({'name': search_value})}
                    )
                    if linked_table_resp.status_code == 200:
                        linked_records = linked_table_resp.json().get('list', [])
                        if linked_records:
                            update_payload[key] = linked_records[0]['id']

        # Update the record if any link fields were found
        if update_payload:
            update_resp = r.patch(
                f"http://{os.getenv('SERVER_URL')}/api/v2/tables/{affected_table['id']}/records/{record['Id']}",
                headers=AUTH_HEADER,
                json=update_payload
            )
            if update_resp.status_code != 200:
                print(f"Failed to update links for record {record['Id']}: {update_resp.json()}")

def main():
    data_insertion_needed = True

    while data_insertion_needed:
        table = list_tables()
        affected_records = push_json(table)
        update_links(table, affected_records)

        data_insertion_needed = True if (input("Would you like to upload another file? [y/N] ").lower() == "y") else False

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        exit(0)