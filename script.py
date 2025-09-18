from dotenv import load_dotenv
import requests as r
import json
import os

load_dotenv()

# Authentication header for NocoDB API and Base URL
AUTH_HEADER = {"xc-token": os.getenv("DB_KEY")}
BASE_URL = f"http://{os.getenv('SERVER_URL')}/api/v2"
LINKS_DESCRIPTION = eval(os.getenv("LINKS_DESCRIPTION"))

def list_tables() -> dict[str, str]:
    """Retrieve the list of existing tables and prompts the user to select one."""
    table_list = r.get(
        f"{BASE_URL}/meta/bases/{os.getenv('BASE_ID')}/tables",
        headers=AUTH_HEADER
    )

    if table_list.status_code != 200:
        raise Exception(f"Table list failed with status code {table_list.status_code}\n{table_list.json()}")

    table_list = table_list.json()["list"]

    # Making the user select which table to edit
    print("What table do you want to import the data to?")
    for i, table_item in enumerate(table_list):
        print(f"{i + 1}) {table_item['title']}")

    return table_list[int(input("")) - 1]

def push_json(table: dict[str, str], data: list[dict[str, str]]) -> list[dict[str, str]]:
    """Pushes a list of records to the selected NocoDB table."""
    # Remove void fields from the data
    processed_data = [
        {k: v for k, v in item.items() if v}
        for item in data
    ]

    request = r.post(
        f"{BASE_URL}/tables/{table['id']}/records",
        headers=AUTH_HEADER,
        json=processed_data
    )

    if request.status_code != 200:
        raise RuntimeError(f"Request failed with status code {request.status_code}\n{request.json()}")

    return request.json()

def get_link_information(search_table : str, field_name : str) -> list[str, str]:
    table_id = ""
    requested_id = ""

    table_list = r.get(
        f"{BASE_URL}/meta/bases/{os.getenv('BASE_ID')}/tables",
        headers=AUTH_HEADER
    )

    if table_list.status_code != 200:
        raise Exception(f"Table list failed with status code {table_list.status_code}\n{table_list.json()}")

    for table in table_list.json()["list"]:
        if table["table_name"] == search_table:
            table_id = table["id"]
            break

    if table_id == "":
        raise Exception(f"Table not found")

    table_content = r.get(
        f"{BASE_URL}/tables/{table_id}/records",
        headers=AUTH_HEADER
    )

    if table_content.status_code != 200:
        raise Exception(f"Table data retrieve failed with status code {table_list.status_code}\n{table_list.json()}")

    for field in table_content.json()["list"]:
        if field["Title"] == field_name:
            requested_id = field["Id"]
            break
    print(f"{requested_id} {table_id}")
    return [requested_id, table_id]

def update_links(table : dict[str, str], original_file, affected_records : list[dict[str, str]]) -> bool:
    selected_link = {}

    for link in LINKS_DESCRIPTION:
        if link["table_name"] == table["table_name"]:
            selected_link = link
            break

    if selected_link == {}:
        return False

    for linked_field in selected_link["links"]:
        for i, record in enumerate(original_file):
            link_info = get_link_information(linked_field["linked_table_name"], record[linked_field["field_name"]])
            payload = {
                "Id" : link_info[0],
            }

            print(payload)

            update = r.post(
                f"{BASE_URL}/tables/{link_info[1]}/links/{linked_field["id"]}/records/{affected_records[i]['Id']}",
                headers=AUTH_HEADER,
                json=payload
            )
            print(update.json())

    return True

def main():
    """Main execution loop."""
    data_insertion_needed = True
    while data_insertion_needed:
        table = list_tables()

        file_path = input("\nWhat is the file full path? \n").strip()
        with open(file_path, 'r') as file:
            original_data = json.load(file)

            affected_records = push_json(table, original_data)

            print(f"\nSuccessfully inserted {len(affected_records)} records. Now attempting to link fields...\n")

            update_links(table, original_data, affected_records)


        data_insertion_needed = input("\nWould you like to upload another file? [y/N] ").lower() == "y"

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nExiting program.")
        exit(0)
    except Exception as e:
        print(f"\nAn error occurred: {e}")