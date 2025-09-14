from dotenv import load_dotenv
import requests as r
import json
import os

load_dotenv()

# Authentication header for NocoDB API and Base URL
AUTH_HEADER = {"xc-token": os.getenv("DB_KEY")}
BASE_URL = f"http://{os.getenv('SERVER_URL')}/api/v2"

def list_tables() -> dict[str, str]:
    """Retrieve the list of existing tables and prompts the user to select one."""
    table_list_url = f"{BASE_URL}/meta/bases/{os.getenv('BASE_ID')}/tables"
    table_list = r.get(table_list_url, headers=AUTH_HEADER)

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

    # Pushing the new entries in the selected table
    request_url = f"{BASE_URL}/tables/{table['id']}/records"
    request = r.post(request_url, headers=AUTH_HEADER, json=processed_data)

    if request.status_code != 200:
        raise RuntimeError(f"Request failed with status code {request.status_code}\n{request.json()}")

    return request.json()

def update_links(table: dict, affected_records: list[dict], original_data: list[dict]) -> None:
    """
    Automatically creates links for newly inserted records based on a configuration
    defined in the 'LINKED_FIELDS' environment variable.

    Args:
        table (dict): The table object of the records just inserted.
        affected_records (list): The list of records returned by the initial POST request.
        original_data (list): The original JSON data used for the insertion.
    """

    link_config = os.getenv("LINKED_FIELDS")

    # Filter config to only include rules for the table we just edited
    relevant_rules = [
        rule for rule in link_config if rule[0][0] == table.get("title")
    ]
    if not relevant_rules:
        print(f"INFO: No linking rules found for table '{table.get('title')}'.")
        return

    # Cache all table and column metadata to avoid repeated API calls ---
    print("INFO: Fetching database metadata...")
    try:
        base_meta_url = f"{BASE_URL}/meta/bases/{os.getenv('BASE_ID')}"
        all_tables_meta = r.get(f"{base_meta_url}/tables", headers=AUTH_HEADER).json()["list"]
        metadata_cache = {
            tbl["title"]: {
                "id": tbl["id"],
                "columns": {
                    col["column_name"]: col
                    for col in r.get(f"{base_meta_url}/tables/{tbl['id']}", headers=AUTH_HEADER).json()["columns"]
                }
            } for tbl in all_tables_meta
        }
    except Exception as e:
        print(f"ERROR: Failed to fetch metadata. Cannot proceed with linking. Details: {e}")
        return

    # Iterate through each new record and apply the relevant linking rules ---
    for i, new_record in enumerate(affected_records):
        parent_record_id = new_record.get("Id")
        if not parent_record_id:
            continue

        for rule in relevant_rules:
            parent_info, linked_info = rule
            parent_table_name, parent_field_name = parent_info
            linked_table_name, linked_lookup_field = linked_info

            # Get the value to look up from the original JSON (e.g., a person's name, a project title)
            lookup_value = original_data[i].get(parent_field_name)
            if not lookup_value:
                continue

            # Use the cache to get table and column IDs
            linked_table_id = metadata_cache[linked_table_name]["id"]

            # Find the related record by its lookup value
            find_url = f"{BASE_URL}/tables/{linked_table_id}/records?where=({linked_lookup_field},eq,{lookup_value})"
            find_resp = r.get(find_url, headers=AUTH_HEADER)

            if find_resp.status_code == 200 and find_resp.json().get("list"):
                related_record_id = find_resp.json()["list"][0].get("Id")
            else:
                print(f"WARN: Could not find a record in '{linked_table_name}' where '{linked_lookup_field}' is '{lookup_value}'.")
                continue

            # Use the dedicated linking API to create the link ---
            link_field_id = metadata_cache[parent_table_name]["columns"][parent_field_name]["id"]
            parent_table_id = metadata_cache[parent_table_name]["id"]

            link_url = f"{BASE_URL}/tables/{parent_table_id}/links/{link_field_id}/records/{parent_record_id}"
            link_payload = [related_record_id] # API expects a list of IDs
            link_resp = r.post(link_url, headers=AUTH_HEADER, json=link_payload)

            if link_resp.status_code == 200:
                print(f"SUCCESS: Linked record {parent_record_id} via '{parent_field_name}' to '{lookup_value}' in '{linked_table_name}'.")
            else:
                print(f"ERROR: Failed to create link for record {parent_record_id}. Status: {link_resp.status_code}, Response: {link_resp.text}")


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

            update_links(table, affected_records, original_data)

        data_insertion_needed = input("\nWould you like to upload another file? [y/N] ").lower() == "y"

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nExiting program.")
        exit(0)
    except Exception as e:
        print(f"\nAn error occurred: {e}")