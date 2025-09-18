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
        raise Exception(f"Request failed with status code {request.status_code}\n{request.json()}")

    return request.json()

def get_link_id(search_table : str, field_name : str) -> int:
    table_id = ""
    requested_id = None

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

        if field["Title"] in field_name:
            requested_id = field["Id"]
            break

    return requested_id

def update_links(table : dict[str, str], original_file, affected_records : list[dict[str, str]]) -> bool:
    """
    Function that updates the links in the tables if their description is present in the `.env` file in the form of:\n
    [
        {
            "table_name" : "Users",
            "links" : [
                {
                    "id" : "cpqm7kwgydoyx7r",
                    "field_name" : "Department",
                    "linked_table_name" : "Departments"
                },
                {
                    "id" : "c25rihsxrl3rn08",
                    "field_name" : "Area",
                    "linked_table_name" : "Areas"
                }
            ]
        },
        {...}
    ]

    Args:
        table: the table to be updated that was selected by the user
        original_file: the original file whose content needs to be inserted in the databsae
        affected_records: the list of records that were created by the `push_json` function
    Returns:
        True if the links were updated successfully
        False if the links were not updated successfully

    """

    # The dictionary that is supposed to store the `LINKS_DESCRIPTION` data relative to the selected table
    selected_link = {}

    for link in LINKS_DESCRIPTION:
        if link["table_name"] == table["table_name"]:
            selected_link = link
            break

    # If the selected table doesn't contain any link the function terminates
    if selected_link == {}:
        return False

    # For each link a post request is made to the database, containing the link id and the id of the linked element to be
    # associated with the corresponding element in the table selected by the user
    for linked_field in selected_link["links"]:
        # Scans each element in the json file
        for i, record in enumerate(original_file):
            payload = {
                # The conversion to str is forced because a string is excepted by the API endpoint
                "Id" : str(
                    get_link_id(linked_field["linked_table_name"], record[linked_field["field_name"]])
                )
            }

            update = r.post(
                f"{BASE_URL}/tables/{table["id"]}/links/{linked_field["id"]}/records/{affected_records[i]['Id']}",
                headers=AUTH_HEADER,
                json=payload
            )

    return True

def main():
    """Main execution loop."""

    data_insertion_needed = True

    while data_insertion_needed:
        table = list_tables()

        file_path = input("\nWhat is the file full path? \n").strip()

        # Trying to open the file and to load its content
        try:
            with open(file_path, 'r') as file:
                original_data = json.load(file)

        except Exception:
            raise Exception(f"{file_path} could not be opened")

        # Call the push_json function to push the data in the selected table
        affected_records = push_json(table, original_data)

        print(f"\nSuccessfully inserted {len(affected_records)} records. Now attempting to link fields...\n")

        # Update the links, if there are any, as to as make the data import complete
        update_links(table, original_data, affected_records)

        data_insertion_needed = input("\nWould you like to upload another file? [y/N] ").lower() == "y"

if __name__ == "__main__":
    try:
        main()

    except (KeyboardInterrupt):
        print("\nExiting program.")
        exit(0)

    except Exception:
        print("\nSomething went wrong, restarting the program\n")
        main()