# Import necessary libraries for environment variables, HTTP requests, JSON handling, and OS interaction.
from dotenv import load_dotenv
import requests as r
import json
import os

# Load environment variables from a .env file.
load_dotenv()

# Set up the authentication header and base URL for the NocoDB API using environment variables.
AUTH_HEADER = {"xc-token": os.getenv("DB_KEY")}
BASE_URL = f"http://{os.getenv('SERVER_URL')}/api/v2"
# Load the configuration for table links from an environment variable.
LINKS_DESCRIPTION = eval(os.getenv("LINKS_DESCRIPTION"))


def list_tables() -> dict[str, str]:
    """Connects to the NocoDB API to get all tables in the base,
    then prompts the user to select one from the list.

    Args:
        None

    Returns:
        dict[str, str]: A dictionary containing the metadata of the user-selected table, including its ID and name.
    """
    # Make a GET request to fetch all tables in the specified base.
    table_list = r.get(
        f"{BASE_URL}/meta/bases/{os.getenv('BASE_ID')}/tables",
        headers=AUTH_HEADER
    )

    # Check for a successful response, otherwise raise an exception.
    if table_list.status_code != 200:
        raise Exception(f"Table list failed with status code {table_list.status_code}\n{table_list.json()}")

    # Extract the list of tables from the JSON response.
    table_list = table_list.json()["list"]

    # Display the tables and prompt the user to choose one.
    print("What table do you want to import the data to?")
    for i, table_item in enumerate(table_list):
        print(f"{i + 1}) {table_item['title']}")

    # Return the dictionary of the selected table.
    return table_list[int(input("")) - 1]


def push_json(table: dict[str, str], data: list[dict[str, str]]) -> list[dict[str, str]]:
    """Takes a list of records and inserts them into a specified NocoDB table via a POST request.
    It cleans the data by removing any key-value pairs with empty values before sending.

    Args:
        table (dict[str, str]): A dictionary containing metadata for the target NocoDB table.
        data (list[dict[str, str]]): A list of dictionaries, where each dictionary represents a record to be created.

    Returns:
        list[dict[str, str]]: A list of dictionaries representing the records created by the API, including their new IDs.
    """
    # Create a new list of records, removing any key-value pairs where the value is empty.
    processed_data = [
        {k: v for k, v in item.items() if v}
        for item in data
    ]

    # Make a POST request to create new records in the specified table.
    request = r.post(
        f"{BASE_URL}/tables/{table['id']}/records",
        headers=AUTH_HEADER,
        json=processed_data
    )

    # Check for a successful response, otherwise raise an exception.
    if request.status_code != 200:
        raise Exception(f"Request failed with status code {request.status_code}\n{request.json()}")

    # Return the JSON response containing the newly created records.
    return request.json()


def get_link_id(search_table: str, field_name: str) -> int:
    """Finds the unique ID of a record in a specified table by searching for a match in its 'Title' column.
    This is a helper function used to retrieve foreign keys for linking records.

    Args:
        search_table (str): The `table_name` of the table to search within.
        field_name (str): The value to match in the 'Title' column of the `search_table`.

    Returns:
        int: The unique integer 'Id' of the record that was found.
    """
    table_id = ""
    requested_id = None

    # Get the list of all tables to find the ID of the `search_table`.
    table_list = r.get(
        f"{BASE_URL}/meta/bases/{os.getenv('BASE_ID')}/tables",
        headers=AUTH_HEADER
    )
    # Handle API errors.
    if table_list.status_code != 200:
        raise Exception(f"Table list failed with status code {table_list.status_code}\n{table_list.json()}")

    # Find the target table's ID from the list.
    for table in table_list.json()["list"]:
        if table["table_name"] == search_table:
            table_id = table["id"]
            break

    # If the table wasn't found, raise an exception.
    if table_id == "":
        raise Exception(f"Table not found")

    # Get all records from the target table to find the specific record ID.
    table_content = r.get(
        f"{BASE_URL}/tables/{table_id}/records",
        headers=AUTH_HEADER
    )
    # Handle API errors.
    if table_content.status_code != 200:
        raise Exception(f"Table data retrieve failed with status code {table_list.status_code}\n{table_list.json()}")

    # Search for the record where the 'Title' matches the `field_name` and get its ID.
    for field in table_content.json()["list"]:
        if field["Title"] in field_name:
            requested_id = field["Id"]
            break

    # Return the found ID.
    return requested_id


def update_links(table: dict[str, str], original_file: list[dict], affected_records: list[dict[str, str]]) -> bool:
    """Creates "link-to-another-record" relationships for the newly inserted rows.
    It uses the `LINKS_DESCRIPTION` environment variable to determine which fields need to be linked.

    Args:
        table (dict[str, str]): The metadata dictionary for the table that received the new records.
        original_file (list[dict]): The original data from the JSON file, used to look up values for linking.
        affected_records (list[dict[str, str]]): The list of newly created records returned by the API after insertion.

    Returns:
        bool: Returns True if the linking process was executed, and False if no link configuration was found for the specified table.
    """
    # This dictionary will store the link configuration for the selected table.
    selected_link = {}

    # Find the link configuration that matches the currently selected table.
    for link in LINKS_DESCRIPTION:
        if link["table_name"] == table["table_name"]:
            selected_link = link
            break

    # If no link configuration is found for this table, exit the function.
    if selected_link == {}:
        return False

    # Iterate through each link that needs to be created for this table.
    for linked_field in selected_link["links"]:
        # Iterate through each record from the original input file.
        for i, record in enumerate(original_file):
            # Prepare the payload for the API request. It needs the ID of the record in the other table.
            payload = {
                "Id": str(
                    # Use the helper function to get the ID from the linked table.
                    get_link_id(linked_field["linked_table_name"], record[linked_field["field_name"]])
                )
            }

            # Make a POST request to associate/link the records.
            update = r.post(
                f"{BASE_URL}/tables/{table['id']}/links/{linked_field['id']}/records/{affected_records[i]['Id']}",
                headers=AUTH_HEADER,
                json=payload
            )
    # Return True indicating the link update process was attempted.
    return True


def main():
    """Serves as the main execution loop for the script. It guides the user through selecting a table,
    providing a file path, inserting the data, updating linked fields, and then asks if they wish to repeat the process.

    Args:
        None

    Returns:
        None
    """
    # Controls whether the main loop should continue.
    data_insertion_needed = True

    while data_insertion_needed:
        # Let the user select a table to import data into.
        table = list_tables()

        # Get the full path of the JSON file from the user.
        file_path = input("\nWhat is the file full path? \n").strip()

        # Try to open and load the JSON data from the specified file.
        try:
            with open(file_path, 'r') as file:
                original_data = json.load(file)
        except Exception:
            raise Exception(f"{file_path} could not be opened")

        # Push the data from the file to the selected NocoDB table.
        affected_records = push_json(table, original_data)

        print(f"\nSuccessfully inserted {len(affected_records)} records. Now attempting to link fields...\n")

        # After inserting, update any linked records as defined in the .env configuration.
        update_links(table, original_data, affected_records)

        # Ask the user if they want to upload another file.
        data_insertion_needed = input("\nWould you like to upload another file? [y/N] ").lower() == "y"


# Standard Python entry point.
if __name__ == "__main__":
    try:
        # Run the main function.
        main()
    except (KeyboardInterrupt):
        # Handle graceful exit on Ctrl+C.
        print("\nExiting program.")
        exit(0)
    except Exception:
        # Catch any other exceptions and restart the main function.
        print("\nSomething went wrong, restarting the program\n")
        main()