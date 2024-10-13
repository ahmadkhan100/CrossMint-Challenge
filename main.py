import requests
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def retry_operation(retry_attempts=3, wait_time=1):
    '''
    Decorator for retrying a function call if it fails.

    Parameters:
            retry_attempts (int): Number of retry attempts
            wait_time (int): Time to wait between retries in seconds
    '''

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(retry_attempts):
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    last_exception = e
                    logging.warning(
                        f"Retrying {func.__name__} due to error: {e}. Attempt {attempt + 1}/{retry_attempts}")
                    time.sleep(wait_time)
            raise last_exception

        return wrapper

    return decorator


@retry_operation()
def fetch_goal_map(candidate_identifier):
    '''
    Retrieves the goal map and converts it into a matrix format.

            Parameters:
                    candidate_identifier (str): Candidate ID string

            Returns:
                    goal_matrix (list): A list representing the goal map with celestial objects in the desired order
    '''
    api_endpoint = f"https://challenge.crossmint.io/api/map/{candidate_identifier}/goal"
    response = requests.get(api_endpoint)
    response.raise_for_status()
    goal_matrix = response.json()['goal']
    logging.info(f"Successfully retrieved goal map for candidate ID: {candidate_identifier}")
    return goal_matrix


# Refactored function to generalize object creation
@retry_operation()
def create_object(api_endpoint, payload, candidate_id, object_type):
    '''
    Sends a request to create a celestial object (polyanet, soloon, cometh).

            Parameters:
                    api_endpoint (str): API endpoint for the object creation
                    payload (dict): Payload containing required data
                    candidate_id (str): Candidate ID string
                    object_type (str): Type of the celestial object being created
    '''
    time.sleep(10)  # Delay for creating each object to avoid rate limiting issues
    headers = {"Content-Type": "application/json"}
    payload["candidateId"] = candidate_id
    response = requests.post(api_endpoint, data=json.dumps(payload), headers=headers)
    response.raise_for_status()
    logging.info(f"Successfully created {object_type} with details: {payload}")


@retry_operation()
def create_polyanet(candidate_identifier, row_idx, col_idx):
    '''
    Wrapper for creating a polyanet using the generalized create_object function.
    '''
    api_endpoint = "https://challenge.crossmint.io/api/polyanets"
    payload = {"row": row_idx, "column": col_idx}
    create_object(api_endpoint, payload, candidate_identifier, "polyanet")


@retry_operation()
def create_soloon(candidate_identifier, row_idx, col_idx, soloon_color):
    '''
    Wrapper for creating a soloon using the generalized create_object function.
    '''
    api_endpoint = "https://challenge.crossmint.io/api/soloons"
    payload = {"row": row_idx, "column": col_idx, "color": soloon_color}
    create_object(api_endpoint, payload, candidate_identifier, f"soloon ({soloon_color})")


@retry_operation()
def create_cometh(candidate_identifier, row_idx, col_idx, travel_direction):
    '''
    Wrapper for creating a cometh using the generalized create_object function.
    '''
    api_endpoint = "https://challenge.crossmint.io/api/comeths"
    payload = {"row": row_idx, "column": col_idx, "direction": travel_direction}
    create_object(api_endpoint, payload, candidate_identifier, f"cometh ({travel_direction})")


def handle_position(candidate_identifier, goal_matrix, row_idx, col_idx):
    '''
    Processes a specific cell in the goal matrix to create the corresponding celestial object.

            Parameters:
                    candidate_identifier (str): Candidate ID string
                    goal_matrix (list): The matrix containing the goal map
                    row_idx (int): Row index
                    col_idx (int): Column index
    '''
    # If the cell is not empty space, create the corresponding celestial object
    if goal_matrix[row_idx][col_idx] != "EMPTY":
        cell_value = goal_matrix[row_idx][col_idx].lower()

        if cell_value == "polyanet":
            create_polyanet(candidate_identifier, row_idx, col_idx)
        else:
            value_parts = cell_value.split("_")
            attribute = value_parts[0]
            object_type = value_parts[-1]

            if object_type == "soloon":
                create_soloon(candidate_identifier, row_idx, col_idx, attribute)
            elif object_type == "cometh":
                create_cometh(candidate_identifier, row_idx, col_idx, attribute)


def main():
    '''
    Main function to initiate the creation of celestial objects based on the goal map.
    '''
    # Candidate ID
    candidate_identifier = "91f84bce-dbd5-4ef1-a59f-9530ddcc316b"
    try:
        # Fetch the goal map and initiate creation of celestial objects
        goal_matrix = fetch_goal_map(candidate_identifier)
        logging.info("Beginning the creation of celestial objects in the Megaverse...")

        # Using ThreadPoolExecutor to handle object creation concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            for row_idx in range(len(goal_matrix)):
                for col_idx in range(len(goal_matrix[0])):
                    executor.submit(handle_position, candidate_identifier, goal_matrix, row_idx, col_idx)

        logging.info("Megaverse creation completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during Megaverse creation: {e}")


if __name__ == "__main__":
    main()
