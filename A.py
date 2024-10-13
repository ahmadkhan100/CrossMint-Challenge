import requests
import logging
import time
from typing import List, Tuple

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MegaverseAPI:
    """Class to interact with the Megaverse API."""

    BASE_URL = "https://challenge.crossmint.io/api"

    def __init__(self, candidate_id: str):
        """
        Initialize the MegaverseAPI.

        :param candidate_id: The candidate ID for API authentication
        """
        self.candidate_id = candidate_id

    def create_polyanet(self, row: int, column: int) -> bool:
        """
        Create a POLYanet at the specified position.

        :param row: The row position
        :param column: The column position
        :return: True if creation was successful, False otherwise
        """
        url = f"{self.BASE_URL}/polyanets"
        data = {
            "candidateId": self.candidate_id,
            "row": row,
            "column": column
        }
        max_retries = 5
        retry_delay = 2  # Initial delay in seconds for error handling efficiently

        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=data)
                response.raise_for_status()
                logging.info(f"Created POLYanet at ({row}, {column})")
                return True
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    logging.warning(f"Rate limit reached. Retrying attempt {attempt + 1}/{max_retries} after {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logging.error(f"Failed to create POLYanet at ({row}, {column}): {e}")
                    return False
            except requests.RequestException as e:
                logging.error(f"Failed to create POLYanet at ({row}, {column}): {e}")
                return False

        logging.error(f"Failed to create POLYanet at ({row}, {column}) after {max_retries} attempts due to rate limiting.")
        return False

class MegaverseCreator:
    """Class to manage the creation of the Megaverse."""

    def __init__(self, api: MegaverseAPI):
        """
        Initialize the MegaverseCreator.

        :param api: An instance of MegaverseAPI
        """
        self.api = api

    def generate_polyanet_positions(self, size: int = 11) -> List[Tuple[int, int]]:
        """
        Generate the specific positions for POLYanets in the desired pattern.

        :param size: The size of the grid (default is 11)
        :return: List of coordinate tuples for the POLYanets
        """
        positions = []
        for i in range(size):
            if i == 2 or i == 8:
                positions.append((i, i))
                positions.append((i, size - 1 - i))
            elif i == 3 or i == 7:
                positions.append((i, i))
                positions.append((i, size - 1 - i))
            elif i == 4 or i == 6:
                positions.append((i, i))
                positions.append((i, size - 1 - i))
            elif i == 5:
                positions.append((i, i))
        return positions

    def create_polyanet_cross(self, size: int = 11) -> None:
        """
        Create POLYanets in the specific pattern.

        :param size: The size of the grid (default is 11)
        """
        positions = self.generate_polyanet_positions(size)
        for row, col in positions:
            self.api.create_polyanet(row, col)

def main():
    """Main function to execute the Megaverse creation."""
    candidate_id = "91f84bce-dbd5-4ef1-a59f-9530ddcc316b"
    api = MegaverseAPI(candidate_id)
    creator = MegaverseCreator(api)

    # Creating the POLYanets in the desired pattern
    creator.create_polyanet_cross()

    logging.info("Megaverse creation completed.")

if __name__ == "__main__":
    main()
