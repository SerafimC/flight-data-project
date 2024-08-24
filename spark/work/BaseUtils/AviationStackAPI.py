import requests

class AviationstackAPI:
    def __init__(self, access_key: str):
        self.base_url = "http://api.aviationstack.com/v1/"
        self.access_key = access_key

    def get_all_airports(self):
        endpoint = "airports"
        limit = 100  # Maximum limit based on the API documentation
        offset = 0
        all_airports = []

        while True:
            params = {
                'access_key': self.access_key,
                'limit': limit,
                'offset': offset
            }
            response = requests.get(f"{self.base_url}{endpoint}", params=params)
            response_data = response.json()
            
            if 'data' in response_data:
                all_airports.extend(response_data['data'])
            else:
                break  # Exit if there's no data (possible API error)

            pagination = response_data.get('pagination', {})
            total = pagination.get('total', 0)
            count = pagination.get('count', 0)

            if offset + count >= total:
                break  # Exit if we've reached the end of the results
            
            offset += count  # Move to the next set of results

        return all_airports

# Example usage:
# api = AviationstackAPI(access_key="your_access_key_here")
# airports = api.get_all_airports()
# print(airports)
