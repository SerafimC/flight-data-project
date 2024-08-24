import requests

# https://rapidapi.com/aedbx-aedbx/api/aerodatabox/playground/apiendpoint_40271c13-cd08-40be-954d-3290219fb71d
class TripAdvisorAPI:
    def __init__(self, access_key: str):
        self.base_url = "https://tripadvisor16.p.rapidapi.com/api/v1/"
        self.access_key = access_key

    def get_airports_by_location(self, query: str):
        endpoint = "flights/searchAirport"

        url = self.base_url + endpoint
        
        querystring = {"query": query}

        headers = {
        	"x-rapidapi-key": self.access_key,
        	"x-rapidapi-host": "tripadvisor16.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)
                
        if response.status_code == 403:
            raise Exception("403 Forbidden")

        # print(response.status_code)
        # print(response.json())

        all_airports = response.json()

        return all_airports
