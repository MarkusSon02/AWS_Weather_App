import requests
import functions

url = "https://api.weatherstack.com/historical?access_key=d19781abe3457032c492f516530ec81d"

start = '2025-01-01'
end = '2025-05-29'

date_list = functions.generate_date_list(start, end)
print(len(date_list))
print(date_list)
print(date_list[0])
print(date_list[-1])

# for date in date_list:
#     print(date)
#     querystring = {"query":"Edmonton", "historical_date":date}

#     response = requests.get(url, params=querystring)

#     data = response.json()

#     print(data)

#     par_locations = functions.partition_data(data)
#     print(par_locations)

#     par_dates = functions.partition_date(date)
#     print(par_dates)

#     params = par_locations + par_dates

#     bucket = "weather-api-data-son"

#     if functions.is_dict(data):
#         functions.upload_partitioned_weather(data, params, bucket)
#     else:
#         print("Data is not in json format")




