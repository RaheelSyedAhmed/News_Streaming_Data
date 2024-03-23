import json, csv
import string


from newsapi import NewsApiClient
# Use NewsApiClient to get all articles about apple 
# from the oldest time our plan allows to the newest time our plan allows

# Code used is commented out to prevent too many API calls.

# If you want to generate this dataset yourself, feel free to get an API key
newsapi = NewsApiClient(api_key='YOUR_API_KEY_HERE')

# Dump all information into a json file

# Note that our search term is "apple".
all_articles = newsapi.get_everything(q='apple', language='en')

# Write out all article data to a file.
json_object = json.dumps(all_articles, indent=4)
with open("all_apple_articles.json", 'w') as f:
    f.write(json_object)


# Read the article JSON data
with open('all_apple_articles.json') as json_file:
    data = json.load(json_file)

# Specify important fields
wanted_fields = ['author', 'title', 'description', 'publishedAt', 'content']

# Make individual csv files to represent "new, incoming" article data.
counter = 1
for row in data['articles']:
    with open('articles/apple_info_{}.csv'.format(counter), 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(wanted_fields)
        writer.writerow([row[wf] for wf in wanted_fields])
    counter += 1



