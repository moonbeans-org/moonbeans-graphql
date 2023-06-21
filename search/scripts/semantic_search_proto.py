import weaviate
import os

def query_weaviate(query, collection_name):
    nearText = {
        "concepts": [query],
        "distance": 0.7,
    }

    properties = [
        "name", "attributes",
        "_additional {certainty distance}"
    ]

    result = (
        client.query
        .get(collection_name, properties)
        .with_near_text(nearText)
        .with_limit(10)
        .do()
    )
    
    # Check for errors
    if ("errors" in result):
        print ("\033[91mYou probably have run out of OpenAI API calls for the current minute – the limit is set at 60 per minute.")
        raise Exception(result["errors"][0]['message'])
    
    return result["data"]["Get"][collection_name]


# Connect to your Weaviate instance
client = weaviate.Client(
    url="http://localhost:8080/",
    # auth_client_secret=weaviate.auth.AuthApiKey(api_key="<YOUR-WEAVIATE-API-KEY>"), # comment out this line if you are not using authentication for your Weaviate instance (i.e. for locally deployed instances)
    additional_headers={
        "X-OpenAI-Api-Key":"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"
    }
)

# Check if your instance is live and ready
# This should return `True`
client.is_ready()

# Get the string input from the user
input_string = input("Search for some beanies: ")

query_result = query_weaviate(input_string, "Beanie")

for i, article in enumerate(query_result):
    print(f"{i+1}. { article['name']} (Score: {round(article['_additional']['certainty'],3) })")
