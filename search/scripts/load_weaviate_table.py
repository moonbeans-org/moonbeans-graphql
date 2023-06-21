import json
import weaviate
import os

client = weaviate.Client(
    url="http://localhost:8080/",
    # auth_client_secret=weaviate.auth.AuthApiKey(api_key="<YOUR-WEAVIATE-API-KEY>"), # comment out this line if you are not using authentication for your Weaviate instance (i.e. for locally deployed instances)
    additional_headers={
        "X-OpenAI-Api-Key":"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"
        # "X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY")
    }
)

file_path = "beanies_test_data.json"

with open(file_path) as file:
    data = json.load(file)

# Clear up the schema, so that we can recreate it
client.schema.delete_all()
client.schema.get()

print("Initializing schema...")

# Define the Schema object to use `text-embedding-ada-002` on `title` and `content`, but skip it for `url`
beanies_schema = {
    "class": "Beanie",
    "description": "A collection of beanies",
    "vectorizer": "text2vec-openai",
    "moduleConfig": {
        "text2vec-openai": {
          "model": "ada",
          "modelVersion": "002",
          "type": "text"
        }
    },
    "properties": [{
        "name": "name",
        "description": "Name of the beanie",
        "dataType": ["text"]
    },
    {
        "name": "dna",
        "description": "unique identifier",
        "dataType": ["text"]
    },
    {
        "name": "attributes",
        "description": "attributes of our beanie friend",
        "dataType": ["text"],
    }]
}

# add the Article schema
client.schema.create_class(beanies_schema)

# get the schema to make sure it worked
print(client.schema.get())

print("Importing beanies")

counter=0

with client.batch as batch:
    for beanie in data:
        if (counter %10 == 0):
            print(f"Import {counter} / {len(data)} ")

        properties = {
            "name": beanie["name"],
            "dna": beanie["dna"],
            # "attributes": [f"{attribute['value']} - {attribute['trait_type']}" for attribute in beanie["attributes"]]
            "attributes": str(beanie["attributes"])
        }
        
        batch.add_data_object(properties, "Beanie")
        counter = counter+1

print("Importing Beanies complete!")       

result = (
    client.query.aggregate("Beanie")
    .with_fields("meta { count }")
    .do()
)
print("Object count: ", result["data"]["Aggregate"]["Beanie"], "\n")