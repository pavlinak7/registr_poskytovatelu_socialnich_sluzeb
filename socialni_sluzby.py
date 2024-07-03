import json
from pymongo import MongoClient
import pandas as pd
import subprocess
import os
pd.set_option("display.max_colwidth",500)

dir1 = "input"
dir2 = "output"

if not os.path.exists(dir1):
    os.mkdir(dir1)
    
if not os.path.exists(dir2):
    os.mkdir(dir2)

##########################################################################################
def save_json(adresa, nazev):
    print("Downloading started")
    result = subprocess.run(['wget', '-O', nazev, adresa],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) #suppressing output

    if result.returncode == 0:
        print("File has been downloaded. \n")
    else:
        print("Download failed. \n")


    
def modify_json(input_filepath: str, output_filepath: str):
    """
    Creates a new "document" for each item in the "polozky" array
    """
    
    print("Modifying has begun")
    
    with open(input_filepath, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    items = data[list(data.keys())[0]] # Extract the items in the "polozky" array
    
    documents = []
    for item in items:
        # Create a new document for each item in the "polozky" array
        new_doc = {"_id": item['portalId'], **item}
        documents.append(new_doc)
    
    # Save the new documents into a single JSON file
    with open(output_filepath, 'w', encoding='utf-8') as outfile:
        json.dump(documents, outfile, ensure_ascii=False, indent=4)
    
    print("Modified file has been saved. \n")
    

def import_data_to_mongodb(json_filepath: str, db_name: str, collection_name: str, host: str = 'localhost', port: int = 27017):
    """
    Import data from a JSON file into a MongoDB collection.
    """
    print("Importing has begun")
    
    client = MongoClient(host, port) # Connect to MongoDB
    
    # Create database and collection
    db = client[db_name]
    collection = db[collection_name]
    
    # Load the transformed JSON data
    with open(json_filepath, 'r', encoding='utf-8') as file:
        transformed_data = json.load(file)
    
    # Insert data into the MongoDB collection
    collection.insert_many(transformed_data)
    
    print("Data has been imported to MongoDB. \n")


def query_mongodb_to_dataframe(db_name: str, collection_name: str, query: dict, projection: dict, limit: int, host: str = 'localhost', port: int = 27017):
    """
    Query a MongoDB collection and convert the results to a pandas DataFrame, flattening the 'poskytovatel' field.

    query: MongoDB query to filter the data.
    projection: MongoDB projection to specify the fields to include or exclude.
    limit: Number of documents to retrieve.
    """
    
    print("Querying has begun")
    # Connect to the MongoDB server
    client = MongoClient(f'mongodb://{host}:{port}/')
    
    # Select the database and collection
    db = client[db_name]
    collection = db[collection_name]
    
    # Perform the query
    results = collection.find(query, projection).limit(limit)
    
    # Convert the results to a list
    results_list = list(results)
    
    # Normalize the 'poskytovatel' field
    for result in results_list:
        if 'poskytovatel' in result and isinstance(result['poskytovatel'], dict):
            result['poskytovatel'] = result['poskytovatel'].get('nazev', None)
    
    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(results_list)
    print("Dataframe was created. \n")
    print()
    
    return df


################################################################################################
url = "https://data.mpsv.cz/od/soubory/rpss/rpss.json"
input_file = 'input/rpss.json'
output_file = 'output/rpss_upravene.json'
database = 'socialni_sluzby'
kolekce = 'poskytovatele'
dotaz = {'identifikator': 1, 'poskytovatel.nazev': 1} 
pocet_radku = 5

save_json(url, input_file)
modify_json(input_file, output_file)
import_data_to_mongodb(output_file, database, kolekce)
df = query_mongodb_to_dataframe(database, kolekce, {}, dotaz, pocet_radku)
print(df)
