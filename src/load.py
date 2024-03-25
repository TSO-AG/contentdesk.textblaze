import json
from os import getenv
from dotenv import find_dotenv, load_dotenv
import boto3
import requests
load_dotenv(find_dotenv())

BLAZE_DATABASE_ID = getenv('BLAZE_DATABASE_ID')
BLAZE_TOKEN = getenv('BLAZE_TOKEN')
BLAZE_TABLE_ID = getenv('BLAZE_TABLE_ID')

# Create Row in Blaze Table
def createRow(data):
    url = f"https://data-api.blaze.today/api/database/rows/table/{BLAZE_TABLE_ID}/?user_field_names=true"

    if 'disambiguatingDescription' not in data['values']:
        data['values']['disambiguatingDescription'] = [{'data': ''}]
    if 'description' not in data['values']:
        data['values']['description'] = [{'data': ''}]
    if 'name' not in data['values']:
        return 
        #data['values']['name'] = [{'data': ''}]

    request = requests.post(
        url,
        headers={
            "Authorization": f"Token {BLAZE_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "sku": data['identifier'],
            "name": data['values']['name'][0]['data'],
            "disambiguatingDescription": data['values']['disambiguatingDescription'][0]['data'],
            "description": data['values']['description'][0]['data']
        }
    )
    return request.json()

def nextPage(data):
    url = data['next']
    request = requests.get(
        url,
        headers={
            "Authorization": f"Token {BLAZE_TOKEN}",
        }
    )
    return request.json()

def whileLoopListofRow(data):
    dataList = []
    nextpage = True
    print("While loop to get all rows")
    nextLink = data['next']
    dataList += nextPage(data)['results']
    nextData = data
    while nextpage:
        print("Next page True")
        print(nextLink)
        if nextLink:
            #print(data['results'])
            nextData = nextPage(nextData)
            dataList += nextData['results']
            if 'next' not in nextData:
                nextpage = False
            else:
                nextLink = nextData['next']
        else:
            nextpage = False
    return dataList

def getListofRow():
    print("Getting all rows")
    url = f"https://data-api.blaze.today/api/database/rows/table/{BLAZE_TABLE_ID}/?user_field_names=true"
    request = requests.get(
        url,
        headers={
            "Authorization": f"Token {BLAZE_TOKEN}",
        }
    )
    # Add all rows to a list
    respons = request.json()
    print("Start While loop to get all rows")
    data = whileLoopListofRow(respons)
    print(data)
    return data

# delete all rows in Blaze Table
def deleteAllRows():
    print("Start: Task to delete all rows in Blaze Table")
    rawList = getListofRow()
    print("Start: For loop to delete all rows in Blaze Table")
    for row in rawList:
        deleteRow(row['id'])

def deleteRow(rowId):
    url = f"https://data-api.blaze.today/api/database/rows/table/{BLAZE_TABLE_ID}/{rowId}/"
    requests.delete(
        url,
        headers={
            "Authorization": f"Token {BLAZE_TOKEN}"
        }
    )

def load(data):
    # Clear all rows in Blaze Table
    deleteAllRows()
    # Load all rows in Blaze Table
    for product in data:
        response = createRow(product)
        print(response)