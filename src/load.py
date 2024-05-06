import json
from os import getenv
from dotenv import find_dotenv, load_dotenv
import boto3
import requests
load_dotenv(find_dotenv())

# Blaze API
BLAZE_DATA_ENDPOINT= getenv('BLAZE_DATA_ENDPOINT')
BLAZE_DATABASE_ID = getenv('BLAZE_DATABASE_ID')
BLAZE_TOKEN = getenv('BLAZE_TOKEN')
BLAZE_TABLE_ID = getenv('BLAZE_TABLE_ID')

def setDataJson(data):
    if 'name' not in data['values']:
        return 
        #data['values']['name'] = [{'data': ''}]
    if 'disambiguatingDescription' not in data['values']:
        data['values']['disambiguatingDescription'] = [{'data': ''}]
    if 'description' not in data['values']:
        data['values']['description'] = [{'data': ''}]
    if 'legalName' not in data['values']:
        data['values']['legalName'] = [{'data': ''}]
    if 'streetAddress' not in data['values']:
        data['values']['streetAddress'] = [{'data': ''}]
    if 'addressLocality' not in data['values']:
        data['values']['addressLocality'] = [{'data': ''}]
    if 'postalCode' not in data['values']:
        data['values']['postalCode'] = [{'data': ''}]
    if 'givenName' not in data['values']:
        data['values']['givenName'] = [{'data': ''}]
    if 'familyName' not in data['values']:
        data['values']['familyName'] = [{'data': ''}]
    if 'email' not in data['values']:
        data['values']['email'] = [{'data': ''}]
    if 'telephone' not in data['values']:
        data['values']['telephone'] = [{'data': ''}]
    if 'url' not in data['values']:
        data['values']['url'] = [{'data': ''}]
    if 'image' not in data['values']:
        data['values']['image'] = [{'data': ''}]

    json_data = {
        "sku": data['identifier'],
        "name": data['values']['name'][0]['data'],
        "disambiguatingDescription": data['values']['disambiguatingDescription'][0]['data'],
        "description": data['values']['description'][0]['data'],
        "legalName": data['values']['legalName'][0]['data'],
        "streetAddress": data['values']['streetAddress'][0]['data'],
        "addressLocality": data['values']['addressLocality'][0]['data'],
        "postalCode": data['values']['postalCode'][0]['data'],
        "givenName": data['values']['givenName'][0]['data'],
        "familyName": data['values']['familyName'][0]['data'],
        "email": data['values']['email'][0]['data'],
        "telephone": data['values']['telephone'][0]['data'],
        "url": data['values']['url'][0]['data'],
        "image": data['values']['image'][0]['data']
    }

    return json_data

# Find Raw 
def findRaw(data):
    url = f"{BLAZE_DATA_ENDPOINT}/api/database/{BLAZE_TABLE_ID}/query/"
    sku = data['identifier']
    response = requests.post(
        url,
        headers={
            "Authorization": f"Token {BLAZE_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "query": "UPDATE pimTestData SET sku="+data['identifier']+", name="+data['values']['name'][0]['data']+" WHERE sku = " + sku
        }
    )
    print(response)
    print(response.json())
    return response['results']

# Update Row in Blaze Table
def updateRow(data):
    url = f"{BLAZE_DATA_ENDPOINT}/api/database/rows/table/{BLAZE_TABLE_ID}/{data['id']}/?user_field_names=true"
    json_data = setDataJson(data)
    repsonse = requests.patch(
        url,
        headers={
            "Authorization": "Token YOUR_SPACE_TOKEN",
            "Content-Type": "application/json"
        },
        json=json_data
    )
    return repsonse.json()

# Create Row in Blaze Table
def createRow(data):
    url = f"{BLAZE_DATA_ENDPOINT}/api/database/rows/table/{BLAZE_TABLE_ID}/?user_field_names=true"
    json_data = setDataJson(data)

    request = requests.post(
        url,
        headers={
            "Authorization": f"Token {BLAZE_TOKEN}",
            "Content-Type": "application/json"
        },
        json=json_data
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
    dataList += data['results']
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
    url = f"{BLAZE_DATA_ENDPOINT}/api/database/rows/table/{BLAZE_TABLE_ID}/?user_field_names=true"
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
    url = f"{BLAZE_DATA_ENDPOINT}/api/database/rows/table/{BLAZE_TABLE_ID}/{rowId}/"
    requests.delete(
        url,
        headers={
            "Authorization": f"Token {BLAZE_TOKEN}"
        }
    )

def load(data):
    # Clear all rows in Blaze Table
    #deleteAllRows()
    # Load all rows in Blaze Table
    for product in data:
        id = findRaw(product)
        if id:
            print(f"Row already exists: {id}")
            response = updateRow(product)
        else:
            response = createRow(product)
        print(response)