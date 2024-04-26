import requests
import os

from dotenv import load_dotenv
from .colors import bcolors
from datetime import datetime
from .tables import tables

load_dotenv()



def api(client, bigquery):

    try:
        
        cliendResponse = requestData('metadata/client')

        if(code(cliendResponse) == 200):

            customerID = getValues(cliendResponse, True)
            customerProfileResponse = requestData(f'{customerID}/metadata/customer')

            if(code(customerProfileResponse) == 200):

                customerProfileList = getValues(customerProfileResponse)

                customerProfileID = customerProfileList[0]['customer_profile_id']
                customerProfileNetworkType = customerProfileList[0]['network_type'].replace("fb_", "").replace("_account", "").lower()
                customerProfileNativeID = customerProfileList[0]['native_id']
                customerProfileName = customerProfileList[0]['name'].replace(" ", "-").lower()

                # Obtenemos la fecha del registro
                newDate = datetime.now()
                formatDate = newDate.strftime("%Y-%m-%d")

               
                table_name = tables(client, bigquery, customerProfileName, formatDate, customerProfileNativeID, customerProfileNetworkType)

                print(table_name)

                # Obtenemos todos los id de los perfiles y tambien a que red pertenecen
                #for customProfile in customerProfileList:

                    #customerProfileID = customProfile['customer_profile_id']
                    #customerProfileNetworkType = customProfile['network_type']
                    #customerProfileName = customProfile['name']



                    #print(bcolors.OKGREEN + f'{customerProfileNetworkType}' + bcolors.ENDC)



            else:

                print(bcolors.FAIL + f'Error: {error(customerProfileResponse)}' + bcolors.ENDC)


        else:
            print(bcolors.FAIL + f'Error: {error(cliendResponse)}' + bcolors.ENDC)

    
    except Exception as e: 
        print(bcolors.FAIL + f'Error: {e}' + bcolors.ENDC)




def code(response):
    return int(response['code'])




def error(response):
    return response['message']


def getValues(response, only = False):

    if(only):
        return response['response'].get('data', [{}])[0].get('customer_id')

    return response['response'].get('data', [{}])
    




def requestData(other):

    api_url = 'https://api.sproutsocial.com/v1'
    bearer_token = os.getenv('SPROUTSOCIAL')

    headers = {
        'Authorization': f'Bearer {bearer_token}'
    }

    response = requests.get(f"{api_url}/{other}", headers=headers)
    code = response.status_code

    if code == 200:

        return {
            'code': code,
            'response': response.json()
        }
    
    return response.json()