import fattureincloud_python_sdk
from fattureincloud_python_sdk.rest import ApiException
from pprint import pprint
import requests
import os
from dotenv import load_dotenv
load_dotenv()
# Defining the host is optional and defaults to https://api-v2.fattureincloud.it
# See configuration.py for a list of all supported configuration parameters.
configuration = fattureincloud_python_sdk.Configuration(
    host = "https://api-v2.fattureincloud.it"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]


# Enter a context with an instance of the API client
with fattureincloud_python_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = fattureincloud_python_sdk.ClientsApi(api_client)
    company_id = 309951 # int | The ID of the company.
    page = 28
    fields = 'ei_code'
#    fields = 'fields_example' # str | List of comma-separated fields. (optional)
    fieldset = 'detailed' # str | Name of the fieldset. (optional)
    sort = 'entity_ei_code' # str | List of comma-separated fields for result sorting (minus for desc sorting). (optional)
    #page = 1 # int | The page to retrieve. (optional) (default to 1)
    per_page = 10 # int | The size of the page. (optional) (default to 5)
    #q = 'q_example' # str | Query for filtering the results. (optional)
   # q = 'entity_name="ZUCCOTTI ASSICURAZIONI SRL"'

    try:
        # List Clients
        api_response = api_instance.list_clients(company_id, per_page=per_page, page=page,sort=sort,fieldset=fieldset,fields=fields)
        print(api_response.data)
    except Exception as e:
        print("Exception when calling ClientsApi->list_clients: %s\n" % e)