import io
import os
import csv
import json
import logging
import string

import borneo
from borneo import (
    AuthorizationProvider, DeleteRequest, GetRequest,
    IllegalArgumentException, NoSQLHandle, NoSQLHandleConfig, PutRequest,
    QueryRequest, Regions, TableLimits, TableRequest)
from borneo.iam import SignatureProvider
from borneo.kv import StoreAccessTokenProvider


def get_connection():          
    provider = SignatureProvider.create_with_resource_principal()
    compartment_id = provider.get_resource_principal_claim(borneo.ResourcePrincipalClaimKeys.COMPARTMENT_ID_CLAIM_KEY)

    config = NoSQLHandleConfig(os.getenv('NOSQL_REGION'), provider).set_logger(None).set_default_compartment(compartment_id)
    return NoSQLHandle(config)

                

def load_data(input_csv_text, dbconnection):
    try:
        csvReader = csv.DictReader(input_csv_text.split('\n'), delimiter=',')

        request = PutRequest().set_table_name(os.getenv('TABLE_NAME'))
        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            jsonString = json.dumps(row)
        
            logging.getLogger().info(jsonString)

            request.set_value_from_json(jsonString)
            dbconnection.put(request)
        
        logging.getLogger().info("Upload complete")
    except Exception as e:
        logging.getLogger().error("Failed:" + str(e))
        