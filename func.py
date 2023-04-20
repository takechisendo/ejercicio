# Copyright (c) 2016, 2018, Oracle and/or its affiliates.  All rights reserved.
import io
import os
import json
import sys
import oci
import logging
from fdk import response
from database import (get_connection, load_data)
from object_storage import get_object

def handler(ctx, data: io.BytesIO = None):
    # authentication based on instance principal
    logging.getLogger().info('signer request')  
    signer = oci.auth.signers.get_resource_principals_signer()
    
    try:        
        body = json.loads(data.getvalue())
        logging.getLogger().info('request body: {0}'.format(str(body)))        
    except Exception as e:
        error = 'Input a JSON object in the format: ' + e
        logging.getLogger().error('ERROR: ' + error)
        raise Exception(error)

    resp = do(signer, body)

    return response.Response(
        ctx, response_data=json.dumps(resp),
        headers={"Content-Type": "application/json"}
    )

def do(signer, body):
    try:
        fileName = body["data"]["resourceName"]
        namespace = body["data"]["additionalDetails"]["namespace"]
        bucketName = body["data"]["additionalDetails"]["bucketName"]

        if fileName.lower().find("csv") > 0:               
            companyName = os.getenv('COMPANY')
            topicId = os.getenv('TOPIC_OCID')

            logging.getLogger().info("File object received")

            input_csv_text = get_object(signer, namespace, bucketName, fileName)

            dbconnection = get_connection()

            logging.getLogger().info("Successfully retrieved connection")
       
            load_data(input_csv_text, dbconnection)
            """
            messageDetails = oci.ons.models.MessageDetails(
                body="Load process complete", title="Racing to the cloud :: " + companyName)

            ons_client = oci.ons.NotificationDataPlaneClient(config={}, signer=signer)
            ons_client.publish_message(topicId,messageDetails) 

            logging.getLogger().info("Load process complete")
            """
 
        else:
            raise SystemExit("File extension is not supported")    
            
    except Exception as e:
        logging.getLogger().error("Failed:" + str(e))
        raise SystemExit(str(e))
        
    response = {
        "content": "OK"
    }
    return response