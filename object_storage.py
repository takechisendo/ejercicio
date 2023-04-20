import os
import logging

import oci.object_storage

def get_object(signer, namespace, bucketName, fileName):
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    message = "Failed: The object " + str(fileName) + " could not be retrieved."

    try:

        logging.getLogger().info("Searching for bucket and object ")
        
        
        csvdata = client.get_object(namespace, bucketName, fileName)

        logging.getLogger().info("found objec")

        if csvdata.status == 200:            
            message = "Success: The object " + str(fileName) + " was retrieved" 
            logging.getLogger().info(message)
        else:
            raise Exception("cannot rename object {0}".format(fileName))         

    except Exception as e:
        message = "Failed: " + str(e.message)
        raise Exception("cannot rename object {0}".format(message))
    
    input_csv_text = str(csvdata.data.text)

    return input_csv_text