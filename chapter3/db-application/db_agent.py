#!/usr/bin/python -W ignore
########################################################## {COPYRIGHT-TOP} ###
# Licensed Materials - Property of IBM
# 5737-I32
#
# (C) Copyright IBM Corp. 2019
#
# US Government Users Restricted Rights - Use, duplication, or
# disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
########################################################## {COPYRIGHT-END} ###

from ibm_spectrum_discover_application_sdk.ApplicationMessageBase import ApplicationMessageBase, ApplicationReplyMessage
from ibm_spectrum_discover_application_sdk.ApplicationLib import ApplicationBase
from ibm_spectrum_discover_application_sdk.DocumentRetrievalBase import DocumentKey, DocumentRetrievalFactory

import os
import paramiko
import logging
import sys
import json
import csv
import requests
from urllib.parse import urljoin 

#urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

ENCODING = 'utf-8'

SD_HOST = ""
SD_USER = ""
SD_PASSWORD = ""
SD_TOKEN = ""

def load_db(csvdb) :
    """Load the CSV database"""
    def client_dict(client):
        return {
            "name":         client[0],
            "blood_group":  client[1],
            "email":        client[2],
            "age":          client[3],
            "sex":          client[4],
            "smoker":       client[5]
        }
    data={}
    with open(csvdb, "r") as db:
        data = {client[0]: client_dict(client[1:]) for client in csv.reader(db)}
    return data

def get_token():
    """Get an API Token using the provided credentials"""
    global SD_HOST
    global SD_USER
    global SD_PASSWORD
    headers = {}
    basic_auth = requests.auth.HTTPBasicAuth(SD_USER, SD_PASSWORD)
    identity_auth_url=urljoin(SD_HOST,"/auth/v1/token")
    logger.debug("Getting a new token for %s:%s on %s" % (SD_USER,SD_PASSWORD,SD_HOST))
    try:
        response = requests.get(identity_auth_url, verify=False, headers=headers, auth=basic_auth)
        # check response from identity auth server
        if response.status_code == 200:
            application_token = response.headers['X-Auth-Token']
            return application_token

        raise Exception("Attempt to obtain token returned (%d)" % response.status_code)
    except Exception as exc:
            logger.error('Application failed to obtain token (%s)', str(exc))
            raise
    return

def get_fkey_metadata(fkey):
    """Get a file (identified through its fkey) metadata, querying Discover"""
    global SD_TOKEN
    global SD_HOST
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer %s' % SD_TOKEN if SD_TOKEN else ""
    }
    json_query = {
        "query":"fkey = '%s'" % fkey,
        "filters":[],
        "group_by":[],
        "sort_by":[],
        "limit":3
    }
    query_url=urljoin(SD_HOST,"/db2whrest/v1/search")
    try:
        response = requests.post(url=query_url, verify=False, json=json_query, headers=headers, auth=None)
        #API token may be expired, renew it and retry the query
        if response.status_code == 401:
            logger.debug("A new token is required")
            SD_TOKEN = get_token()
            logger.debug("New token is %s"%SD_TOKEN)
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer %s' % SD_TOKEN
            }
            response = requests.post(url=query_url, verify=False, json=json_query, headers=headers, auth=None)

        if response.status_code == 200:
            result = response.json()
            metadata = json.loads(result["rows"])
            if metadata:
                return metadata[0]
            return None
        raise Exception("Metadata failed with code %d" % response.status_code)
    except Exception as exc:
        logger.error('Application failed to obtain token (%s)', str(exc))
        raise

if __name__ == '__main__':
    print("---------------------------------------------------------------------")
    # Instantiate logger
    loglevels = {'INFO': logging.INFO, 'DEBUG': logging.DEBUG,
                 'ERROR': logging.ERROR, 'WARNING': logging.WARNING}
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(stream=sys.stdout,
                        format=log_format,
                        level=loglevels[log_level])
    logger = logging.getLogger(__name__)

    registration_info = {
        "action_id": "DEEPINSPECT",
        "action_params": ["extract_tags"]
    }

    # Create application instance
    application = ApplicationBase(registration_info)
    # start function performs all required initializations and connections
    application.start()
    
    # Get a message handler (abstraction of Kafka)
    am = ApplicationMessageBase(application)
    # Need to create a document retrieval handler for each unique datasource we
    # receive in the work message, create them dynamically and store them.
    # May be persisted to re-use over multiple work messages.
    drh = {}

    SD_HOST = os.getenv("SPECTRUM_DISCOVER_HOST","https://localhost")
    SD_PASSWORD = os.getenv("APPLICATION_USER_PASSWORD")
    SD_USER = os.getenv("APPLICATION_USER")
    valid_username_password = True
    if not SD_USER:
        logger.error("APPLICATION_USER is not set via an environment variable.")
        valid_username_password = False
    if not SD_PASSWORD:
        logger.error("APPLICATION_USER_PASSWORD is not set via an environment variable.")
        valid_username_password = False
    if not valid_username_password:
        raise SystemExit("Missing APPLICATION_USER and or APPLICATION_USER_PASSWORD environment variable.")

    logger.debug("Host : %s" % SD_HOST)
    logger.debug("User : %s" % SD_USER)
    logger.debug("Password : %s" % SD_PASSWORD)

    #Load the CSV database.
    #In our example, to keep it simple, the database is embedded in the container
    db = load_db("/application/client_database.csv")

    # message processing loop
    logger.info("Looking for job")
    while True:
        logger.info("Trying to read messages")
        msg = am.read_message(timeout=100)  # or timeout can be zero to poll

        if msg:
            # Application can choose to parse message, or as below use provided parse function
            work = am.parse_work_message(msg)

            # similar with reply message, can construct manually, or use helpers as below
            reply = ApplicationReplyMessage(msg)

            for docs in work['docs']:
                # DocumentKey is a unique identifier for a document, amalgam of connection + name
                key = DocumentKey(docs)

                logger.info('PID:{} Inspecting Document:{}'.format(os.getpid(), key.path))

                ##################################################
                ################ Start Custom Code ###############
                ##################################################
    
                tags = {}
                
                blood_group = ""
                email = ""
                smoker = ""
                status='failed'

                #Get file metadata from Discover
                metadata = get_fkey_metadata(docs["fkey"])
                if metadata:
                    #Get the additional metadata from the database. 
                    #metadata["dicom_pid"] contains the Social Security number of the patient
                    db_line = db[metadata["dicom_pid"]]
                    if db_line:
                        blood_group=db_line["blood_group"]
                        email=db_line["email"]
                        smoker=db_line["smoker"]
                        
                        logger.debug('... blood group : %s' % blood_group)
                        logger.debug('... email : %s' % email)
                        logger.debug('... smoker : %s' % smoker)
                        
                        tags_to_extract = work['action_params']['extract_tags']

                        #Assign value to the corresponding tag
                        #for example, the blood_group value will be assign to any tag containing the string "blood_group"#
                        #for example, the name of the tag could be dicom_blood_group
                        for tag in tags_to_extract:
                            if "blood_group" in tag:
                                value=blood_group
                            elif "email" in tag:
                                value=email
                            elif "smoker" in tag:
                                value=smoker
                            else:
                                value=""
                            logger.info("Set %s to %s"%(tag,value))
                            tags[tag]=value
                        status='success'
                    
                ##################################################
                ################# End Custom Code ################
                ##################################################
                logger.debug(tags)
                reply.add_result(status, key, tags)
            
            # Finally, send our constructed reply
            logger.info("Sending result to Discover")
            am.send_reply(reply)
        else:
            # timeout
            logger.info("Poll timeout reached - passing")
            pass
