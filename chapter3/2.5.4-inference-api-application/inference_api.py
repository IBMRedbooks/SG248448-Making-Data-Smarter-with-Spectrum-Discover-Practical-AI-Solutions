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
import requests
import logging
import sys
import json

ENCODING = 'utf-8'

def check_for_connection_updates(application, drh):
    """Check for connection updates and close connection if needed."""
    while True:
        try:
            # This will raise a KeyError when nothing is in the set
            conn = application.kafka_connections_to_update.pop()

            logger.debug("Closing connection: %s", str(conn))
            drh[conn].close_connection()

            # Always remove the element without error
            drh.pop(conn, None)
        except KeyError:
            break

if __name__ == '__main__':
    # Instantiate logger
    loglevels = {'INFO': logging.INFO, 'DEBUG': logging.DEBUG,
                 'ERROR': logging.ERROR, 'WARNING': logging.WARNING}
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(stream=sys.stdout,
                        format=log_format,
                        level=loglevels[log_level])
    logger = logging.getLogger(__name__)
    
    #Get variable about inference script
    inference_server_host = os.environ.get('INFERENCE_API_SERVER_HOST', '')
    inference_server_endpoint = os.environ.get('INFERENCE_API_SERVER_ENDPOINT', '/infer')
    inference_server_port = os.environ.get('INFERENCE_API_SERVER_PORT', '5757')
    
    valid_inference_vars=True
    if not inference_server_host:
        logger.error("INFERENCE_SERVER_IP is not set via an environment variable.")
        valid_inference_vars=False
    if not inference_server_endpoint:
        logger.error("INFERENCE_API_SERVER_ENDPOINT is not set via an environment variable.")
        valid_inference_vars=False
    if not inference_server_port.isnumeric():
        logger.error("INFERENCE_API_SERVER_PORT is not a number.")
        valid_inference_vars=False
    if not valid_inference_vars:
        raise SystemExit("Missing one or more environment variables.")

    inference_server_url=inference_server_host + ':' + inference_server_port + inference_server_endpoint
    logger.debug("Inference server : %s" % inference_server_url)
        
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

                # check to see if there are any connection updates available and close them
                check_for_connection_updates(application, drh)

                # DocumentKey is a unique identifier for a document, amalgam of connection + name
                key = DocumentKey(docs)
                # Create and store a retriever for this if we haven't yet
                if key.id not in drh:
                    drh[key.id] = DocumentRetrievalFactory().create(application, key)

                logger.info('PID:{} Inspecting Document:{}'.format(os.getpid(), key.path))

                # Application does its work on the file based on the action params
                # get_document the abstracted function that brings back the file path
                try:
                    tmpfile_path = drh[key.id].get_document(key)
                except AttributeError:
                    logger.info("Connection does not exist for %s. Skipping.", str(key.id))
                    reply.add_result('skipped', key)
                    continue
                logger.debug("File retrieved : %s ", tmpfile_path)

                if tmpfile_path:
                    ##################################################
                    ################ Start Custom Code ###############
                    ##################################################
                    model_version=""
                    filename_seg=""
                    nodules_count=""
                    result=""
                    tags = {}
                    logger.debug("Open the file : %s ", tmpfile_path)
                    try:
                        files = {'file': open(tmpfile_path,'rb')}
                    except FileNotFoundError:
                        logger.info("Could not find file: %s.", tmpfile_path)
                        reply.add_result('failed', key)
                        continue
                    except PermissionError:
                        logger.info("Could not open file: %s.", tmpfile_path)
                        reply.add_result('failed', key)
                        continue

                    # Send file to inference through the exposed API and get the result
                    logger.debug("Send file to  : %s ", inference_server_url)
                    try:
                        response = requests.post(inference_server_url, files=files)
                    except requests.exceptions.RequestException as ex:
                        logger.info("Error while sending file %s to inference: %s.", tmpfile_path, ex)
                        reply.add_result('failed', key)
                        continue
                    
                    #Parse the JSON output and retrieve the value
                    try:
                        logger.debug("JSON : %s", response.text)
                        logger.debug("HTTP Code : %s", str(response.status_code))
                        dict=response.json()
                        model_version=str(dict["model_version"])
                        filename_seg=dict["filename_seg"]
                        nodules_count=str(dict["obj_count"])
                        result=json.dumps(dict["result"])
                    except json.decoder.JSONDecodeError as ex:
                        logger.info("Error while reading the inference response: %s.", ex)
                        reply.add_result('failed', key)
                        continue

                    logger.debug('... modele version : %s' % model_version)
                    logger.debug('... filename seg : %s' % filename_seg)
                    logger.debug('... nodule count : %s' % nodules_count)
                    logger.debug('... result : %s' % result)

                    #Assign value to the corresponding tag
                    #for example, the nodules_count value will be assign to any tag containing the string "nodules_count"#
                    #for example, the name of the tag could be inference_nodule_count
                    tags_to_extract = work['action_params']['extract_tags']
                    for tag in tags_to_extract:
                        if "segfile" in tag:
                            value=filename_seg
                        elif "model_version" in tag:
                            value=model_version
                        elif "nodules_count" in tag:
                            value=nodules_count
                        elif "result" in tag:
                            value=result
                        else:
                            value=""
                        logger.debug("Set %s to %s"%(tag,value))
                        tags[tag]=value
                    status='success'
                    ##################################################
                    ################# End Custom Code ################
                    ##################################################

                    drh[key.id].cleanup_document()
                    reply.add_result('success', key, tags)
                else:
                    reply.add_result('failed', key)

            logger.info("Sending result to Discover")
            am.send_reply(reply)
        else:
            # timeout
            logger.info("Poll timeout reached - passing")
            pass


