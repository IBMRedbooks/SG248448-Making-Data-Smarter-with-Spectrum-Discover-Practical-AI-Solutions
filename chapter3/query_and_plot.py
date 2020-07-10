#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import json
import logging
import os
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

from urllib.parse import urljoin

SD_HOST = os.environ["SD_HOST"]
SD_USER = os.environ["SD_USER"]
SD_PASSWORD = os.environ["SD_PASSWORD"]

def get_token():
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


def get_discover_data():
    global SD_TOKEN
    global SD_HOST
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer %s' % SD_TOKEN if SD_TOKEN else ""
    }
    json_query = {
        "query":"path like '/export/lidcdata/dataset%'",
        "filters":[],
        "group_by":[],
        "sort_by":[],
        # "limit":3
    }
    query_url=urljoin(SD_HOST,"/db2whrest/v1/search")
    try:
        response = requests.post(url=query_url, verify=False, json=json_query, headers=headers, auth=None)
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
            return json.loads(response.json()["rows"])
        raise Exception("Metadata failed with code %d" % response.status_code)
    except Exception as exc:
        logger.error('Application failed to obtain token (%s)', str(exc))
        raise


# Plots ########################################################################
def plot(age, sex, smoker):
    sns.set()

    # Ages histogram
    bins = np.linspace(0, 120, 20)
    sns.distplot(age[sex == "M"], color="blue", bins=bins)
    sns.distplot(age[sex == "F"], color="red", bins=bins)
    plt.savefig("plot-ages_histogram.png")

    # Ages catplot
    plt.clf()
    sns.catplot(x="sex", y="age", kind="box", data=pd.DataFrame({"age": age, "sex": sex}))
    plt.savefig("plot-ages_catplot.png")

    # Smokers stats plots
    m_smokers = dict(np.array(np.unique(smoker[sex == "M"], return_counts=True)).T) 
    f_smokers = dict(np.array(np.unique(smoker[sex == "F"], return_counts=True)).T) 
    smokers_data = pd.DataFrame([[m_smokers[True], m_smokers[False]], [f_smokers[True], f_smokers[False]]],
            columns=["Smoker", "No smoker"], index=["Male", "Female"])

    sns.heatmap(smokers_data, square=True, annot=True, cbar=False, cmap='Blues', fmt='g')
    plt.savefig("plot-smokers_heatmap.png")

if __name__=="__main__":
    SD_TOKEN = get_token()

    age, sex, smoker = [], [], []
    for l in get_discover_data():
        age.append(int(l["dicom_page"]))
        sex.append(l["dicom_psex"])
        smoker.append(l["dicom_smoker"] == "True")
    age = np.array(age)
    sex = np.array(sex)
    smoker = np.array(smoker)

    plot(age, sex, smoker)

