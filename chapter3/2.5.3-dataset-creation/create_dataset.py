#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import faker
faker = faker.Faker()

import pickle as pkl
import pydicom
from pydicom.datadict import DicomDictionary, keyword_dict
import SimpleITK as sitk

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import requests

# Generate random metadata #####################################################

def random_metadata(output_size):
    # Age #####
    # random gaussian
    age = np.random.randn(output_size) * 70 + 50
    # clip values between 18 and 100, more or less
    age[age < 18] = abs(age[age < 18]) + 48
    age[age > 100] = age[age > 100] % 82 + 18
    # add one outlier
    age[513] = -5
    age = age.astype(np.int8)

    # Sex #####
    sex = np.random.choice(["M", "F"], size=output_size, p=[0.63, 0.37])

    # Smoking habits #####
    smoker = np.array([None] * output_size)
    smoker[sex == "M"] = np.random.choice([True, False], size=np.sum(sex == "M"), p=[0.42, 0.58])
    smoker[sex == "F"] = np.random.choice([True, False], size=np.sum(sex == "F"), p=[0.27, 0.73])
    return age, sex, smoker


# Convert NRRD to DICOM #########################################################

def nrrd2dcm(input_path, output_path):
    """
    Cast NRRD (in input_path) into DICOM (in output_path)
    Make sure the extensions are correct because type is automatically detected
    """
    assert(input_path.endswith(".nrrd"))
    assert(output_path.endswith(".dcm"))
    # Load image
    img = sitk.ReadImage(input_path)
    # Cast to unsigned int16 (otherwise not supported by sitk) 
    # From https://discourse.itk.org/t/unable-to-write-dicom-file-with-double-values/2299/5
    castFilter = sitk.CastImageFilter()
    castFilter.SetOutputPixelType(sitk.sitkUInt16)
    img = castFilter.Execute(img)
    # Save image
    sitk.WriteImage(img, output_path)



# Replace metadatas with fake ones ##############################################
# I couldn't manage to do it using SimpleITK, so I used pydicom to add them

def fake_metadata(input_path, age, sex, smoker, output_path):
    """
    Replace metadata of the DICOM at path by fake ones:
    PatientID, PatientName, PatientSex
    """
    # load image and remove existing tags
    img = pydicom.dcmread(input_path)
    # for tag in img.dir():
    #     del img[tag]
    # add fake tags
    # it's possible to add custom tags, see 
    # https://pydicom.github.io/pydicom/stable/auto_examples/metadata_processing/plot_add_dict_entries.html
    profile = faker.profile()
    img.PatientID = profile["ssn"]
    img.PatientName = profile["name"]
    img.PatientAge = str(age)
    img.PatientSex = str(sex)
    print("%s,%s,%s,%s,%s,%s,%s" % (
        profile["ssn"],profile["name"],profile["blood_group"],
        profile["mail"],age,sex,smoker))
    # save image
    img.save_as(output_path)




def transform(input_path, age, sex, smoker):
    # print(os.path.basename(input_path))
    output_path = os.path.join("/wmlce/data/data/LIDC-DICOM", os.path.splitext(os.path.basename(input_path))[0] + ".dcm")
    nrrd2dcm(input_path, output_path) # NRRD to DICOM
    fake_metadata(output_path, age, sex, smoker, output_path) # overwrite existing image



if __name__=="__main__":
    input_dir = "/wmlce/data/data/LIDC-IDRI/"
    nrrd_paths = sorted(glob.glob(input_dir + "**/*_CT.nrrd", recursive=True))
    age, sex, smoker = random_metadata(len(nrrd_paths))
    print("ssn,name,address,blood_group,mail,age,sex,smoker")
    for i, nrrd in enumerate(nrrd_paths):
        # print("[",i+1,"/",len(nrrd_paths),"]",sep="")
        transform(nrrd, age[i], sex[i], smoker[i])




