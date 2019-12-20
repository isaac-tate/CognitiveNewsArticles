#Imports
import pandas as pd

import os
import time

import firebase_admin
import google.cloud
from firebase_admin import credentials
from firebase_admin import firestore


#Parameters
current_path = os.path.join(os.getcwd(), "all_sources")
current_path = os.path.join(current_path, "all_sources_csv.csv")
source_collection = 'feeds'
number_of_docs = None

#Initiate Firestore
cred = credentials.Certificate("FS_KEY.json")
app = firebase_admin.initialize_app(cred)



def populateFeeds():
    store = firestore.client()
    df = pd.read_csv(current_path)
    for index, row in df.iterrows():
        doc_ref = store.collection(source_collection).document(row["Source"])
        doc_ref.set({
                u'Name' : row["Source"],
                u'Image': row["Image"]
            })

def populateBranches():
    store = firestore.client()
    df = pd.read_csv(current_path)
    for index, row in df.iterrows():
        doc_ref = store.collection(source_collection).document(row["Source"]).collection("branches").document(row["Full Name"])
        doc_ref.set({
                u'Name' : row["Full Name"],
                u'Domain': row["Domain"],
                u'Short Name': row["Short Name"],
                u'Parent' : row["Source"],
                u'Image': row["Image"]
            })


populateFeeds()
populateBranches()

