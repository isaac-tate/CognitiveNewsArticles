#IMPORTS
import feedparser
import time
from subprocess import check_output
import sys

import nltk
from newspaper import Article

import os
import requests
import time
import datetime
import csv
from xml.etree import ElementTree

import random

import os, uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess

import pandas as pd

import firebase_admin
import google.cloud
from firebase_admin import credentials
from firebase_admin import firestore

#CLASSES
class TextToSpeech(object):
    def __init__(self, subscription_key, mp3_file_name, text):
        self.subscription_key = subscription_key
        self.tts = text
        self.mp3_file_name = mp3_file_name
        self.timestr = time.strftime("%Y%m%d-%H%M")
        self.access_token = None
        
    def get_token(self):
        fetch_token_url = "https://eastus.api.cognitive.microsoft.com/sts/v1.0/issueToken"
        headers = {
            'Ocp-Apim-Subscription-Key': self.subscription_key
        }
        response = requests.post(fetch_token_url, headers=headers)
        self.access_token = str(response.text)
        
    def save_audio_male(self):

        base_url = 'https://eastus.tts.speech.microsoft.com/'
        path = 'cognitiveservices/v1'
        constructed_url = base_url + path
        headers = {
            'Authorization': 'Bearer ' + self.access_token,
            'Content-Type': 'application/ssml+xml',
            'X-Microsoft-OutputFormat': 'riff-24khz-16bit-mono-pcm',
            'User-Agent': 'Speech-ytc-n'
        }
        xml_body = ElementTree.Element('speak', version='1.0')
        xml_body.set('{http://www.w3.org/XML/1998/namespace}lang', 'en-us')
        voice = ElementTree.SubElement(xml_body, 'voice')
        voice.set('{http://www.w3.org/XML/1998/namespace}lang', 'en-US')
        voice.set(
            'name', 'Microsoft Server Speech Text to Speech Voice (en-US, GuyNeural)')
        voice.text = self.tts
        body = ElementTree.tostring(xml_body)
        
        response = requests.post(constructed_url, headers=headers, data=body)
        if response.status_code == 200:
            with open(self.mp3_file_name, 'wb') as audio:
                audio.write(response.content)
                print("Audio file generated")
        else:
            print("\nStatus code: " + str(response.status_code) +
                  "\nSomething went wrong. Check your subscription key and headers.\n")

    def save_audio_female(self):

        base_url = 'https://eastus.tts.speech.microsoft.com/'
        path = 'cognitiveservices/v1'
        constructed_url = base_url + path
        headers = {
            'Authorization': 'Bearer ' + self.access_token,
            'Content-Type': 'application/ssml+xml',
            'X-Microsoft-OutputFormat': 'riff-24khz-16bit-mono-pcm',
            'User-Agent': 'Speech-ytc-n'
        }
        xml_body = ElementTree.Element('speak', version='1.0')
        xml_body.set('{http://www.w3.org/XML/1998/namespace}lang', 'en-us')
        voice = ElementTree.SubElement(xml_body, 'voice')
        voice.set('{http://www.w3.org/XML/1998/namespace}lang', 'en-US')
        voice.set(
            'name', 'Microsoft Server Speech Text to Speech Voice (en-US, JessaNeural)')
        voice.text = self.tts
        body = ElementTree.tostring(xml_body)
        
        response = requests.post(constructed_url, headers=headers, data=body)
        if response.status_code == 200:
            with open(self.mp3_file_name, 'wb') as audio:
                audio.write(response.content)
                print("Audio file generated")
        else:
            print("\nStatus code: " + str(response.status_code) +
                  "\nSomething went wrong. Check your subscription key and headers.\n")

                  

def pullNewStories(feed, feed_url, last_link, domain, source_s, fullname_s, source_image):
    try:
        try:
            d = feedparser.parse(feed_url)
            link = d.entries[0].link
            
        except:
            print("No data in new link... Waiting for next in " + feed)
            return last_link

        if(link == last_link):
            print("No new stories from " + feed)
            time.sleep(60)
            return None
        else:
            dtlocal = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            print("Getting new story from: " + feed + " at " + dtlocal)

            
            article = Article(link)
            article.download()
            article.parse()
            
            title = article.title
            authors = article.authors
            text = article.text

            try:
                story_image = article.top_image
            except:
                story_image = None

            if(text.count(" ") < 50):
                print("Article too short...")
                return link

            publish_date = article.publish_date
            keywords = article.keywords
            
            dtlocal = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            dtlocal_int = int(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))

            mp3_file_name = feed +'-' + dtlocal + '.mp3'
            
            app = TextToSpeech(subscription_key, mp3_file_name, text)
            app.get_token()

            #Choose voice at random
            rand_num = random.randint(1,2)

            if(rand_num == 1):
                app.save_audio_female()
            else:
                app.save_audio_male()
            
            service = BlockBlobService(account_name=sa_name, account_key=sa_key)
            
            service.create_blob_from_path(container, mp3_file_name, mp3_file_name)
            url = service.make_blob_url(container, mp3_file_name)

            #Firestore Part
            
            store = firestore.client()
            doc_ref = store.collection(u'feeds').document(source_s).collection("branches").document(fullname_s).collection("audio").document()

            doc_ref.set({
                u'Feed' : feed,
                u'Keywords' : keywords,
                u'Publish Date' : publish_date,
                u'Authors' : authors,
                u'Story URL' : link,
                u'Title': title,
                u'URL': url,
                u'Domain':domain,
                u'Upload': dtlocal,
                u'Upload Int': dtlocal_int,
                u'Story Image': story_image,
                u'Source Image': source_image
            })


            #Maintain top articles

            id_of_oldest = ""
            date_of_oldest = 99999999999999
            count = 0

            doc_ref2 = store.collection(u"collections").document(domain).collection(u"audio").stream()
            for doc in doc_ref2:
                docdic = doc.to_dict()
                count = count + 1
                thisDate = docdic["Upload Int"]
                if(thisDate < date_of_oldest):
                    date_of_oldest = thisDate
                    id_of_oldest = doc.id

            if(count > 20):
                store.collection(u"collections").document(domain).collection(u"audio").document(id_of_oldest).delete()
                print("Deleting oldest doc from collection")


            doc_ref3 = store.collection(u"collections").document(domain).collection(u"audio").document()
            doc_ref3.set({
                u'Feed' : feed,
                u'Keywords' : keywords,
                u'Publish Date' : publish_date,
                u'Authors' : authors,
                u'Story URL' : link,
                u'Title': title,
                u'URL': url,
                u'Domain':domain,
                u'Upload': dtlocal,
                u'Upload Int': dtlocal_int,
                u"Story Image": story_image,
                u'Source Image': source_image
            })


            os.remove(mp3_file_name)

            return link

    except Exception as e:
        print("Troubles connecting to " + feed)
        print(e)
        return link

    

#Parameters
subscription_key = "a76e8650a3964a23acbfd0c77e0d9f3c"
container = 'media'
sa_name = 'stogaccytc001'
sa_key = 'cCTLKAxKQ0SOP6waWVngZaBD920Yr7LzrVyciwzbdNCGm70J/QYbQXvLF7m7KDGXQLyO8TK/pXdNsyT6fNP/eQ=='

#ARRAYS
current_path = os.path.join(os.getcwd(), "all_sources")
current_path = os.path.join(current_path, "all_sources_csv.csv")

#INITIAL SET UP
cred = credentials.Certificate("FS_KEY.json")
app = firebase_admin.initialize_app(cred)

while(True):
    df = pd.read_csv(current_path)
    for index, row in df.iterrows():
        source_s = row["Source"]
        fullname_s = row["Full Name"]
        feed_s = row["Short Name"]
        rss_s = row["RSS"]
        last_link = row["Last Link"]
        domain = row["Domain"]
        source_image = row["Image"]

        voice_rand = random.randint(1,2)
        new_link = pullNewStories(feed_s, rss_s, last_link, domain, source_s, fullname_s, source_image)

        if(new_link is not None):  
            row["Last Link"] = new_link
        
    df.to_csv(current_path, index=False)


        
