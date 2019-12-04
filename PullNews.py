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

import os, uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess

import firebase_admin
import google.cloud
from firebase_admin import credentials
from firebase_admin import firestore

#Parameters
subscription_key = "f3f3b632ddc34803bc780cda56b37cc4"
container = 'media'
sa_name = 'stogaccytc001'
sa_key = 'cCTLKAxKQ0SOP6waWVngZaBD920Yr7LzrVyciwzbdNCGm70J/QYbQXvLF7m7KDGXQLyO8TK/pXdNsyT6fNP/eQ=='

#ARRAYS
current_path = os.path.join(os.getcwd(), "sources")
directory = os.fsencode(current_path)

#CLASSES
class TextToSpeech(object):
    def __init__(self, subscription_key, mp3_file_name, text):
        self.subscription_key = subscription_key
        self.tts = text
        self.mp3_file_name = mp3_file_name
        self.timestr = time.strftime("%Y%m%d-%H%M")
        self.access_token = None
        
    def get_token(self):
        fetch_token_url = "https://eastus2.api.cognitive.microsoft.com/sts/v1.0/issuetoken"
        headers = {
            'Ocp-Apim-Subscription-Key': self.subscription_key
        }
        response = requests.post(fetch_token_url, headers=headers)
        self.access_token = str(response.text)
        
    def save_audio(self):
        base_url = 'https://eastus2.tts.speech.microsoft.com/'
        path = 'cognitiveservices/v1'
        constructed_url = base_url + path
        headers = {
            'Authorization': 'Bearer ' + self.access_token,
            'Content-Type': 'application/ssml+xml',
            'X-Microsoft-OutputFormat': 'riff-24khz-16bit-mono-pcm',
            'User-Agent': 'Speech-YTC'
        }
        xml_body = ElementTree.Element('speak', version='1.0')
        xml_body.set('{http://www.w3.org/XML/1998/namespace}lang', 'en-us')
        voice = ElementTree.SubElement(xml_body, 'voice')
        voice.set('{http://www.w3.org/XML/1998/namespace}lang', 'en-US')
        voice.set(
            'name', 'Microsoft Server Speech Text to Speech Voice (en-US, JessaRUS)')
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

                  

def pullNewStories(feed, feed_url, last_link):
    try:
        try:
            d = feedparser.parse(feed_url)
            link = d.entries[0].link
        except:
            print("No data in new link... Waiting for next in " + feed)
            return last_link

        if(link == last_link):
            print("No new stories from " + feed)
            time.sleep(2)
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
            publish_date = article.publish_date
            keywords = article.keywords
            
            dtlocal = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            mp3_file_name = feed +'-' + dtlocal + '.mp3'
            
            app = TextToSpeech(subscription_key, mp3_file_name, text)
            app.get_token()
            app.save_audio()
            
            service = BlockBlobService(account_name=sa_name, account_key=sa_key)
            
            service.create_blob_from_path(container, mp3_file_name, mp3_file_name)
            url = service.make_blob_url(container, mp3_file_name)
            
            store = firestore.client()
            doc_ref = store.collection(u'audio').document() 

            doc_ref.set({
                u'Feed' : feed,
                u'Keywords' : keywords,
                u'Publish Date' : publish_date,
                u'Authors' : authors,
                u'Story URL' : link,
                u'Title': title,
                u'URL': url,
                u'Upload': dtlocal
            })

            os.remove(mp3_file_name)

            return link

    except Exception as e:
        print("Troubles connecting to " + feed)
        #print(e)
        return link


#INITIAL SET UP
cred = credentials.Certificate("FS_KEY.json")
app = firebase_admin.initialize_app(cred)

while(True):
    print("Starting file cycle")
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        with open(os.path.join(current_path, filename)) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')

            csv_lines = list(csv_reader)

            feed_s = csv_lines[0][0]
            rss_s = csv_lines[0][1]
            last_title_s = csv_lines[0][2]

            new_link = pullNewStories(feed_s, rss_s, last_title_s)

            csv_file.close()

            if(new_link is not None):
                os.remove(os.path.join(current_path, filename))
                with open(os.path.join(current_path, filename), "w") as new_file:
                    writer = csv.writer(new_file)
                    writer.writerow([feed_s, rss_s, new_link])
                    new_file.close()
                
        
