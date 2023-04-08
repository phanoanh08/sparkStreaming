from googleapiclient.discovery import build
import pathlib
from urllib.parse import urlparse, parse_qs
import json
import os
import sys
import random

DEVELOPER_KEY = "AIzaSyAW6qa2wIs1gm5mcbLRam-ppftgZvE88qs"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

youtube_service = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey = DEVELOPER_KEY)

# video_link = "https://www.youtube.com/watch?v=wxh-7v4VlZk"
# data_root = 'comment/'

def getVideoId(url):
    try:
        parts = urlparse(url)
        id = parse_qs(parts.query)
        if id:
            return id['v'][0]
        id = parts.path.split('/')[-1]
        if id:
            return id
        else:
            print('Cannot get video id!')
    except:
        print('Failed to get video id!')

# def predictLabel(cmt): ##illustrate model to predict label
#     label = random.randint(0, 2)
#     return label

def extractData(url):
    data_appended = []

    video_id = getVideoId (url)
    response = youtube_service.commentThreads().list(
        part='snippet',
        textFormat='plainText',
        videoId=video_id
    ).execute()

    while response:
        items = response['items']
        for item in items:
            try:
                author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                author_id = item['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                datetime = item['snippet']['topLevelComment']['snippet']['updatedAt']           
            except:
                continue
            data = dict({'video_id':video_id, 'dtime':datetime, 'author_id':author_id, 'comment':comment, 'label':0})
            data_appended.append(data)
        
        if 'nextPageToken' in response:
            response = youtube_service.commentThreads().list(
                part='snippet',
                textFormat='plainText',
                videoId=video_id,
                pageToken=response['nextPageToken']
            ).execute()
        else: 
            break
    print("\n>>>>>Number of comment: ", len(data_appended))

    # for item in data_appended:
    #     item['label'] = predictLabel(item['comment'])

    return data_appended

# if __name__ == "__main__":
#     # data_root = sys.argv[1]
#     # video_link = sys.argv[2]
#     data_root = 'comment/'
#     video_link = 'https://youtu.be/N0VOGhhDWpc'
#     writeComment(video_link, data_root)



