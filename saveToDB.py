from extractData import getVideoId
from extractData import extractData
from datetime import datetime
import mysql.connector
import sys
import itertools
import pandas as pd
import json
from tqdm import tqdm



def mysqlConnector(username, pw, db):
    my_db = mysql.connector.connect(
        host="localhost",
        user=username, 
        password=pw,
        database=db
    )
    return my_db    

def formatDatetime(dtime):
    dt = datetime.fromisoformat(dtime[:-1]).strftime(r"%y-%m-%d %H:%M:%S")
    return dt

def insertVideoComment(ipt):
    data_extracted = extractData(ipt)
    sql = "insert into videoCmt (videoId, updateAt, authorId, cmt, label) values (%s, %s, %s, %s, %s)"
    vals = []
    for item in data_extracted:
        dtime = formatDatetime(item['dtime'])
        vals.append(tuple([item['video_id'], dtime, item['author_id'], item['comment'], item['label']]))
    return sql, list(set(vals))

def insertVideo():
    sql = "insert into video (videoId, lastUpdate) values (%s, %s)"
    dtime= datetime.now().strftime(r"%y-%m-%d %H:%M:%S")
    val =(video_id, dtime)    
    return sql, val

def updateVideo(video_id):
    dtime= datetime.now().strftime(r"%y-%m-%d %H:%M:%S")
    sql = "update video set lastUpdate = '{}' where videoId = '{}'".format(dtime, video_id)
    return sql


if __name__ == "__main__":
    mydb = mysqlConnector("oanh", "123456a_", "commentOnYtb")
    cursor = mydb.cursor(buffered=True)
    # ipt = sys.argv[1]
    src_path = "source.json"
    ipt_list = list(pd.read_json(src_path, lines=True)['share_url'].values)
    # ipt = 'https://youtu.be/N0VOGhhDWpc'

    for ipt in tqdm(ipt_list, total=len(ipt_list)): 
        video_id = getVideoId(ipt)
        cursor.execute("select videoId from video")
        video = list(itertools.chain(*cursor.fetchall()))

        sql_videoCmt, vals = insertVideoComment(ipt)
        if (len(video))!=0 and (video_id in video):
            # cursor.execute("select lastUpdate from video where videoId='{}'".format(video_id))
            # last_update = cursor.fetchall()[0][0]
            cursor.execute("select updateAt from videoCmt where videoId='{}' order by updateAt desc".format(video_id))
            max_date = max(list(itertools.chain(*cursor.fetchall())))
            vals = [i for i in vals if i[1] > max_date.strftime(r"%y-%m-%d %H:%M:%S")]
            sql_video = updateVideo(video_id)
            try:
                cursor.execute(sql_video)
                mydb.commit()
                print(">>>>>Updated 1 video!")
            except:
                print(">>>>>Update videoId unsuccessfully!')")
        else:
            sql_video, val = insertVideo()
            try:
                cursor.execute(sql_video, val)
                mydb.commit()
                print(">>>>>Inserted 1 video!")
            except:
                print(">>>>>Insert video unsuccessfully!')")  
        if len(vals) != 0:
            try:
                cursor.executemany(sql_videoCmt, vals)
                mydb.commit()
                print(">>>>>Inserted {} rows!".format(len(vals)))
            except:
                print('Insert unsuccessfully!')   
        else:
            print(">>>>>Donot have data!")
        cursor.execute("select * from videoCmt where videoId='{}'".format(video_id))
        if len(cursor.fetchall())==0: 
            cursor.execute("delete from video where videoId='{}'".format(video_id))
            mydb.commit()
            print(">>>>>Delete 1 video that havenot insert cmt yet!")
