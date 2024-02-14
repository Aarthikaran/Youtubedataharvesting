from googleapiclient.discovery import build
import pymongo
import pymysql
import mysql.connector
import numpy as np
import pandas as pd
import streamlit as st


def api_connect():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = 'AIzaSyCITDJACVK63GLdO1VV2U1x1dl7iWOtrEM'
    
    youtube = build(api_service_name, api_version, developerKey=api_key)

    return youtube
youtube = api_connect()

# get channel_information
def get_channel_info(channel_id):

    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data = dict(channel_name = i['snippet']['title'],
                    channel_id = i['id'],
                    channel_description = i['snippet']['description'],
                    
                    channel_playlist = i['contentDetails']['relatedPlaylists']['uploads'],
                    channel_scount = i['statistics']['subscriberCount'],
                    channel_vcount = i['statistics']['videoCount'],
                    channel_viewcount = i['statistics']['viewCount'])
        return data
    

  # get video_id
def get_videos_ids(channel_id):

    video_ids=[]
    response = youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    channel_playlist = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True: 
        response1 = youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=channel_playlist,
                                        maxResults=50,
                                        pageToken=next_page_token
                                        ).execute()
        for i in range(len(response1['items'])):

            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids


# get video_information
def get_video_info(video_id):

    video_data=[]

    for video_id in video_id:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                            Channel_Id=item["snippet"]["channelId"],
                            Video_Id=item["id"],
                            Title=item["snippet"]["title"],
                            Tags=item["snippet"].get('tags'),
                            Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                            Video_Description=item["snippet"].get("description"),
                            Published_Date=item["snippet"]["publishedAt"],
                            Duration=item["contentDetails"]["duration"],
                            Views_Count=item["statistics"].get("viewCount"),
                            likes=item["statistics"].get("likeCount"),
                            Comments=item["statistics"].get('commentCount'),
                            Favourite_count=item['statistics']["favoriteCount"],
                            Definition=item["contentDetails"]["definition"],
                            Caption_Status=item["contentDetails"]["caption"]
                            )
        video_data.append(data)
    return video_data  


# get comment_information

def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()
            for item in response['items']:
                data = dict(comment_Id= item['snippet']['topLevelComment']['id'],
                        video_Id= item['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_Text= item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_Author= item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_Published= item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
    except:
        pass

    return comment_data


# get playlist information

def get_playlist_details(channel_id):
    
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(playlist_Id=item['id'],
                      Title=item['snippet']['title'],
                      channel_id=item['snippet']['channelId'],
                      channel_Name=item['snippet']['channelTitle'],
                      publishedAt=item['snippet']['publishedAt'],
                      video_count=item['contentDetails']['itemCount'])
            All_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data


# Way to MongoDB
from pymongo import MongoClient
myclient = MongoClient("mongodb://localhost:27017/")
db = myclient['youtube_data_harvesting']
coll1=db["channels_details"]

def main(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)
    
    coll1 = db['channels_details']
    coll1.insert_one({'channel_information':ch_details,
                      'playlist_information':pl_details,
                      'video_information':vi_details,
                      'comment_information':com_details})
    return "upload completed successfully"


# table creation for channels, playlist, videos and comments
def channels_table():

    mydb=pymysql.connect(host="localhost",
                        user="root",
                        password="MySQL@123",
                        database='youtube_data_harvesting')
    mycursor=mydb.cursor()

    drop_query='''DROP TABLE IF EXISTS channels'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels(channel_name varchar(255),
                                                                    channel_id varchar(100) primary key,
                                                                    channel_description text,
                                                                    channel_playlist varchar(100),
                                                                    channel_scount bigint,
                                                                    channel_vcount int,
                                                                    channel_viewcount bigint)'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        print('channel tables are already created')

    ch_list = []
    db=myclient["youtube_data_harvesting"]
    coll1=db["channels_details"]
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])

    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query='''insert into channels(channel_name,
                                                    channel_id,
                                                    channel_description,
                                                    channel_playlist,
                                                    channel_scount,
                                                    channel_vcount,
                                                    channel_viewcount)
                                            
                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['channel_name'],
                row['channel_id'],
                row['channel_description'],
                row['channel_playlist'],
                row['channel_scount'],
                row['channel_vcount'],
                row['channel_viewcount'])
        
        
        # mycursor.execute(insert_query, values)
        # mydb.commit()
        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
                print(e)


def videos_table():
    mydb=mysql.connector.connect(
                                host='localhost',
                                user='root',
                                password='MySQL@123',
                                database='youtube_data_harvesting'
    )
    mycursor=mydb.cursor()

    drop_query='''DROP TABLE IF EXISTS video_details'''
    mycursor.execute(drop_query)
    mydb.commit()


    create_query='''CREATE TABLE IF NOT EXISTS video_details (
        Channel_Name VARCHAR(100),
        Channel_Id VARCHAR(100),
        Video_Id VARCHAR(100),
        Title VARCHAR(150),
        Thumbnail VARCHAR(200),
        Video_Description TEXT,
        Published_Date varchar(50),
        Duration varchar(50),
        Views_Count bigint,
        Likes varchar(50),
        Comments varchar(50),
        Favourite_Count varchar(50),
        Definition VARCHAR(50),
        Caption_Status VARCHAR(50)
    )
    '''

    mycursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db=myclient["youtube_data_harvesting"]
    coll1=db["channels_details"]
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])

    df1 = pd.DataFrame(vi_list)

    for index,row in df1.iterrows():
        
        insert_query='''INSERT INTO video_details(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Thumbnail,
                                                        Video_Description,
                                                        Published_Date,
                                                        Duration,
                                                        Views_Count,
                                                        Likes,
                                                        Comments,
                                                        Favourite_Count,
                                                        Definition,
                                                        Caption_Status)

                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Thumbnail'],
                row['Video_Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views_Count'],
                row['likes'],
                row['Comments'],
                row['Favourite_count'],
                row['Definition'],
                row['Caption_Status']

                )
        mycursor.execute(insert_query,values)
        mydb.commit()
        # try:
        #     mycursor.execute(insert_query, values)
        #     mydb.commit()
        # except Exception as e:
        #         print(e)

def comments_table():

    mydb=pymysql.connect(host="localhost",
                        user="root",
                        password="MySQL@123",
                        database='youtube_data_harvesting')
    mycursor=mydb.cursor()

    drop_query='''DROP TABLE IF EXISTS comments'''
    mycursor.execute(drop_query)
    mydb.commit()


    create_query = '''CREATE TABLE IF NOT EXISTS comments(comment_Id varchar(100) primary key,
                                                        video_Id varchar(50),
                                                        comment_Text text,
                                                        comment_Author varchar(150),
                                                        comment_Published varchar(100))'''
    mycursor.execute(create_query)
    mydb.commit()

    com_list = []
    db=myclient["youtube_data_harvesting"]
    coll1=db["channels_details"]
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])

    df2 = pd.DataFrame(com_list)

    for index,row in df2.iterrows():
            insert_query='''insert into comments(comment_Id,
                                                    video_Id,
                                                    comment_Text,
                                                    comment_Author,
                                                    comment_Published)
                                                
                                                    values(%s,%s,%s,%s,%s)'''
            
            values=(row['comment_Id'],
                    row['video_Id'],
                    row['comment_Text'],
                    row['comment_Author'],
                    row['comment_Published']
                    )
            
            
            mycursor.execute(insert_query,values)
            mydb.commit()

def tables():
    channels_table()
    videos_table()
    comments_table()

    return 'tables created succcessfully'



def show_channels_table():
    ch_list = []
    db=myclient["youtube_data_harvesting"]
    coll1=db["channels_details"]
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])

    df = st.dataframe(ch_list)

    return df

def show_videos_table():
    vi_list = []
    db=myclient["youtube_data_harvesting"]
    coll1=db["channels_details"]
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])

    df1=st.dataframe(vi_list)

    return df1

def show_comments_table():
    com_list = []
    db=myclient["youtube_data_harvesting"]
    coll1=db["channels_details"]
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])

    df2=st.dataframe(com_list)

    return df2

# streamlit part
# with st.sidebar:
#     st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
#     st.header("Skill Take Away")
#     st.caption("Python Scripting")
#     st.caption("Data Collection")
#     st.caption("MongoDB")
#     st.caption("API Integration")
#     st.caption("Data Management using MongoDB and MySQL")

# st.image(r"C:\Users\Aarthi\Downloads\pngtree-youtube-social-media-3d-stereo-png-image_6308427.png")

st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")


channel_id=st.text_input("Enter the channel_ID")

if st.button("collect and store data"):
    ch_ids=[]
    db = myclient['youtube_data_harvesting']
    coll1=db["channels_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data['channel_information']['channel_id'])

        if channel_id in ch_ids:
            st.success("Channel details of the given Channel Id already exists")
        else:
            insert=main(channel_id)
            st.success(insert)

if st.button('Migrate to sql'):
    Tables=tables()
    st.success(Tables)

show_table=st.radio("SELECT THE TABLE FOR VIEW",('CHANNELS','VIDEOS','COMMENTS'))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()

# sql connection

mydb=pymysql.connect(host="localhost",
                    user="root",
                    password="MySQL@123",
                    database='youtube_data_harvesting')
mycursor=mydb.cursor()

questions = st.selectbox("Select Your Questions",("1. What are the names of all the videos and their corresponding channels",
                                                  "2. Which channels have the most number of videos, and how many videos do they have",
                                                  "3. What are the top 10 most viewed videos and their respective channels",
                                                  "4. How many comments were made on each video, and what are their corresponding video names",
                                                  "5. Which videos have the highest number of likes, and what are their corresponding channel names",
                                                  "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names",
                                                  "7. What is the total number of views for each channel, and what are their corresponding channel name",
                                                  "8. What are the names of all the channels that have published videos in the year 2022",
                                                  "9. What is the average duration of all videos in each channel, and what are their corresponding channel names",
                                                  "10. Which videos have the highest number of comments, and what are their corresponding channel name"))

mydb=pymysql.connect(host="localhost",
                    user="root",
                    password="MySQL@123",
                    database='youtube_data_harvesting')
mycursor=mydb.cursor()

if questions=="1. What are the names of all the videos and their corresponding channels":

    query1='''select Title as videos,channel_Name as channelname from video_details;'''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif questions=="2. Which channels have the most number of videos, and how many videos do they have":

    query2='''select Channel_Name as channelname,channel_vcount as no_videos from channels order by channel_vcount desc'''
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)

elif questions=="3. What are the top 10 most viewed videos and their respective channels":

    query3='''select Views_Count as views,Title as channelname,Title as videotitle from video_details where Views_Count is not null order by views desc'''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","video title"])
    st.write(df3)

elif questions=="4. How many comments were made on each video, and what are their corresponding video names":

    query4='''select Comments as no_comments,Title as videotitle from video_details where Comments is not null'''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no_comments","videotitle"])
    st.write(df4)

elif questions=="5. Which videos have the highest number of likes, and what are their corresponding channel names":

    query5='''select Title as videotitle,Channel_Name as channelname,likes as likescount from video_details where likes is not null order by likes desc'''
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["video title","channel name","likes"])

    st.write(df5)

elif questions=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names":

    query6='''select likes as likecount,Title as videotitle from video_details'''
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likes","video title"])

    st.write(df6)

elif questions=="7. What is the total number of views for each channel, and what are their corresponding channel name":

    query7='''select channel_viewcount as totalviews, channel_name as channelname from channels'''
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["views","channel name"])

    st.write(df7)

elif questions=="8. What are the names of all the channels that have published videos in the year 2022":

    query8='''select Title as video_title,Published_Date as publisheddate,Title as channelname from video_details where Published_Date=2022;'''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video title","published date","channel_name"])
    
    st.write(df8)

elif questions=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names":

    query9='''select Channel_Name as channelname,avg(duration) as averageduration from video_details group by Channel_Name'''
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channeltitle=row["channelname"]
        Duration=row["averageduration"]
        average_duration_str=str(Duration)
        T9.append(dict(channel_Name=channeltitle,Duration=average_duration_str))
    df1=pd.DataFrame(T9)    
    st.write(df1)

elif questions=="10. Which videos have the highest number of comments, and what are their corresponding channel name":

    query10='''select Title as videotitle,Comments as comments,Channel_Name as channelname from video_details where Comments is not null order by Comments desc'''
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","comments","channel_name"])
    st.write(df10)  












