
import mysql
import mysql.connector
import pandas as pd
import streamlit as st
import googleapiclient.discovery
import time as t
from streamlit_option_menu import option_menu
import re

st.set_page_config(page_title="Youtube Data Harvesting and Warehousing",
                   page_icon= "mod_youtube.png",
                   layout= "wide",
                   initial_sidebar_state= "auto",
                   )

# CREATING OPTION MENU
with st.sidebar:
    st.sidebar.image("mod_youtube.png")
    selected = option_menu("Main Menu", ["Data Collection","SQL Migration","Data Analysis"], 
                           icons=["cloud-upload","database","filetype-sql"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "23px","text-align": "centre", "margin": "0px", 
                                                "--hover-color": "yellow"},
                                   "icon": {"font-size": "28px"},
                                   "container" : {"max-width": "4000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})
def get_client():
    from pymongo import MongoClient
    connection = MongoClient("mongodb+srv://karthikeyancbm1982:em9uXzrMmlmF90GF@cluster0.gameu7o.mongodb.net/")
    return connection
get_client()

mongodb = get_client()

db=mongodb['youtube_data'] 
col=db['channel_details']

def get_connect():

    connect = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="youtube_data")
    return connect
get_connect()

my_sql = get_connect()

mycursor = my_sql.cursor()
    
def Api_connect():
    
    api_key = "AIzaSyATGv2bIbANrgYutI_H2ymikbLrepMbkHk"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version,developerKey = api_key)
    return youtube

youtube = Api_connect()


def channel_list(ch_id):
    ch_data=[]
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=ch_id
    )
    response=request.execute()

    for i in range(len(response['items'])):
       
        channel = dict(channel_id =response['items'][i]['id'],channel_name = response['items'][i]['snippet']['title'],channel_desc = response['items'][i]['snippet']['description'],
                channel_views = response['items'][i]['statistics']['viewCount'],
                Play_list =response['items'][i] ['contentDetails']['relatedPlaylists']['uploads'],
                Channel_type = response['items'][i]['kind']
                
                )
        ch_data.append(channel)
    
    return ch_data




def get_video_id(ch_id):

    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=ch_id
    )
    response=request.execute()
    
    Play_list = response['items'][0] ['contentDetails']['relatedPlaylists']['uploads']
    Play_list
    play_id=Play_list
    
    vid_request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId = play_id,
        maxResults=100       
    )
    response = vid_request.execute()
    videos_id=[]
    for i in range(len(response['items'])):
        videos_id.append((response['items'][i]['contentDetails']['videoId']))

    nextPageToken = response.get('nextPageToken')
    
    while True:    
        if nextPageToken is None:
            break
        else:
            vid_request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId = play_id,
            maxResults=50,
            pageToken = nextPageToken
            )
            response = vid_request.execute()

            for i in range(len(response['items'])):
                videos_id.append((response['items'][i]['contentDetails']['videoId']))
            
            nextPageToken = response.get('nextPageToken')
       
    return videos_id

def duration_convert(duration):
    pattern = r'PT(\d+H)?(\d+M)?(\d+S)?'
    find = re.match(pattern,duration)
    if find:
        hours, minutes, seconds = find.groups()
        hours = int(hours[:-1]) if hours else 0
        minutes = int(minutes[:-1]) if minutes else 0
        seconds = int(seconds[:-1]) if seconds else 0
        return "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)
    else:
        return "{:02d}:{:02d}:{:02d}".format(0, 0, 0)




def get_video_info(videos_id):

        responses = []
        for video_id in videos_id :
                request1 = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id)        
                response1 = request1.execute()
                responses.append(response1)

        video_data =[]        
        for response in responses:
                for video in response['items']:
                        video_stat = dict(channel_id=video['snippet']['channelId'],channel_Name=video['snippet']['channelTitle'],Title = video['snippet']['title'],Published_date=video['snippet']['publishedAt'],video_description =video['snippet']['description'],
                                view_count = video['statistics']['viewCount'],Like_count = video['statistics'].get('likeCount'),
                                favorite_count = video['statistics']['favoriteCount'],comment_count = video['statistics'].get('commentCount'),
                                duration = duration_convert(video['contentDetails']['duration']),thumbnail = video['snippet']['thumbnails']['default']['url'],
                                caption_status=video['contentDetails']['caption'],Video_id = video['id'],play_list_id = video['snippet']['channelId'])
                        
                        video_data.append(video_stat)
        return video_data


def get_comments(videos_id):
    comment_info=[]
    try:
        for i in videos_id:
            comm_request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=i        
            )
            response = comm_request.execute()
            for comment in response['items']:
                comment_details = dict(video_id = comment['snippet']['videoId'],
                          comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                          comment_author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          comment_published_date = comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                          comment_id = comment['snippet']['topLevelComment']['id'])      
                comment_info.append(comment_details)
    except:
         pass
    return comment_info

def get_youtube_data(ch_id):

    video_details = get_video_id(ch_id)    
    data = dict(channel_details = channel_list(ch_id),
    video_info_details = get_video_info(video_details),
    comment_details = get_comments(video_details))
    return data

def channel_table():
    
    query = '''CREATE TABLE IF NOT EXISTS channel(channel_id varchar (100),
                                    channel_name varchar (100),
                                    channel_desc text,
                                    channel_views bigint,
                                    Play_list varchar (90),
                                    Channel_type varchar (80)
                                    )'''
    mycursor.execute(query)
    my_sql.commit()

print(channel_table())

def video_table():
    
    
    query = '''CREATE TABLE IF NOT EXISTS video(channel_id varchar (100),channel_name varchar (100),
                                Title varchar (300),
                                 Published_date  varchar (100),
                                 video_description text,
                                 view_count bigint,
                                 Like_count bigint,
                                favorite_count bigint,
                                comment_count bigint,
                                duration varchar (50),
                                thumbnail varchar(255),
                                caption_status varchar(255),
                                Video_id varchar(255),
                                playlist_id varchar(255)       
                                
                                
                                 )'''
    mycursor.execute(query)
    my_sql.commit()
print(video_table())

def comment_table(): 
    
    
    query = '''CREATE TABLE IF NOT EXISTS comment(video_id varchar (300),
                                 comment_text  text,
                                 comment_author varchar(100),
                                 comment_published_date varchar(50),
                                 comment_id varchar(100)
                                )'''
    mycursor.execute(query)
    my_sql.commit()
print(comment_table())

def channel_view_table():
    import mysql.connector
    connect = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="youtube_data")
    mycursor = connect.cursor()
    channel= []
    query = "select * from channel"
    mycursor.execute(query)
    result = mycursor.fetchall()
    for data in result:
        channel.append(data)
    return channel       


def video_view_table():
    import mysql.connector
    connect = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="youtube_data")
    mycursor = connect.cursor()
    video = [] 
    query = "select * from video"
    mycursor.execute(query)
    result = mycursor.fetchall()
    for data in result:
        video.append(data)
    return video


def comment_view_table():
    import mysql.connector
    connect = mysql.connector.connect(host="localhost",user="root",password="Bairavi@17",database="youtube_data")
    mycursor = connect.cursor()
    comment = []
    query = "select * from comment"
    mycursor.execute(query)
    result = mycursor.fetchall()
    for data in result:
        comment.append(data)
    return comment

channel_view = channel_view_table()
video_view = video_view_table()
comment_view = comment_view_table()


if selected == "Data Collection":
    
    st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.markdown("#    ")
    st.write("### Enter :red[YouTube] :rainbow[Channel ID]:point_down:")
    ch_id = st.text_input("Enter the Channel id:herb:")

    if ch_id and st.button("Extract Data:satellite_antenna:"):
        with st.spinner("Collecting Data...:running:"):
            t.sleep(8)
        ch_details = channel_list(ch_id)
        st.write(f'#### Extracted data from :green["{ch_details[0]["channel_name"]}"] channel')
        st.title("Channel Information:loudspeaker:")
        st.dataframe(ch_details)
        with st.spinner("Collecting Data...:running:"):
            t.sleep(8)
        videos_id = get_video_id(ch_id)
        video_details= get_video_info(videos_id)
        st.title("Video Information:loudspeaker:")
        st.dataframe(video_details)
        with st.spinner("Collecting Data...:running:"):
            t.sleep(10)
        comment_details = get_comments(videos_id)
        st.title("Comment Information:loudspeaker:")
        st.dataframe(comment_details)
        st.balloons()

    if st.button("Upload to MongoDB:cherries:"):
        with st.spinner("Uploading Data...:running:"):
            t.sleep(25)

        chann_id = []         
        for data in col.find({},{'_id':0}):
            chann_id.append(data["channel_details"][0]["channel_id"]),
        
        for index  in chann_id:
            if ch_id in index:
                st.success("Channel Already Exists")
                break
        else:
            def get_data(ch_id):
                video_details = get_video_id(ch_id)
                collection1=get_youtube_data(ch_id)
                col.insert_one(collection1)
                return "Uploaded"
            get_data(ch_id)
            st.success("Uploaded")
            st.balloons()

if selected == "SQL Migration":
    st.title("Migrate Data into SQL Database")
    st.markdown("#    ")
    chann_name = []         
    for data in col.find({},{'_id':0}):
        chann_name.append(data["channel_details"][0]["channel_name"])
    
    channel_options = chann_name
    channel_selected = st.selectbox("Select the Channel to be Inserted into SQL",(channel_options),      
    index=None,
    placeholder="Select the Channel Name...")

    st.write('You selected:',channel_selected)
    sql_button = st.button("Submit")
    radio_buttons = ['NONE','CHANNELS','VIDEOS','COMMENTS']
    buttons = st.radio("Select the Options to View",radio_buttons)
    st.write("**You Selected:**",buttons)
    if sql_button:
        if channel_selected in channel_options:
            with st.spinner("Migrating Data...:running:"):
                t.sleep(10)
            def get_channel():
                channel_name = channel_selected
                query = {"channel_details.channel_name": channel_name}
                document = col.find_one(query, {"_id": 0, "channel_details": 1})
                channel_details_list = document.get("channel_details", [])
                channel_details = channel_details_list[0]  
                return tuple(channel_details.values())

            channel_data_tuple = get_channel()
            
            query = "INSERT INTO channel (channel_id, channel_name, channel_desc, channel_views, Play_list, Channel_type) VALUES (%s, %s, %s, %s, %s, %s)"
            mycursor.execute(query, channel_data_tuple)
            my_sql.commit()
        
                
            def get_video():
                channel_name = channel_selected
                query = {"channel_details.channel_name": channel_name}
                document = col.find_one(query, {"_id": 0, "video_info_details": 1})
                video_details_list = document.get("video_info_details", [])
                video_details = video_details_list[0]  
                return [tuple(video.values()) for video in video_details_list]
            
            video_data_tuple = get_video()
        
            query = "insert into video(channel_id ,channel_name,Title,Published_date,video_description,view_count,Like_count,favorite_count,comment_count,duration,thumbnail,caption_status,Video_id,playlist_id)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            mycursor.executemany(query,video_data_tuple)
            my_sql.commit()
        
                
            
            def get_comment():
                channel_name = channel_selected
                query = {"channel_details.channel_name": channel_name}
                for c_data in col.find(query, {"_id": 0, 'comment_details': 1}):
                    for i in c_data:
                        tuple_values=tuple(c_data[i])
                df=pd.DataFrame(tuple_values)
                
                rows1=[]
                for index in df.index:
                    row= tuple(df.loc[index].values)
                    row = tuple([str(d) for d in row])
                    rows1.append(row)
                query = "insert into comment(video_id,comment_text,comment_author,comment_published_date,comment_id)  values(%s,%s,%s,%s,%s)"
                mycursor.executemany(query,rows1)
                my_sql.commit()
                return "Comment Table Created"
            print(get_comment())
        else:
            def get_channel():
                channel_name = channel_selected
                query = {"channel_details.channel_name": channel_name}
                document = col.find_one(query, {"_id": 0, "channel_details": 1})
                channel_details_list = document.get("channel_details", [])
                channel_details = channel_details_list[0]  
                channel_tuple = tuple(channel_details.values())
                return channel_tuple
            print(get_channel())

            query = "insert into channel(ch_id,ch_name,ch_desc,ch_views,Play_list,Ch_type)  values(%s,%s,%s,%s,%s,%s)"
            mycursor.execute(query,get_channel())
            my_sql.commit()
            def get_video():
                channel_name = channel_selected
                query = {"channel_details.channel_name": channel_name}
                for v_data in col.find(query, {"_id": 0, 'video_info_details': 1}):
                    for i in v_data:
                        tuple_values=tuple(v_data[i])
                df=pd.DataFrame(tuple_values)
            
                rows=[]
                for index in df.index:
                    row= tuple(df.loc[index].values)
                    rows.append(row)
                return rows
            print(get_video())
            
            query = "insert into video(Title,Published_date,video_description,view_count,Like_count,favorite_count,comment_count,duration,thumbnail,caption_status,Video_id,playlist_id)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            mycursor.executemany(query,get_video())
            my_sql.commit()
            
                
            
            def get_comment():
                channel_name = channel_selected
                query = {"channel_details.channel_name": channel_name}
                for c_data in col.find(query, {"_id": 0, 'comment_details': 1}):
                    for i in c_data:
                        tuple_values=tuple(c_data[i])
                df=pd.DataFrame(tuple_values)
                
                rows1=[]
                for index in df.index:
                    row= tuple(df.loc[index].values)
                    row = tuple([str(d) for d in row])
                    rows1.append(row)
                query = "insert into comment(video_id,comment_text,comment_author,comment_published_date,comment_id)  values(%s,%s,%s,%s,%s)"
                mycursor.executemany(query,rows1)
                my_sql.commit()
                return "Comment Table Created"
            print(get_comment())

        st.success("Data Migrated Sucessfully")
    
    if buttons == "CHANNELS":
        st.write("**CHANNELS TABLE DISPLAY**")
        st.dataframe(channel_view)
    elif buttons == "VIDEOS":
        st.write("**VIDEO TABLE DISPLAY**")        
        st.dataframe(video_view)
    elif buttons == "COMMENTS":
        st.write("**COMMENT TABLE DISPLAY**")        
        st.dataframe(comment_view)


    
if selected == "Data Analysis":
    st.title("SQL Data Analysis ")
    st.markdown("#    ")

    option = st.selectbox(
    "Select the Quries to be Analysed",
    ("1.What are the names of all the videos and their corresponding channels?",
    "2.Which channels have the most number of videos, and how many videos do they have?",
    "3.What are the top 10 most viewed videos and their respective channels?",
    "4.How many comments were made on each video, and what are their corresponding video names?",
    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
    "8.What are the names of all the channels that have published videos in the year 2022?",
    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10.Which videos have the highest number of comments, and what are their corresponding channels names?"    
     ),
    index=None,
    placeholder="Select the Query...",
    )

    st.write('You selected:', option)

    if option == "1.What are the names of all the videos and their corresponding channels?":
        question_1 = []
        query1= '''select Title as videos,channel_name as channelname from video'''
        mycursor.execute(query1)
        result = mycursor.fetchall()
        for data in result:
            question_1.append(data)
        Q1 = question_1
        df1=pd.DataFrame(Q1,columns=["video title","channelname"])
        st.write(df1)
    elif option == "2.Which channels have the most number of videos, and how many videos do they have?":
        query2 = "select channel_name,count(*) as No_of_videos from video group by channel_name"
        mycursor.execute(query2)
        result = mycursor.fetchall()
        df2=pd.DataFrame(result,columns=["channel name","No of videos"])
        st.write(df2)
        # 3.	What are the top 10 most viewed videos and their respective channels?"
    elif option == "3.What are the top 10 most viewed videos and their respective channels?":
        query3 = '''select channel_name as channelname,Title as Videos,view_count as Most_viewed_Videos from video order by view_count desc limit 10'''
        mycursor.execute(query3)
        result = mycursor.fetchall()
        df3=pd.DataFrame(result,columns=["channel name","Videos","Most Viewed Videos"])
        st.write(df3)        
            
    elif option == "4.How many comments were made on each video, and what are their corresponding video names?":
        query4 = "select Title as Videos,comment_count as Comments_count from video order by comment_count desc"
        mycursor.execute(query4)
        result = mycursor.fetchall()
        df4=pd.DataFrame(result,columns=["Videos","Comments_count"])
        st.write(df4)
    elif option == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5 = "select channel_name as channelname,Title as Videos,Like_count Likecount from video order by Like_count desc"
        mycursor.execute(query5)
        result = mycursor.fetchall()
        df5=pd.DataFrame(result,columns=["channelname","Video","Like counts"])
        st.write(df5)
    elif option == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query6 = "select Title as Videos,Like_count as Like_counts from video order by Like_count  desc"
        mycursor.execute(query6)
        result = mycursor.fetchall()
        df6=pd.DataFrame(result,columns=["Videos","Like_counts"])
        st.write(df6)
    elif option == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
        query7 = "select channel_name as Channel_Names,sum(view_count) as Total_Views from video group by channel_name order by sum(view_count) desc"
        mycursor.execute(query7)
        result = mycursor.fetchall()
        df7=pd.DataFrame(result,columns=["Channel_Names", "Total_Views"])
        st.write(df7)
    elif option == "8.What are the names of all the channels that have published videos in the year 2022?":
        query8 = "select channel_name as Channel_Names,Published_date as Date_of_Release from video where year(Published_date) = 2022"
        mycursor.execute(query8)
        result = mycursor.fetchall()
        df8=pd.DataFrame(result,columns=["Channel_Names", "Date Of Release"])
        st.write(df8)
    elif option == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = "select channel_name as Channel_Names,avg(duration) as Average_Duration from video group by channel_name"
        mycursor.execute(query9)
        result = mycursor.fetchall()
        df9=pd.DataFrame(result,columns=["Channel_Names", "Average_Duration"])
        st.write(df9)
    elif option == "10.Which videos have the highest number of comments, and what are their corresponding channels names?":
        query10 = "select channel_name as Channel_Names,Title as Videos, max(comment_count) as Max_Comment_Counts  from video group by channel_name,Title order by max(comment_count) desc"
        mycursor.execute(query10)
        result = mycursor.fetchall()
        df10=pd.DataFrame(result,columns=["Channel_Names", "Videos","Max Comment Counts"])
        st.write(df10)



