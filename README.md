# YoutuYoutube-Data-Harvesting-and-Warehousing

# Introduction

This project is about building a streamlit application used for extracting data of youtube channels via youtube API, storing extracted data in MongoDB Atlas,Fetch data from MongoDB Atlas and store in MySQL database in structured format.The data stored in MySQL database is used for Data Analysis vide SQL Quries.

# Domain
Social Media

# Technologies used
Python 3.11.5

MySQL 8.0.34

MongoDB Atlas

Streamlit

Youtube API

# Features of Application
Retrieve details from YouTube

The channel ID of a YouTube channel needs to be provided as input. The details of the channel such as channel details, video details, and comments details  will be retrieved from YouTube using YouTube API.

The Channel information, Video Information, and Comment information of the respective channel will be displayed after having extracted the data.

# Upload to MongoDB Atlas
By clicking the ‘Upload to MongoDB’ button in the application, the retrieved details are stored in MongoDB Atlas, a cloud-based NoSQL database that stores data in JSON documents.

Every channel is stored as a separate collection in MongoDB Atlas.

The code has been written in such a way that if it tries to upload the same data already available in MongoDB, It will pop up  the message as "Channel Already Exists" and won't upload the same again.

If it is not so, the data will be uploaded.


# Migrate to MySQL Database

The list of channels available in MongoDB Atlas can be selected in application.By clicking ‘Submit’ button, the selected channel details are fetched from MongoDB Atlas and stored in MySQL Database.

# Analysis using SQL
By using details of channels stored in MySQL database, the results are derived using MySQL queries for 10 questions about the youtube channels. The results are displayed in streamlit application in form of tables.

Requirements
google-api-python-client ==2.102.0

streamlit==1.27.2

streamlit-option-menu==0.3.6
pymongo==4.5.0

pandas==2.1.1

mysql-connector-python==8.1.0
