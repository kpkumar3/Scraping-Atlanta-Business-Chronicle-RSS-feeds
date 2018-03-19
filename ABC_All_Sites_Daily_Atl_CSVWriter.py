# -*- coding: utf-8 -*-
"""
Created on Mon Sep 18 10:38:54 2017
Environment: Built on python 2.7
@author: Praneeth
This program 
1. Gets the data from the RSS feeds of Atlanta Business Chronicle.
2. Appends the daily data to abc_daily_data file.
3. Appends articles containing the keyword 'Atlanta' to abc_data_atlanta file. 

Important Note: At any time, RSS feeds page maintains only 10 articles.
This program needs to be at least once in a day to capture all news articles. 

Web Source:
List of all industries for RSS feeds: https://www.bizjournals.com/atlanta/syndication
All headlines feed source: http://feeds.bizjournals.com/bizj_atlanta
Industries: http://feeds.bizjournals.com/industry_'+ industry number'

"""
#Import required libraries. 
#csv is used to write data to csv files
import csv
#pandas is used to view and alter data in structured (tabular) format
import pandas as pd
#From pandas, importing the EmptyDataError to check if the csv file is empty or not.
from pandas.io.common import EmptyDataError
#Import date util parser to parse date and time to required format
from dateutil.parser import parse
#Use requests module to request data from RSS feed website
import requests as r
#Beautiful soup module to parse the HTML/XML data
from bs4 import BeautifulSoup as bs
#infile = 'c:/Users/Praveen/Desktop/GSU/City of Atlanta/ABC scraping/abc_raw_data.csv'
daily_file = 'c:/Users/Praveen/Desktop/GSU/City of Atlanta/ABC scraping/abc_daily_data.csv'
atl_file = 'c:/Users/Praveen/Desktop/GSU/City of Atlanta/ABC scraping/abc_data_atlanta.csv'

####################################################################################
#User defined Functions. Main cotrol does not start here. 
####################################################################################
#This function skips any non-ascii characters - avoids unicode conversion errors.
def Convert(s):
    j=''
    for i in s:
        try:
          j+=str(i)
        except:
          j+=''
    return j

###############################################################################
#This function checks if the o/p file is empty and writes header information.
#When the program is run for the first time in any system, this writes header information.
def Write_Header(list):
    try:
        #check if data is present in file. If it is present, sort the data by date and time and return the latest article Date.
        df=pd.read_csv(daily_file)#,delim_whitespace=True) #delim is not whitespace
        #df.drop_duplicates(subset=['User_Name','Description', 'Headlines'],keep='first', inplace=True)
        #df['Date'] = pd.to_datetime(df.Date)
        df.sort_values(['Date','Time'], axis=0, ascending=False, inplace=True, kind='quicksort', na_position='last')
        df.reset_index(drop=True,inplace=True)
        latest_ID=str(df.iloc[0].ID)
        #df.to_csv(daily_file,index=False)
        #print latest_ID
        return latest_ID
    except EmptyDataError:
    #File is empty. Write header data
        latest_ID = ''
        with open(daily_file, 'ab') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',',quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow(list)
        return latest_ID

###############################################################################
#Main function which gets data from RSS feeds URL and parses the information to required 
#format and writes into the output file. 
def make_soup(url,latest_ID):
    #assign the url to local variable
    my_url = url
    #get the page data using URL
    page = r.get(my_url)
    #from bs4 import BeautifulSoup as bs
    soup=bs(page.content,'html.parser')
    
    #iterate through each article and extract the required information using the tags.
    for article in soup.find_all('item'):
        #11-13-17 changes
        #DataSource = str(unicode(soup.channel.generator.text))       
        DataSource = str(unicode(soup.channel.title.text)).split(' News')[0]
        #11-13-17 changes
        
        Article_ID = 'na'
        User_ID = 'na'
        Screen_Name = 'na'
        
        #Original Source
        #11-13-17 changes
        #Original_Source = str(unicode(soup.channel.title.text))
        Original_Source = 'Atlanta Business Chronicle - RSS feed'
        #11-13-17 changes
        #Language
        Language = str(unicode(soup.channel.language.string))
        if Language == 'en-us':
            Language = 'ENG'
        Zone = 'UTC-05:00' 
        Location = '33.830054,-84.384729'
        City = 'Atlanta'
        State = 'Georgia'
        Country = 'US'
        Share_Count = '0' 
        Favourite_Count = '0' 
        Comment_Count = '0' 

        #author name
        lists=article.text.split('http')
        author=lists[0].split('\n')
        Author_name = author[-2]
    
        #Time and Date
        timestamp = str(unicode(article.pubdate.string))
        dt = parse(timestamp)
        #print dt
        #print type(dt)
        time_split = timestamp.split(' ')
        #print time_split
        #Date = pd.to_datetime(dt.strftime('%m/%d/%Y'))
        news_date = dt.strftime('%Y-%m-%d')
        Time = time_split[4]
        Article_ID = news_date + ' ' + Time
        #print  type(Article_ID)
        #print  type(news_date)
        #print  type(latest_ID)
        Article_ID = str(parse(Article_ID))
        news_date = str(dt.strptime(news_date, '%Y-%m-%d').date())
        latest_ID = str(parse(latest_ID))
        #print Article_ID, news_date, latest_ID

        #print str(news_date), Article_ID
        #zone = time_split[5]

        #URL
        URL = article.link.string 
    
   
        #Complete text
        Text=' '

        #Headline
        Headlines = (unicode(article.title.text))
        #Headlines = str(unicode(article.title.text)) + '@!@,'
        #Headlines=Headlines.replace('\n','')
        Headlines=" ".join(Headlines.split())
    
        #Text
        st=article.description.string
        st1=bs(st)
        Description=st1.get_text() 
        #Text=Text.replace('\n','')
        Description=' '.join(Description.split())
        csv_data = []
        #if the article Date is greater than the latest article Date from the csv file, write article data to the files. 
        if Article_ID >= latest_ID:#Add a code to check presence of Atlanta in headlines/text
            csv_data = [#str(Convert(DataSource)), Article_ID, Convert(User_ID), 
                        str(Convert(DataSource)), parse(Article_ID), Convert(User_ID),
                        Convert(Screen_Name), Convert(Author_name), Convert(Original_Source), 
                        #Convert(Language), Convert(Time) , news_date , Convert(Zone), 
                        Convert(Language), Convert(Time) , dt.strptime(news_date, '%Y-%m-%d').date() , Convert(Zone), 
                        Location, City, State, Country, Share_Count, Favourite_Count, 
                        Comment_Count, Convert(URL), Convert(Description), Convert(Headlines), 
                        Convert(Text)]
            #writes the data to daily file
            with open(daily_file, 'ab') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter=',',quoting=csv.QUOTE_MINIMAL)
                csvwriter.writerow(csv_data)
            #writes the data to daily file only if the key word 'atlanta' is present in description or headlines   
            if 'atlanta' in str.lower(Convert(Description + Headlines)):
                with open(atl_file, 'ab') as csvfile2:
                    csvwriter = csv.writer(csvfile2, delimiter=',',quoting=csv.QUOTE_MINIMAL)
                    csvwriter.writerow(csv_data)    
###############################################################################
#PROGRAM STARTS HERE
###############################################################################
#Common URL to get RSS feed from dufferent industries.    
comm_url = "http://feeds.bizjournals.com/industry_"
#Main URL to get all headlines.
main_url = "http://feeds.bizjournals.com/bizj_atlanta"
#Header information. Program gets info from RSS feeds and writes them to the output file with the below order.
csv_data = ['Data Source','ID','User_Id','Screen_Name','User_Name','Original Source','Language','Time','Date','Time_Zone','Location','City','State','Country','Share_Count','Favorite_Count','Comment_Count','URL','Description','Headlines','Text']

#Call Write_Header function to write header data (if the program is run for the first time)
#If the data already exists in file, get the date of latest article. 
latest_ID=Write_Header(csv_data)
csv_data = ''

#Pass the main URL, latest article ID to make_soup to extract feed from main URL.
#Refer the function for more functional details.
make_soup(main_url,latest_ID)

#List all the industries to extract data from. 
#Attach each number in the list to common URL to get link of each industry.
#Iterate through each URL and extract the data.
a=[2,5,6,7,8,10,11,12,14,17,18,19,20,21,22,23,24]
for page in a:
    my_url=comm_url + str(page)
    make_soup(my_url,latest_ID)

#Read the atlanta data file
df=pd.read_csv(atl_file)#,delim_whitespace=True) #delim is not whitespace
#Remove duplicates from the file based on author, description and headlines
df.drop_duplicates(subset=['User_Name','Description', 'Headlines'],keep='first', inplace=True)
#Convert date and time to yyyy-mm-dd format
df['Date'] = pd.to_datetime(df.Date)
#Combine date and time to form unique ID
df['ID'] = df.apply(lambda x: x.Date.strftime('%Y-%m-%d') + ' ' + x.Time, axis=1)
df['ID'] = pd.to_datetime(df.ID)
#sort news articles in descending order on date and time
df.sort_values(['Date','Time'], axis=0, ascending=False, inplace=True, kind='quicksort', na_position='last')
df.reset_index(drop=True,inplace=True)
#Write the data to CSV file without index
df.to_csv(atl_file,index=False)

#Read the atlanta data file
df=pd.read_csv(daily_file)#,delim_whitespace=True) #delim is not whitespace
#Remove duplicates from the file based on author, description and headlines
df.drop_duplicates(subset=['User_Name','Description', 'Headlines'],keep='first', inplace=True)
#Convert date and time to yyyy-mm-dd format
df['Date'] = pd.to_datetime(df.Date)
#Combine date and time to form unique ID
df['ID'] = df.apply(lambda x: x.Date.strftime('%Y-%m-%d') + ' ' + x.Time, axis=1)
df['ID'] = pd.to_datetime(df.ID)
#sort news articles in descending order on date and time
df.sort_values(['Date','Time'], axis=0, ascending=False, inplace=True, kind='quicksort', na_position='last')
df.reset_index(drop=True,inplace=True)
#Write the data to CSV file without index
df.to_csv(daily_file,index=False)
