#  This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by    the Free Software Foundation, either version 3 of the License, or (at your option) any later version.This program is distributed in the hope that it will be useful,but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#Ver 12 Road Analyzer Written by Andrea Ciufo andreaciufo [a/t] lovabledata.com 

#Ver 1.1 modified how the gps dataframe is extracted, in this case we use .iloc[]
#removed all the acceleration manipulation for a better undestanding why we have different
#Ver2.0 Added the distance traveled:
#progressive distance: from the initial point to the n-th row
#partial distance:from the n-th-1 to the n row
# % of the total travel distance recorded 
#3.0 Added a driving scorecard model
#12 Corrected spelling errors and continued cleaning the code from unnecessary parts
#the csv contained in the anlysis contained the same informations but 
	#acceleration in some files was expressed in terms of g in ohter m/s^2
	#there was some little differences in the columns header,this differences required to clean the header before computing our variables of interest

import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

import glob
#insert the correct path where the data are contained 
#for example C:\python/your/PATH/*.csv 
gps_record=pd.DataFrame()
speed_df_after_peak=pd.DataFrame()
speed_df_bef_peak=pd.DataFrame()
maxb=[]
path = "C:\python/your/path/*.csv"

#this function will be used to calculate the traveled distance from two
#adjacent gps coordinates

def haversine(lat1, long1, lat2, long2):
    r = 6371  # radius of Earth in km
    # haversine formula
    lat = lat2 - lat1
    lon = long2 - long1
    a = np.sin(lat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(lon/2)**2
    c = 2*np.arcsin(np.sqrt(a))
    d = r*c
    return d

#looping through files in the folder 
for fname in glob.glob(path):

	csvname=fname
#reading the csv and creating the dataframe 
	dataframe_raw=pd.read_csv(csvname,sep=',',header=0,skipfooter=0, index_col=False)
#cleaning the columns, this way all the file will have the same columns
	dataframe_raw.columns=['timestamp','type','lat','lon','height','accuracy','speed','bearing','x','y','z']
	
#calculating the accelaration absolute value
	dataframe_raw['abs']=np.sqrt(dataframe_raw['x']**2+dataframe_raw['y']**2+dataframe_raw['z']**2)
#adding the path name of the csv file to the dataframe 
	dataframe_raw['file']=fname 
#calculating the difference between each record to calculate sampling frequency
	dataframe_raw['diff']=dataframe_raw['timestamp'].diff()
#calculating the difference between the last and the first time stamp in the record
	start_trip=dataframe_raw['timestamp'].iloc[0]
	end_trip=dataframe_raw['timestamp'].iloc[-1] 
	
	travel_time=(end_trip-start_trip)/(1000) #in seconds
	
	#converting seconds in h - m -s for a better human readibility
	m, s = divmod(travel_time, 60)
	h, m = divmod(m, 60)
	#adding the travel time information to the dataframe 
	dataframe_raw['travel_time']=travel_time
	
	#expressing the % cumulated travel time 
	dataframe_raw['%traveled_time']=(((dataframe_raw['timestamp']-start_trip)/1000)/travel_time)*100
	#identifying if the acceleration is expressed in m/s^2 or in g
	dataframe_raw['%remaing']= 100-dataframe_raw['%traveled_time']
	if not dataframe_raw['abs'].max()<20: 
		dataframe_raw[['x','y','z','abs']]=dataframe_raw[['x','y','z','abs']]/9.81
	if not dataframe_raw['abs'].max()<20: 
		dataframe_raw[['x','y','z','abs']]=dataframe_raw[['x','y','z','abs']]/10
	#Identifying the row with the max absolute acceleration value 
	max_row=dataframe_raw['abs'].idxmax()
	#Selecting all the data in the dataframe row where max abs acceleration values is contained 
	max_peak=dataframe_raw.iloc[max_row].copy()
	#Calculating the median sampling frequency for each file 
	#as the difference between each timestamp and then calculating the median
	max_peak['median']=dataframe_raw['diff'].median() 	
	#identify and extract all gps records inside the file
	gps= dataframe_raw.index[dataframe_raw['type']=="gps"]
	#Creating a copy of the dataframe containing only the gps information 
	#of all the travell, then it will be filterd and will contain only the information collected after the max peak 
	gps_temp=dataframe_raw.iloc[(gps)].copy()
	#Calculating median and mean of the travel time 
	#Is intersting (future work to do) to know and divide in the future this value it travel time chunks
	gps_temp['average gps speed']=(gps_temp['speed'].mean())*1.852 #km/h
	gps_temp['median gps speed']=(gps_temp['speed'].median())*1.852 #km/h
	#Calculating the max and min speed values recorded in all the journey
	gps_temp['max gps speed']=(gps_temp['speed'].max())*1.852 #km/h
	gps_temp['min gps speed']=(gps_temp['speed'].min())*1.852 #km/h
	#Converting GPS coordinates in radians
	gps_temp['lat_rad']=gps_temp['lat'].apply(lambda x: np.radians(x))
	gps_temp['lon_rad']=gps_temp['lon'].apply(lambda x: np.radians(x))
	#creating an empty array then calculating the travelled partial distance
	#between two adjacent point 
	gps_temp['partial distance']=np.nan
	#travelled partial distance 
	gps_temp['partial distance']=haversine(gps_temp['lat_rad'],gps_temp['lon_rad'], gps_temp['lat_rad'].shift(),gps_temp['lon_rad'].shift())*1000
	#Calculating the progressive distance at each time stamp
	gps_temp['progressive_distance']=gps_temp['partial distance'].cumsum()
	#Calculating the % progressive distance at each time stamp
	gps_temp['% travel space']=gps_temp['progressive_distance']/gps_temp['partial distance'].sum()*100
	#selecting the first gps log after the max acceleration peak recorded 
	#based on a Logical condition 
	#gps_temp['timestamp']>max_peak['timestamp']
	gps_temp_greater=gps_temp[gps_temp['timestamp']>max_peak['timestamp']]
	print('gps_temp_greater',gps_temp_greater.head(3))
	#Transforming a gps_temp-greter to a dataframe 
	gps_peak=pd.DataFrame(gps_temp_greater)
	#selecting the first row containing the gps information after the max acceleration peak
	gps_peak=gps_peak.iloc[0]
	#Adding to the dataframe containing the max accelaration peak information the first gps data after the peak 
	max_peak['% travel space']=gps_peak['% travel space']
	max_peak['progressive_distance']=gps_peak['progressive_distance']
	#speed after the peak is a dataframe computed to analyze the speed trend over the time after the peak 	
	speed_after_peak=dataframe_raw.iloc[max_row:].copy()
	speed_after_peak=pd.DataFrame(speed_after_peak)
	#calculating speed mean and median after the peak
	speed_after_peak['average gps speed_ap']=(speed_after_peak['speed'].mean())*1.852 #km/h
	speed_after_peak['median gps speed_ap']=(speed_after_peak['speed'].median())*1.852 #km/h
	#calculating max and min peak after the peak 
	speed_after_peak['max gps speed_ap']=(speed_after_peak['speed'].max())*1.852 #km/h
	speed_after_peak['min gps speed_ap']=(speed_after_peak['speed'].min())*1.852 #km/h
	#this dataframe will study speed trend before the max accelaration peak
	#this way it will be possible to compare the driving behaviour before and after the peak 
	speed_before_peak=dataframe_raw.iloc[:max_row].copy()
	speed_before_peak=pd.DataFrame(speed_before_peak)
	speed_before_peak['average gps speed_bp']=(speed_before_peak['speed'].mean())*1.852 #km/h
	speed_before_peak['median gps speed_bp']=(speed_before_peak['speed'].median())*1.852 #km/h
	speed_before_peak['max gps speed_bp']=(speed_before_peak['speed'].max())*1.852 #km/h
	speed_before_peak['min gps speed_bp']=(speed_before_peak['speed'].min())*1.852 #km/h
	#adding the speed information after and before the peak to the max_peak dataframe 
	max_peak['median gps speed_ap']=speed_after_peak['median gps speed_ap'].iloc[0]
	max_peak['median gps speed_bp']=speed_before_peak['median gps speed_bp'].iloc[0]
	max_peak['max gps speed_ap']=speed_after_peak['max gps speed_ap'].iloc[0]
	max_peak['max gps speed_bp']=speed_before_peak['max gps speed_bp'].iloc[0]
	max_peak['min gps speed_ap']=speed_after_peak['min gps speed_ap'].iloc[0]
	max_peak['min gps speed_bp']=speed_before_peak['min gps speed_bp'].iloc[0]
	#we append gps and speed after before peak for further analysis, we don't know if we will use in further development, we don't know that
	gps_record=gps_record.append(gps_temp)
	speed_df_after_peak=speed_df_after_peak.append(speed_after_peak)
	speed_df_bef_peak=speed_df_bef_peak.append(speed_before_peak)
	maxb.append(max_peak)
#converting to a more clean pandas dataframe our dataframe 
#
#TO DO: understanding why this step is needed 	
#
speed_df_after_peak=pd.DataFrame(speed_df_after_peak)
speed_df_bef_peak=pd.DataFrame(speed_df_bef_peak)
maxa=pd.DataFrame(maxb)
#translating in human readable string the timestamp 
#of the max acceleration peak 
maxa['h_timestamp']	= pd.to_datetime(maxa['timestamp'], unit='ms')	
maxa['%(space-time)']=maxa['% travel space']-maxa['%traveled_time']
#Adding th "Bad Driving Scorecard" column to maxa dataframe 
maxa['%Bad Driving Scorecard']=0
maxa['event']=np.nan
#This is the filtering part of the code
#Here is developed a driving behavior classification system  
#First filter: separate all the the records with a>3g
filter_1_layer=(maxa['abs']>3)
#Second Filter layer: Clustering the records in three subsets
#Second Filter: subset with 3<a<5
filter_2_layer_AB=(maxa['abs']>3) & (maxa['abs']<5)
#Second Filter: subset with a>5
filter_2_layer_CD=(maxa['abs']>5)
#Second Filter: subset with a<3
filter_2_layer_EF=(maxa['abs']<3)
#Third Filter: create new subsets
#Third Filter: clustering analysis based on 
#The difference between the % of the space traveled until the max acceleration peak and the % of time recorded until the max acceleration peak 
#
#Third Filter: ['%(space-time)']>30
filter_3_layer_A=(maxa['abs']<5) &(maxa['abs']>3)& (maxa['%(space-time)']>30)
#Third Filter: ['%(space-time)']<30
filter_3_layer_B=(maxa['abs']<5) &(maxa['abs']>3)& (maxa['%(space-time)']<30)
#Third Filter: ['%(space-time)']<25 strictly condition for a>5 
filter_3_layer_C=(maxa['abs']>5) & (maxa['%(space-time)']<25)
filter_3_layer_D=(maxa['abs']>5)& (maxa['%(space-time)']>25)
#Third Filter: ['%(space-time)']>25 and travel time >60 
filter_3_layer_E=(maxa['abs']<3)& (maxa['%(space-time)']>25) & (maxa['% travel space']>60)
#Fourth filter, the last filter, here we define the scorecard for each branch of our "decision tree"
# In some case nothing change from the previous case, in other we use the %of travel space and the comparison of the speed trend before and after the peak 
#to indentify the type of accident
#An error in the nomenclature need to be fixed
#
#B1 Probably accident but a more granular analysis on acceleration is needed
#B1 a >4g 
filter_4_layer_B1=(maxa['abs']<5) & (maxa['abs']>4)&(maxa['%(space-time)']<15)&(maxa['% travel space']>95)
#B2 medium accident a is < 4 g because we have the similar condition above but less space and time was traveled
filter_4_layer_B2=(maxa['abs']<4) & (maxa['abs']>3)&(maxa['%(space-time)']>15)
#b3 not possible to identify
filter_4_layer_B3=(maxa['abs']<4) & (maxa['abs']>3)&(maxa['%(space-time)']<15)
#b4 no accident
#E1 low gravity accident
filter_4_layer_E1=(maxa['abs']<3)& (maxa['%(space-time)']>25) & (maxa['% travel space']>60)
#E0 no accident probably potholes on the road 
filter_4_layer_E0=(maxa['abs']<3) & (maxa['% travel space']>60)&(maxa['median gps speed_ap']<maxa['median gps speed_bp'])
#E2 no accident probably potholes on the road 
filter_4_layer_E2=(maxa['abs']<3)&  (maxa['% travel space']>60)&(maxa['median gps speed_ap']>(maxa['max gps speed_bp']))
#Assigning a scorecard for every case identified
#NOTE scoring value are arbitrary
maxa.loc[filter_1_layer,'%Bad Driving Scorecard'] +=1 	
maxa.loc[filter_2_layer_AB,'%Bad Driving Scorecard'] +=1
maxa.loc[filter_2_layer_CD,'%Bad Driving Scorecard']+=2
maxa.loc[filter_3_layer_A,'%Bad Driving Scorecard'] +=3
maxa.loc[filter_3_layer_C,'%Bad Driving Scorecard'] +=4
maxa.loc[filter_3_layer_D,'%Bad Driving Scorecard'] +=3
maxa.loc[filter_4_layer_B1,'%Bad Driving Scorecard'] +=1
maxa.loc[filter_4_layer_B2,'%Bad Driving Scorecard'] +=1.5
maxa.loc[filter_4_layer_B3,'%Bad Driving Scorecard'] +=0.5
maxa.loc[filter_4_layer_E1,'%Bad Driving Scorecard'] +=1
maxa.loc[filter_4_layer_E0,'%Bad Driving Scorecard'] +=0.3
#Assigning a event name output for every case identified	
maxa['event'] = 'no accident'
maxa.loc[filter_2_layer_CD,'event']='severe accident type d'
maxa.loc[filter_3_layer_A,'event'] ='medium accident'
maxa.loc[filter_3_layer_C,'event'] ='severe accident type c'
maxa.loc[filter_4_layer_B1,'event'] = 'prob med accident'
maxa.loc[filter_4_layer_B2,'event'] = 'medium accident'
maxa.loc[filter_4_layer_B3,'event'] = 'prob no medium accident '
maxa.loc[filter_4_layer_E0,'event'] ='prob no low accident'
maxa.loc[filter_4_layer_E1,'event'] ='low severity accident'
#Printing specific columns of the dataframe containing the informations of the max acceleration peak for each file in the folder for a visual data comparison 
print(maxa[['abs','%traveled_time','% travel space','%(space-time)','median gps speed_ap','median gps speed_bp','max gps speed_ap','max gps speed_bp','min gps speed_ap','min gps speed_bp','%Bad Driving Scorecard']].sort_values(by=['abs'], ascending=False))
#Printing specific columns of the dataframe containing the informations of the max acceleration peak for each file in the folder for a visual data comparison 
print(maxa[['abs','h_timestamp','file','event','%Bad Driving Scorecard']].sort_values(by=['abs'], ascending=False))
#Printing specific columns of the dataframe containing the informations of the max acceleration peak for each file in the folder for a visual data comparison 
print(maxa[['abs','event','%Bad Driving Scorecard']].sort_values(by=['abs'], ascending=False))
#Printing specific columns of the dataframe containing the informations of the max acceleration peak for each file in the folder for a visual data comparison 
print(maxa[['abs','event']].sort_values(by=['abs'], ascending=False))
#Saving the max_peak dataframe in the analyzed files folder 
maxa.to_csv('roadreport.csv')
#Plotting the correlation between % the traveled time and % the traveled space
#when the max peak was recorded
plt.scatter(maxa['%traveled_time'],maxa['% travel space'])
plt.title('% time traveled and % space traveled at the time when the acceleration peak is recorded ')
plt.xlabel('%traveled_time')
plt.ylabel('% travel space')
plt.show()

	
