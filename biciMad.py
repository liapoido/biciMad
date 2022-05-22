#!/usr/bin/env python
# coding: utf-8

# # BiciMAD
# Practica de Sandra Fjelkestam, Greg Coletti, Lia Poidomani
# 
# We are about to analyse the dataset kindly offered by BiciMad at this link
# https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales-(1). 
# 

# In[29]:


from pyspark import SparkContext
import matplotlib.pyplot as plt
import statistics as stats
import json
from tabulate import tabulate

sc = SparkContext()


# First we take a look at the structure of the data

# In[6]:


get_ipython().system('head -1 201812_Usage_Bicimad.json')


# The data comes in the structure above and each column in the database is decribed by the foloowing way. 
# 
# * _id: identification of the ride
# 
# 
# * user_day_code: the user code. For same date, all the rides have the same code.
# 
# 
# * idunplug_station: id number of the origin station
# 
# 
# * idunplug_base: id number of origin base (a base is the "parking spot" for bike at the station)
# 
# 
# * idplug_station: id number of destination station
# 
# 
# * idplug_base: id number of destination base
# 
# 
# * unplug_hourTime: time the bike is unpluged
# 
# 
# * travel_time: time in seconds of the bikeride
# 
# 
# * user_type: type of user. 
#     it is defined as:
#      - 0: not defined
#      - 1: anual user
#      - 2: ocasional user
#      - 3: worker of bicimad
# 
# 
# * ageRange: age range of the user.
#     it is defined as
#         - 0: not defined
#         - 1: between 0 and 16 years
#         - 2: between 17 and 18 years
#         - 3: between 19 and 26 years
#         - 4: between 27 and 40 years
#         - 5: between 41 and 65 years
#         - 6: more than 65 years
# 
# 
# * zip_code: postal code of the user

# Now we create the a mapper function to get a dictionary

# In[7]:


rdd = {} #dictionary


# In[8]:


def mapper(line):
    data = json.loads(line)
    #oid = data['oid']
    user_day_code = data['user_day_code']  
    idplug_base = data['idplug_base']
    user_type = data['user_type']
    idunplug_base = data['idunplug_base']
    travel_time = data ['travel_time']   
    start_station = data['idunplug_station']
    age = data['ageRange']
    end_station = data['idplug_station']
    year = data['unplug_hourTime']['$date'][0:4]
    month = data['unplug_hourTime']['$date'][5:7]
    day = data['unplug_hourTime']['$date'][8:10]
    hour = data['unplug_hourTime']['$date'][11:19]
    zip_code = data['zip_code']

    return {'user_day_code': user_day_code,
            'idplug_base': idplug_base,
            'user_type': user_type,
            'idunplug_base': idunplug_base,
            'travel_time': travel_time,
            'start_station': start_station,
            'end_station': end_station,
            'age': age,
            'year': year,
            'month': month,
            'day': day,
            'hour': hour,
            'zip_code': zip_code}


# In[9]:


def file_name(i): #to get the file name
    if len(str(i)) == 1:
        file = '20180' + str(i)  + '_Usage_Bicimad.json'
    else:
        file = '2018' + str(i) + '_Usage_Bicimad.json'
    return file
    


# we create a RDD dataset

# In[30]:


rdd['2018'] = sc.emptyRDD()
file_list = []
for i in range(1,13):
    file_list.append(file_name(i))
     
print(file_list)
for filename in file_list:
    #We refer to the file with this format YYYYMM
    name = filename.split("_")[0]
    rdd[name] = sc.textFile(filename).map(mapper)
    rdd['2018'] = rdd['2018'].union(rdd[name])
    #DEBUG starts
    print(name)
    #DEBUG ends


# In[17]:


#the stucture of our dataset
rdd['2018'].filter(lambda x: x['hour']=='18:00:00').take(1)


# 
# 
# Now we do the same for the station's data. First we peek at the structure

# In[14]:


get_ipython().system('head -1 Bicimad_Stations_201812.json')

#!head -1 Bicimad_Estacions_201807.json #DOES NOT WORK


# In[117]:


rdd_station = {} #dictionary


# In[118]:


def mapper_station(line):
    data = json.loads(line)
    activate = data['stations'][0]['activate']  
    name = data['stations'][0]['name']
    reservations_count = data['stations'][0]['reservations_count']
    light = data['stations'][0]['light']
    total_bases = data['stations'][0]['total_bases']   
    free_bases = data['stations'][0]['free_bases']
    number = data['stations'][0]['number']
    longitude = data['stations'][0]['longitude']
    no_available = data['stations'][0]['no_available']   
    address = data['stations'][0]['address']
    latitude = data['stations'][0]['latitude']
    dock_bikes = data['stations'][0]['longitude']
    iid = data['stations'][0]['id']
    
    return {#'activate': activate,
            'name': name,
            'reservations_count': reservations_count,
            'light': light,
            'total_bases': total_bases,
            'free_bases': free_bases,
            'number': number,
            'longitude': longitude,
            'no_available': no_available,
            'address': address,
            'latitude': latitude,
            'dock_bikes': dock_bikes,
            'id':iid}


# In[119]:


rdd_station['2018'] = sc.emptyRDD()
file_list = ['Bicimad_Stations_201812.json']
#for i in range(1,13):
#    file_list.append(file_name(i))
#file_list[0] = 'Bicimad_Stations_201812.json'

for filename in file_list:
    #We refer to the file with this format YYYYMM
    name = filename.split('_')[2].split('.')[0]
    rdd_station[name] = sc.textFile(filename).map(mapper_station)
    rdd_station['2018'] = rdd_station['2018'].union(rdd_station[name])
    #DEBUG starts
    print(name)
    #DEBUG ends


# In[120]:


#the stucture of our dataset
rdd_station['2018'].take(10)


# ## Análisis del tipo de usuarios
# In this section we analyse the variation among the different kind of users during the same period as before. BiciMAD saves the users in the following way:
# 
# 1: annual membership user
# 
# 2: occasional user
# 
# 3: BiciMAD employee 
# 

# In[15]:


#Separate by user type
user_data = rdd['2018'].map(lambda x: x['user_type']).countByValue()
user_data


# In[162]:


288111/59990


# In[122]:


plt.title('Distribution of user types in 2018')
names = list(user_data.keys()) 
values = list(user_data.values())
plt.bar(names, values, 1)
plt.xlabel('User type', fontsize=18)
plt.ylabel('Number of users', fontsize=16)
names.sort()
plt.xticks(names, ["0","1","2","3"])
plt.show()


# In[168]:


user_data.values()


# As expected, the most common user is the type 1. Surprinsingly not many people use it occasionaly, the employees are 4.8026 times more frequente than the casual users.

# ## Most popular stations
# Both start and end stations.

# In[123]:


#Separate by start station
start_station = rdd['2018'].map(lambda x: x['start_station']).countByValue()
start_data = sorted(dict(start_station).items(), key=lambda x:x[1], reverse=True)


# In[81]:


start_data


# In[124]:


#Separate by end station
end_station = rdd['2018'].map(lambda x: x['end_station']).countByValue()
end_data = sorted(dict(end_station).items(), key=lambda x:x[1], reverse=True) 


# In[18]:


end_data


# In[128]:


start_table = [['Start station', 'No. of trips'],
         [start_data[0:10][0][0], start_data[0:10][0][1]],
         [start_data[0:10][1][0], start_data[0:10][1][1]],
         [start_data[0:10][2][0], start_data[0:10][2][1]],
         [start_data[0:10][3][0], start_data[0:10][3][1]],
         [start_data[0:10][4][0], start_data[0:10][4][1]],
         [start_data[0:10][5][0], start_data[0:10][5][1]],
         [start_data[0:10][6][0], start_data[0:10][6][1]],
               
         [start_data[0:10][7][0], start_data[0:10][7][1]],
         [start_data[0:10][8][0], start_data[0:10][8][1]],
         [start_data[0:10][9][0], start_data[0:10][9][1]]]
print(tabulate(start_table))


# In[129]:


end_table = [['End station', 'No. of trips'],
         [end_data[0:10][0][0], end_data[0:10][0][1]],
         [end_data[0:10][1][0], end_data[0:10][1][1]],
         [end_data[0:10][2][0], end_data[0:10][2][1]],
         [end_data[0:10][3][0], end_data[0:10][3][1]],
         [end_data[0:10][4][0], end_data[0:10][4][1]],
         [end_data[0:10][5][0], end_data[0:10][5][1]],
         [end_data[0:10][6][0], end_data[0:10][6][1]],
         [end_data[0:10][7][0], end_data[0:10][7][1]],
         [end_data[0:10][8][0], end_data[0:10][8][1]],
         [end_data[0:10][9][0], end_data[0:10][9][1]]]
print(tabulate(end_table))


# In[130]:


#get the name of each station with help from rdd_stations
station_info = rdd_station['2018'].map(lambda x: x['id']==end_data[0][0]).countByValue()
list(station_info.keys())


# In[ ]:





# They are the same for both end stations and start stations. 
# 
# 

# ## Analysis of monthly travels
# 

# In[174]:


list_month=['01','02','03','04','05','06','07','08','09','10','11','12']

daily_trips = rdd['2018'].map(lambda x: x['month']).countByValue()
for month in list_month:
    rdd_aux = rdd['2018'].filter(lambda x: x['month']==month)
    total_day = len(list(rdd_aux.map(lambda x: x['day']).countByValue()))
    daily_trips[month] = daily_trips[month]/total_day

print(daily_trips)


# In[181]:


l = list(daily_trips.values())


# In[216]:


winter = sum(l[0:3]) #jan, feb, march
spring = sum(l[3:6]) #april, may, june
summer = sum(l[6:9]) #july, ,august, september
autumn = sum(l[9:12]) #october, november, december


# In[218]:





# In[219]:


plt.figure(figsize=(8,10))
plt.title('')
names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dic'] 
values = list(daily_trips.values())
plt.bar(names, values, width=0.7)
plt.xlabel('month', fontsize=18)
plt.ylabel('average daily rides per month', fontsize=16)
plt.show()


# Aa we can notice, the service is most used during the warm and dry months. Exception is made for august, the holiday month. March is the least popular month for bikes, probably due to the rain as these data confirms https://it.weatherspark.com/h/y/36848/2018/Condizioni-meteorologiche-storiche-durante-il-2018-a-Madrid-Spagna#Figures-Summary

# In[220]:


plt.figure(figsize=(8,10))
plt.title('')
names = ["spring", "summer", "autumn", "winter"]
values = [spring, summer, autumn, winter]
plt.bar(names, values, width=0.7)
plt.xlabel('season', fontsize=18)
plt.ylabel('average daily rides per season', fontsize=16)
plt.show()


# In[221]:



labels = 'spring', 'summer', 'autumn', 'winter'
sizes = [spring, summer, autumn, winter]
explode = (0, 0.05, 0.08, 0)  # only "explode" the 2nd slice (i.e. 'Hogs')
plt.figure(figsize=(6,6))
plt.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
        shadow=True, startangle=90, radius=1.4)
plt.show()


# As the graph reveals, summer is the most popular season for cycling in the city. Accordingly to common sense, the cold and wet seasons are a big deterrent for bikers.

# ## Analysis between day/night
# 
# here we are analysing when the users prefer riding
# 
# 
# 

# In[44]:


#Check between day and night
def find_night(data):
    hour_start = 18
    hour_mid1 = 0
    hour_mid2 = 23
    hour_end = 6
    
    current_hour = int(data['hour'][0:2])
    
    curret_hour = 3
    
    if current_hour < hour_end and current_hour >= hour_mid1: #night
        return True
    elif current_hour > hour_start and current_hour <= hour_mid2: #night
        return True
    elif current_hour > hour_end and current_hour <= hour_start: #day
        return False


# In[45]:


#Find the data for the nights
rdd_night = rdd['2018'].filter(lambda x: find_night(x))


#Count the number of users during the night
#night_data = rdd_night.map(lambda x: x['user_type']).count()
rdd_night.count()


# In[46]:


#Find the data for the days
rdd_day = rdd['2018'].filter(lambda x: not(find_night(x))) #we do not so true-->false

#Count the number of users during the day
#day_data = rdd_day.map(lambda x: x['user_type']).count()
day_count = rdd_day.count()


# In[ ]:


night_count
day_count


# In[78]:


#Plotting the figure
plt.figure(figsize=(8,10))
plt.title('Distribution of no. of rides during day/night')
names = ['Day', 'Night']#list(user_data.keys()) 
values = [day_data,night_data] #list(user_data.values())
plt.bar(names, values, width=0.7)
plt.xlabel('Time of day', fontsize=18)
plt.ylabel('Number of users', fontsize=16)
names.sort()
plt.xticks(names, ["Day","Night"])
plt.show()


# CONCLUSIONS,
# 
# As we expected, there are more rides in the day than in the night. We can see that during the day there are more than 2000000 rides, but at night there are less than 1500000.
# 
# Now we are looking to se how the distribution of the user type changes during the day and night. We use the rdd_day/rdd_night that we created before to filter into day and night.

# In[49]:


#Separate by user type during day
day_user = rdd_day.map(lambda x: x['user_type']).countByValue()
day_user


# In[47]:


#Separate by user type during night
night_user = rdd_night.map(lambda x: x['user_type']).countByValue()
night_user


# In[52]:


#Plotting the figure
plt.title('    Distribution of no. of rides during day/night and user')
names = ['type0 D', 'type1 D', 'type2 D', 'type3 D', 'type0 N', 'type1 N', 'type2 N', 'type3 N']


#total users
values = list(day_user.values()) + list(night_user.values())
plt.bar(names, values, width=1)
plt.xlabel('User type and time of day', fontsize=20)
plt.ylabel('Number of users', fontsize=16)
plt.show()

#procentage
value_day = [x/day_data for x in list(day_user.values())]
value_night= [x/night_data for x in list(night_user.values())]
values = value_day+value_night
plt.bar(names, values, width=1)
plt.xlabel('User type and time of day', fontsize=20)
plt.ylabel('Percentage of users', fontsize=16)
plt.show()


# As we mentioned previously, there are more rides in the day than in the night, so we are going to focus here on the percetage rather than the total number of riders. As we can see, type 0 (not defined type) is has the greatest percentage in the day and in the night (more or less 90%), whereas type 2 (occasional users) has a percentage of less than 10%.
# 
# 

# Now we are going to compare the ages of the bike users and compare them to see if there is any difference between day and night.

# In[41]:


#Separate by age range
age_data = rdd['2018'].map(lambda x: x['age']).countByValue()
age_data


# In[ ]:





# 
# 
# 
# Now we are going to compare the length of the bike rides and compare them to see if there is any difference between day and night.

# In[125]:


#Separate by age range
age_data = rdd['2018'].map(lambda x: x['age']).countByValue()
age_data = sorted(dict(age_data).items(), key=lambda x:x[0])
age_data = [age_data[0][1], age_data[1][1], age_data[2][1], age_data[3][1], age_data[4][1], age_data[5][1], age_data[6][1]]


# In[139]:


#Plotting the figure
plt.title('Distribution of the age')
names = ['0','1','2','3','4','5','6']
values = age_data
plt.bar(names, values)
plt.xlabel('Age range', fontsize=18)
plt.ylabel('Number of users', fontsize=16)
names.sort()
plt.xticks(names, ['0','1','2','3','4','5','6'] )
plt.show()


# In[ ]:


#Separation during the day with age range
day_age = rdd_day.map(lambda x: x['age']).countByValue()


# In[ ]:


#Separation during the night with age range
night_age = rdd_night.map(lambda x: x['age']).countByValue()


# In[ ]:


#Plotting the figure
plt.figure(figsize=(12,8))
plt.title('Distribution of no. of rides during day/night and age range')
names = ['0 Day', '1 Day', '2 Day', '3 Day', '4 Day', '5 Day', '6 Day', '0 Night', '1 Night', '2 Night', '3 Night', '4 Night', '5 Night', '6 Night']

#procentage
value_day = [x/day_data for x in list(day_age.values())]
value_night= [x/night_data for x in list(night_age.values())]
values = value_day+value_night
plt.bar(names, values, width=0.7)
plt.xlabel('Age range and time of day', fontsize=18)
plt.ylabel('Percentage of users', fontsize=16)
plt.show()


# CONCLUSIONS,
# 
# Comparig the range of ages of the riders in the day and in the night we can see a lot of differences. For example the young with 17-18 years old are the biggest users in the day, but this number drastically decreases in the night. Younger people from 0 to 16 years old are one of the biggest users both in the day and in the night. In addition the 'not defined' type increase by a lot in the night and the little number of users from 27 to 40 years old that use the bike in the day, go close to zero in the night. In the end the users from 19 to 26 years old usually use the bike more in the night.

# In[28]:


sc.stop()

