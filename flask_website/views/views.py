from flask_website import app

from flask import render_template, request, redirect, session, url_for, flash

#Using the following tibber wrapper: https://github.com/BeatsuDev/tibber.py
import tibber

import os

import datetime

from datetime import timezone

from datetime import date
from dateutil.relativedelta import relativedelta

import pytz

import math

import pandas as pd

import numpy as np

from flask_website.config import tibber_token, my_bucket

from google.cloud import storage


@app.route("/")
def index():

    return render_template("index.html")


@app.route("/collectdata", methods=["GET", "POST"])
def collectdata():

    return render_template("/collectdata.html")

@app.route("/datacollected", methods=["GET", "POST"])
def datacollected():

    if request.method == "POST":

        req = request.form 
        action = req.get("action")
        hourstocollect = req["hourstocollect"]

        if action[13:16] == "24 ":
            hourstocollect = 24
        
        if action[13:16] == "48 ":
            hourstocollect = 48

        if action[13:16] == "72 ":
            hourstocollect = 72

        if action[13:16] == "168":
            hourstocollect = 168

        if action[13:16] == "inp" and hourstocollect != "":
            hourstocollect = int(hourstocollect)

        if action[13:16] == "inp" and hourstocollect == "":
            flash("Please input a number before clicking collect data")
            return redirect("/collectdata")

        if action[13:16] == "whe":

            # Retrieving all files from Google storage and finding the last hour collected
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(my_bucket)            
            my_prefix = "Consumption/"
            blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

            list_filesnames_gcs = []

            for blob in blobs:
                if(blob.name != my_prefix):
                    storedtimes = blob.name.replace(my_prefix, "")
                    list_filesnames_gcs.append(storedtimes)
            
            df_storedtimes = pd.DataFrame((list_filesnames_gcs), columns=['filename'])        
            df_storedtimes["start"] = df_storedtimes.filename.str[0:13]
            df_storedtimes = df_storedtimes.drop(columns=['filename'])            
            df_storedtimes = df_storedtimes.sort_values('start', ascending=False)                    
            list_storedtimes2 = df_storedtimes.to_records(index=False)

            lastcollected = list_storedtimes2[0][0]+':00'

            #Converting start time into a datetime object
            lastcollected2=datetime.datetime.strptime(lastcollected, '%Y-%m-%dT%H:%M')

            #Local date and time converted to European Central Time and then made timezone naive
            date_time_now = datetime.datetime.now()
            ect_timezone = pytz.timezone('Europe/Berlin')                                    
            date_time = date_time_now.astimezone(ect_timezone)
            date_time = date_time.replace(tzinfo=None)

            #Calculating how many hours has passed since last data collection
            tdelta = date_time - lastcollected2
            tsecs = tdelta.total_seconds()
            thours = math.floor(tsecs/(3600))

            #Subtracting one hour since we do not want to collect the last collected hour again
            hourstocollect = int(thours)-1

        if hourstocollect > 720:
            hourstocollect = 720
        if hourstocollect < 0 or hourstocollect == 0:
            flash("Can't collect 0 hours. Please input a number equal to 1 or higher")
            return redirect("/collectdata")

        ###############################################################################################
        #Collecting data from Tibber and saving it in lists that are merged and made into a tuple list
        ###############################################################################################

        account=tibber.Account(tibber_token)
        home = account.homes[0]
        hour_data = home.fetch_consumption("HOURLY", last=hourstocollect)
       
        start=[]
        stop=[]
        price=[]
        cons=[]
        cost=[]

        for hour in hour_data:
            data1=(hour.from_time)
            data2=(hour.to_time)
            data3=(f"{hour.unit_price}{hour.currency}")
            data4=(hour.consumption)
            data5=(hour.cost)

            if data4 == None:
                data4 = 0
            if data5 == None:
                data5 = 0                       

            start.append(data1)
            stop.append(data2)
            price.append(data3)
            cons.append(data4)
            cost.append(data5)        

        #Creating a list of only dates 
        date = [d[:-19] for d in start]

        #Removing unnecessary info from the date variable
        start = [d[:-13] for d in start]
        stop = [d[:-13] for d in stop]

        #Removing SEK from the list containing prices
        price = ([s.replace('SEK', '') for s in price])

        #Merging all lists of data to one tuple list and transforming it into a dataframe
        def merge(date,start,stop,price,cons,cost):
            merged_list = [(date[i], start[i], stop[i], price[i], cons[i], cost[i]) for i in range(0, len(start))]
            return merged_list
        collected_data = merge(date,start,stop,price,cons,cost)

        df_collected = pd.DataFrame((collected_data), columns=['date', 'start', 'stop', 'price', 'cons', 'cost'])
        df_collected_data = pd.DataFrame((collected_data), columns=['date', 'start', 'stop', 'price', 'cons', 'cost'])        

        #Creates a new column with the filename to be used
        df_collected_data["filename"] = df_collected_data.start.str[0:13]

        #############################################################
        # Storing all collected data in Google Cloud Storage
        #############################################################  

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(my_bucket)

        df_collected_data = df_collected_data.reset_index()  # make sure indexes pair with number of rows

        for index, row in df_collected_data.iterrows():
            bucket.blob('Collecteddata/{}.csv'.format(row.iloc[7])).upload_from_string(row.iloc[1:7].to_csv(header=False, index=False), 'text/csv')

        # Storing newly collected consumption data in Google storage        
        my_prefix = "Consumption/"
        blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

        list_filesnames_gcs = []

        for blob in blobs:
            if(blob.name != my_prefix):
                storedtimes = blob.name.replace(my_prefix, "")
                list_filesnames_gcs.append(storedtimes)

        df_storedtimes = pd.DataFrame((list_filesnames_gcs), columns=['filename'])        
        df_storedtimes["start"] = df_storedtimes.filename.str[0:13]+':00'
        df_storedtimes = df_storedtimes.drop(columns=['filename'])            
        df_storedtimes = df_storedtimes.sort_values('start', ascending=False)                    
        list_storedtimes2 = df_storedtimes.to_records(index=False)
        list_storedtimes2  = [tupleObj[0] for tupleObj in list_storedtimes2]            
        list_storedtimes = list(list_storedtimes2)

        df_consumption = df_collected.assign(cons_ev = df_collected.cons * 0)        
        df_consumption["filename"] = df_consumption.start.str[0:13]
        df_consumption = df_consumption.drop(columns=['stop', 'price', 'cost'])

        #Filtering out which consumption data to store and uploading it to Google storage
        df_consumption_to_be_saved_gcs = df_consumption.query('start not in @list_storedtimes')

        df_consumption_to_be_saved_gcs = df_consumption_to_be_saved_gcs.reset_index()

        for index, row in df_consumption_to_be_saved_gcs.iterrows():
            bucket.blob('Consumption/{}.csv'.format(row.iloc[5])).upload_from_string(row.iloc[1:5].to_csv(header=False, index=False), 'text/csv')

        # Storing newly collected cost data in Google storage
        df_cost = df_collected.assign(cost_ev = df_collected.cost * 0)        
        df_cost["filename"] = df_cost.start.str[0:13]
        df_cost = df_cost.drop(columns=['stop', 'price', 'cons'])

        #Filtering out which consumption data to store and uploading it to Google storage
        df_cost_to_be_saved_gcs = df_cost.query('start not in @list_storedtimes')

        #Storing data in projects Google Cloud Bucket
        df_cost_to_be_saved_gcs = df_cost_to_be_saved_gcs.reset_index()

        for index, row in df_cost_to_be_saved_gcs.iterrows():
            bucket.blob('Cost/{}.csv'.format(row.iloc[5])).upload_from_string(row.iloc[1:5].to_csv(header=False, index=False), 'text/csv')

        ###########################################################################
        # Converting collected data to a list to be presented on the webpage
        ###########################################################################        

        data = tuple(sorted(collected_data, reverse=True))

        dayscollected = hourstocollect / 24
        
    return render_template("/datacollected.html", data=data, hourstocollect=hourstocollect, dayscollected=dayscollected)


@app.route("/updateday", methods=["GET", "POST"])
def updateday():

    global chosendate

    if request.method == "GET":

        date_time = datetime.datetime.now()
        chosendate = date_time.strftime("%Y-%m-%d")        

        ################################################################################################
        # Getting paths to files stored on Google storage and making them into a list and dataframe. 
        ################################################################################################

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(my_bucket)        
        my_prefix = "Collecteddata/"
        blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

        list_paths_gcs = []

        for blob in blobs:        
            if(blob.name != my_prefix):
                paths = blob.name.replace(my_prefix, "")            
                list_paths_gcs.append(paths)                

        #Filtering out all dates other than current date and making it into a list
        df_paths = pd.DataFrame((list_paths_gcs), columns=['path'])        
        df_paths["date"] = df_paths.path.str[0:10]
        list_chosendata= [chosendate]
        df_paths = df_paths.query('date in @list_chosendata')        
        df_paths = df_paths.drop(columns=['date'])                    
        list_paths2 = df_paths.to_records(index=False)
        list_paths3  = [tupleObj[0] for tupleObj in list_paths2]            
        #list_paths3 = list(list_paths2)

        #Flashing message if there is no date
        if list_paths3 == []:
            flash("There is no data for today. Please collect data first")
            return redirect("/collectdata")        

        #########################################################
        # Downloading selected files from Google storage
        #########################################################

        gcs_data_collected = []

        for blob_name in list_paths3:
            gcs_file_collected = bucket.get_blob('Collecteddata/'+blob_name).download_as_text().replace("\r\n", ",")
            gcs_file_collected = gcs_file_collected.replace("\n", ",")           
            gcs_data_collected.append(gcs_file_collected)            
     
        #Converting data into a Pandas dataframe
        df_data_collected = pd.DataFrame((gcs_data_collected), columns=['String'])
        df_data_collected[['date', 'start', 'stop', 'price', 'consumption', 'cost', 'Delete']]=df_data_collected["String"].str.split(",", expand=True)
        df_data_collected = df_data_collected.drop(columns=['String', 'Delete'])        

        gcs_data_consumption = []

        for blob_name in list_paths3:
            gcs_file_consumption = bucket.get_blob('Consumption/'+blob_name).download_as_text().replace("\r\n", ",")
            gcs_file_consumption = gcs_file_consumption.replace("\n", ",")                           
            gcs_data_consumption.append(gcs_file_consumption)            
     
        #Converting data into a Pandas dataframe
        df_data_consumption = pd.DataFrame((gcs_data_consumption), columns=['String'])
        df_data_consumption[['date', 'start', 'consumption_house', 'consumption_ev', 'Delete']]=df_data_consumption["String"].str.split(",", expand=True)
        df_data_consumption = df_data_consumption.drop(columns=['String', 'Delete'])        

        gcs_data_cost = []

        for blob_name in list_paths3:
            gcs_file_cost = bucket.get_blob('Cost/'+blob_name).download_as_text().replace("\r\n", ",")  
            gcs_file_cost = gcs_file_cost.replace("\n", ",")                     
            gcs_data_cost.append(gcs_file_cost)            
     
        #Converting data into a Pandas dataframe
        df_data_cost = pd.DataFrame((gcs_data_cost), columns=['String'])
        df_data_cost[['date', 'start', 'cost_house', 'cost_ev', 'Delete']]=df_data_cost["String"].str.split(",", expand=True)
        df_data_cost = df_data_cost.drop(columns=['String', 'Delete'])        

        df_data1 = df_data_collected
        df_data2 = df_data_consumption
        df_data3 = df_data_cost

        #Transforming the lists into dataframes, droping unnecessary colums, transforming strings to floats, merger dataframes, rounding the figures to two decimals and transforming the dataframe to a list
        df_data2 = df_data2.drop(columns=['date'])
        df_data3 = df_data3.drop(columns=['date'])
        df_data1_2 = pd.merge(df_data1, df_data2, on="start")
        df_data1_3 = pd.merge(df_data1_2, df_data3, on="start")
        df_data1_3[["date2", "time"]] = df_data1_3.start.str.split("T", expand=True)
        df_data1_3[["hour", "min"]] = df_data1_3.time.str.split(":", expand=True)
        df_data1_3 = df_data1_3.drop(columns=['date2', 'time', 'min'])
        data = df_data1_3.values.tolist()

        #Adjusting the dataframe and aggregating the data by date
        df_aggr1 = df_data1_3.drop(columns=['start','stop', 'price'])
        df_aggr1['consumption'] = df_aggr1['consumption'].astype(float)
        df_aggr1['cost'] = df_aggr1['cost'].astype(float)
        df_aggr1['consumption_house'] = df_aggr1['consumption_house'].astype(float)
        df_aggr1['consumption_ev'] = df_aggr1['consumption_ev'].astype(float)
        df_aggr1['cost_house'] = df_aggr1['cost_house'].astype(float)
        df_aggr1['cost_ev'] = df_aggr1['cost_ev'].astype(float)
        df_aggr2 = df_aggr1.groupby(['date'], as_index=False).sum()
        rounded_df_aggr2 = df_aggr2.round(decimals=2)
        aggr = rounded_df_aggr2.values.tolist()

    if request.method == "POST":

        req = request.form

        action=req["action"]

        if action[0:4] == "View":

            #Creating a variable from the form data
            chosendate = req["chosendate2"]

            date_time = datetime.datetime.now()
            today = date_time.strftime("%Y-%m-%d")

            #Flashing message if date is not applicable
            if chosendate > today or chosendate == "":
                flash("Please select today or an earlier date")
                return redirect("/updateday")

            ############################################################
            # Retrieving files from Google storage for chosen date
            ############################################################

            storage_client = storage.Client()
            bucket = storage_client.get_bucket(my_bucket)        
            my_prefix = "Collecteddata/"
            blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

            list_paths_gcs = []

            for blob in blobs:        
                if(blob.name != my_prefix):
                    paths = blob.name.replace(my_prefix, "")            
                    list_paths_gcs.append(paths)                

            #Filtering out all dates other than current date and making it into a list
            df_paths = pd.DataFrame((list_paths_gcs), columns=['path'])        
            df_paths["date"] = df_paths.path.str[0:10]
            list_chosendata= [chosendate]
            df_paths = df_paths.query('date in @list_chosendata')        
            df_paths = df_paths.drop(columns=['date'])                    
            list_paths2 = df_paths.to_records(index=False)
            list_paths3  = [tupleObj[0] for tupleObj in list_paths2]            
            #list_paths3 = list(list_paths2)

            #Flashing message if there is no date
            if list_paths3 == []:
                flash("There is no data for selected date. Please collect data first")
                return redirect("/collectdata")            

            #########################################################
            # Downloading selected files from Google storage
            #########################################################

            gcs_data_collected = []

            for blob_name in list_paths3:
                gcs_file_collected = bucket.get_blob('Collecteddata/'+blob_name).download_as_text().replace("\r\n", ",")            
                gcs_file_collected = gcs_file_collected.replace("\n", ",")   
                gcs_data_collected.append(gcs_file_collected)            
        
            #Converting data into a Pandas dataframe
            df_data_collected = pd.DataFrame((gcs_data_collected), columns=['String'])
            df_data_collected[['date', 'start', 'stop', 'price', 'consumption', 'cost', 'Delete']]=df_data_collected["String"].str.split(",", expand=True)
            df_data_collected = df_data_collected.drop(columns=['String', 'Delete'])        

            gcs_data_consumption = []

            for blob_name in list_paths3:
                gcs_file_consumption = bucket.get_blob('Consumption/'+blob_name).download_as_text().replace("\r\n", ",")            
                gcs_file_consumption = gcs_file_consumption.replace("\n", ",")                
                gcs_data_consumption.append(gcs_file_consumption)            
        
            #Converting data into a Pandas dataframe
            df_data_consumption = pd.DataFrame((gcs_data_consumption), columns=['String'])
            df_data_consumption[['date', 'start', 'consumption_house', 'consumption_ev', 'Delete']]=df_data_consumption["String"].str.split(",", expand=True)
            df_data_consumption = df_data_consumption.drop(columns=['String', 'Delete'])        

            gcs_data_cost = []

            for blob_name in list_paths3:
                gcs_file_cost = bucket.get_blob('Cost/'+blob_name).download_as_text().replace("\r\n", ",")   
                gcs_file_cost = gcs_file_cost.replace("\n", ",")                           
                gcs_data_cost.append(gcs_file_cost)            
        
            #Converting data into a Pandas dataframe
            df_data_cost = pd.DataFrame((gcs_data_cost), columns=['String'])
            df_data_cost[['date', 'start', 'cost_house', 'cost_ev', 'Delete']]=df_data_cost["String"].str.split(",", expand=True)
            df_data_cost = df_data_cost.drop(columns=['String', 'Delete'])        

            df_data1 = df_data_collected
            df_data2 = df_data_consumption
            df_data3 = df_data_cost

            #Transforming the lists into dataframes, droping unnecessary colums, transforming strings to floats, merger dataframes, rounding the figures to two decimals and transforming the dataframe to a list
            df_data2 = df_data2.drop(columns=['date'])
            df_data3 = df_data3.drop(columns=['date'])
            df_data1_2 = pd.merge(df_data1, df_data2, on="start")
            df_data1_3 = pd.merge(df_data1_2, df_data3, on="start")
            df_data1_3[["date2", "time"]] = df_data1_3.start.str.split("T", expand=True)
            df_data1_3[["hour", "min"]] = df_data1_3.time.str.split(":", expand=True)
            df_data1_3 = df_data1_3.drop(columns=['date2', 'time', 'min'])
            data = df_data1_3.values.tolist()

            #Adjusting the dataframe and aggregating the data by date
            df_aggr1 = df_data1_3.drop(columns=['start','stop', 'price'])
            df_aggr1['consumption'] = df_aggr1['consumption'].astype(float)
            df_aggr1['cost'] = df_aggr1['cost'].astype(float)
            df_aggr1['consumption_house'] = df_aggr1['consumption_house'].astype(float)
            df_aggr1['consumption_ev'] = df_aggr1['consumption_ev'].astype(float)
            df_aggr1['cost_house'] = df_aggr1['cost_house'].astype(float)
            df_aggr1['cost_ev'] = df_aggr1['cost_ev'].astype(float)
            df_aggr2 = df_aggr1.groupby(['date'], as_index=False).sum()
            rounded_df_aggr2 = df_aggr2.round(decimals=2)
            aggr = rounded_df_aggr2.values.tolist()

        if action[0:4] == "Upda":

            chosendate = req["chosendate"] 

            #################################################################
            # Retrieving paths to files for chosen date from Google storage
            #################################################################

            storage_client = storage.Client()
            bucket = storage_client.get_bucket(my_bucket)        
            my_prefix = "Collecteddata/"
            blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

            list_paths_gcs = []

            for blob in blobs:        
                if(blob.name != my_prefix):
                    paths = blob.name.replace(my_prefix, "")            
                    list_paths_gcs.append(paths)                

            #Filtering out all dates other than current date and making it into a list
            df_paths = pd.DataFrame((list_paths_gcs), columns=['path'])        
            df_paths["date"] = df_paths.path.str[0:10]
            list_chosendata= [chosendate]
            df_paths = df_paths.query('date in @list_chosendata')        
            df_paths = df_paths.drop(columns=['date'])                    
            list_paths2 = df_paths.to_records(index=False)
            list_paths3  = [tupleObj[0] for tupleObj in list_paths2]            
            #list_paths3 = list(list_paths2)

            #########################################################
            # Downloading selected files from Google storage
            #########################################################

            gcs_data_collected = []

            for blob_name in list_paths3:
                gcs_file_collected = bucket.get_blob('Collecteddata/'+blob_name).download_as_text().replace("\r\n", ",")            
                gcs_file_collected = gcs_file_collected.replace("\n", ",")                   
                gcs_data_collected.append(gcs_file_collected)            
        
            #Converting data into a Pandas dataframe
            df_data_collected = pd.DataFrame((gcs_data_collected), columns=['String'])
            df_data_collected[['date', 'start', 'stop', 'price', 'consumption', 'cost', 'Delete']]=df_data_collected["String"].str.split(",", expand=True)
            df_data_collected = df_data_collected.drop(columns=['String', 'Delete'])        

            gcs_data_consumption = []

            for blob_name in list_paths3:
                gcs_file_consumption = bucket.get_blob('Consumption/'+blob_name).download_as_text().replace("\r\n", ",")            
                gcs_file_consumption = gcs_file_consumption.replace("\n", ",")                
                gcs_data_consumption.append(gcs_file_consumption)            
        
            #Converting data into a Pandas dataframe
            df_data_consumption = pd.DataFrame((gcs_data_consumption), columns=['String'])
            df_data_consumption[['date', 'start', 'consumption_house', 'consumption_ev', 'Delete']]=df_data_consumption["String"].str.split(",", expand=True)
            df_data_consumption = df_data_consumption.drop(columns=['String', 'Delete'])        

            data1 = df_data_collected.values.tolist()
            data2 = df_data_consumption.values.tolist()

            #########################################################
            # Creating variables from the form dictionary 
            #########################################################

            if req.get("cons_ev00") != None:            
                cons_ev00 = float(req["cons_ev00"])           
            if req.get("cons_ev01") != None: 
                cons_ev01 = float(req["cons_ev01"])                
            if req.get("cons_ev02") != None: 
                cons_ev02 = float(req["cons_ev02"])                  
            if req.get("cons_ev03") != None: 
                cons_ev03 = float(req["cons_ev03"])               
            if req.get("cons_ev04") != None: 
                cons_ev04 = float(req["cons_ev04"])                 
            if req.get("cons_ev05") != None: 
                cons_ev05 = float(req["cons_ev05"])                 
            if req.get("cons_ev06") != None: 
                cons_ev06 = float(req["cons_ev06"])                
            if req.get("cons_ev07") != None: 
                cons_ev07 = float(req["cons_ev07"])               
            if req.get("cons_ev08") != None: 
                cons_ev08 = float(req["cons_ev08"])                
            if req.get("cons_ev09") != None: 
                cons_ev09 = float(req["cons_ev09"])                
            if req.get("cons_ev10") != None: 
                cons_ev10 = float(req["cons_ev10"])                 
            if req.get("cons_ev11") != None: 
                cons_ev11 = float(req["cons_ev11"])                 
            if req.get("cons_ev12") != None: 
                cons_ev12 = float(req["cons_ev12"])                
            if req.get("cons_ev13") != None: 
                cons_ev13 = float(req["cons_ev13"])                 
            if req.get("cons_ev14") != None: 
                cons_ev14 = float(req["cons_ev14"])                 
            if req.get("cons_ev15") != None: 
                cons_ev15 = float(req["cons_ev15"])                
            if req.get("cons_ev16") != None: 
                cons_ev16 = float(req["cons_ev16"])                 
            if req.get("cons_ev17") != None: 
                cons_ev17 = float(req["cons_ev17"])                 
            if req.get("cons_ev18") != None: 
                cons_ev18 = float(req["cons_ev18"])                 
            if req.get("cons_ev19") != None:
                cons_ev19 = float(req["cons_ev19"])
            if req.get("cons_ev20") != None:           
                cons_ev20 = float(req["cons_ev20"])
            if req.get("cons_ev21") != None:
                cons_ev21 = float(req["cons_ev21"])
            if req.get("cons_ev22") != None:
                cons_ev22 = float(req["cons_ev22"])
            if req.get("cons_ev23") != None:                
                cons_ev23 = float(req["cons_ev23"])

            if req.get("cons00") != None:            
                cons00 = float(req["cons00"])
            if req.get("cons01") != None: 
                cons01 = float(req["cons01"])             
            if req.get("cons02") != None: 
                cons02 = float(req["cons02"])               
            if req.get("cons03") != None: 
                cons03 = float(req["cons03"])                 
            if req.get("cons04") != None: 
                cons04 = float(req["cons04"])                
            if req.get("cons05") != None: 
                cons05 = float(req["cons05"])               
            if req.get("cons06") != None: 
                cons06 = float(req["cons06"])                
            if req.get("cons07") != None: 
                cons07 = float(req["cons07"])               
            if req.get("cons08") != None: 
                cons08 = float(req["cons08"])               
            if req.get("cons09") != None: 
                cons09 = float(req["cons09"])                 
            if req.get("cons10") != None: 
                cons10 = float(req["cons10"])                
            if req.get("cons11") != None: 
                cons11 = float(req["cons11"])               
            if req.get("cons12") != None: 
                cons12 = float(req["cons12"])                 
            if req.get("cons13") != None: 
                cons13 = float(req["cons13"])                
            if req.get("cons14") != None: 
                cons14 = float(req["cons14"])                 
            if req.get("cons15") != None: 
                cons15 = float(req["cons15"])               
            if req.get("cons16") != None: 
                cons16 = float(req["cons16"])                
            if req.get("cons17") != None: 
                cons17 = float(req["cons17"])                
            if req.get("cons18") != None: 
                cons18 = float(req["cons18"])             
            if req.get("cons19") != None:
                cons19 = float(req["cons19"])
            if req.get("cons20") != None:           
                cons20 = float(req["cons20"])
            if req.get("cons21") != None:
                cons21 = float(req["cons21"])
            if req.get("cons22") != None:
                cons22 = float(req["cons22"])
            if req.get("cons23") != None:                
                cons23 = float(req["cons23"])   

            start00 = req.get("start00")
            start01 = req.get("start01")
            start02 = req.get("start02")
            start03 = req.get("start03")
            start04 = req.get("start04")
            start05 = req.get("start05")
            start06 = req.get("start06")
            start07 = req.get("start07")
            start08 = req.get("start08")
            start09 = req.get("start09")
            start10 = req.get("start10")
            start11 = req.get("start11")
            start12 = req.get("start12")
            start13 = req.get("start13")
            start14 = req.get("start14")
            start15 = req.get("start15")
            start16 = req.get("start16")
            start17 = req.get("start17")
            start18 = req.get("start18")
            start19 = req.get("start19")
            start20 = req.get("start20")
            start21 = req.get("start21")
            start22 = req.get("start22")
            start23 = req.get("start23")

            stop00 = req.get("stop00")
            stop01 = req.get("stop01")
            stop02 = req.get("stop02")
            stop03 = req.get("stop03")
            stop04 = req.get("stop04")
            stop05 = req.get("stop05")
            stop06 = req.get("stop06")
            stop07 = req.get("stop07")
            stop08 = req.get("stop08")
            stop09 = req.get("stop09")
            stop10 = req.get("stop10")
            stop11 = req.get("stop11")
            stop12 = req.get("stop12")
            stop13 = req.get("stop13")
            stop14 = req.get("stop14")
            stop15 = req.get("stop15")
            stop16 = req.get("stop16")
            stop17 = req.get("stop17")
            stop18 = req.get("stop18")
            stop19 = req.get("stop19")
            stop20 = req.get("stop20")
            stop21 = req.get("stop21")
            stop22 = req.get("stop22")
            stop23 = req.get("stop23")            

            price00 = req.get("price00")
            price01 = req.get("price01")
            price02 = req.get("price02")
            price03 = req.get("price03")
            price04 = req.get("price04")
            price05 = req.get("price05")
            price06 = req.get("price06")
            price07 = req.get("price07")
            price08 = req.get("price08")
            price09 = req.get("price09")
            price10 = req.get("price10")
            price11 = req.get("price11")
            price12 = req.get("price12")
            price13 = req.get("price13")
            price14 = req.get("price14")
            price15 = req.get("price15")
            price16 = req.get("price16")
            price17 = req.get("price17")
            price18 = req.get("price18")
            price19 = req.get("price19")
            price20 = req.get("price20")
            price21 = req.get("price21")
            price22 = req.get("price22")
            price23 = req.get("price23")

            if start00 != None:
                cons_house00 = round((cons00 - cons_ev00), 3)
                cost00 = round((float(data1[0][3])*cons00), 8)
                cost_house00 = round((float(data1[0][3])*cons_house00), 3)
                cost_ev00 = round((float(data1[0][3])*cons_ev00), 3)
            if start01 != None:      
                cons_house01 = round((cons01 - cons_ev01), 3)
                cost01 = round((float(data1[1][3])*cons01), 8)
                cost_house01 = round((float(data1[1][3])*cons_house01), 3)
                cost_ev01 = round((float(data1[1][3])*cons_ev01), 3)
            if start02 != None:
                cons_house02 = round((cons02 - cons_ev02), 3)
                cost02 = round((float(data1[2][3])*cons02), 8)           
                cost_house02 = round((float(data1[2][3])*cons_house02), 3)
                cost_ev02 = round((float(data1[2][3])*cons_ev02), 3)
            if start03 != None:
                cons_house03 = round((cons03 - cons_ev03), 3)
                cost03 = round((float(data1[3][3])*cons03), 8)                                 
                cost_house03 = round((float(data1[3][3])*cons_house03), 3)
                cost_ev03 = round((float(data1[3][3])*cons_ev03), 3)
            if start04 != None:          
                cons_house04 = round((cons04 - cons_ev04), 3)
                cost04 = round((float(data1[4][3])*cons04), 8)                       
                cost_house04 = round((float(data1[4][3])*cons_house04), 3)
                cost_ev04 = round((float(data1[4][3])*cons_ev04), 3)                
            if start05 != None:   
                cons_house05 = round((cons05- cons_ev05), 3)
                cost05 = round((float(data1[5][3])*cons05), 8)                               
                cost_house05 = round((float(data1[5][3])*cons_house05), 3)
                cost_ev05 = round((float(data1[5][3])*cons_ev05), 3)                 
            if start06 != None:
                cons_house06 = round((cons06 - cons_ev06), 3)
                cost06= round((float(data1[6][3])*cons06), 8)                                  
                cost_house06 = round((float(data1[6][3])*cons_house06), 3)
                cost_ev06 = round((float(data1[6][3])*cons_ev06), 3)                 
            if start07 != None:  
                cons_house07 = round((cons07 - cons_ev07), 3)
                cost07 = round((float(data1[7][3])*cons07), 8)                                
                cost_house07 = round((float(data1[7][3])*cons_house07), 3)
                cost_ev07 = round((float(data1[7][3])*cons_ev07), 3)                 
            if start08 != None:    
                cons_house08 = round((cons08 - cons_ev08), 3)
                cost08 = round((float(data1[8][3])*cons08), 8)                              
                cost_house08 = round((float(data1[8][3])*cons_house08), 3)
                cost_ev08 = round((float(data1[8][3])*cons_ev08), 3)                 
            if start09 != None:   
                cons_house09 = round((cons09 - cons_ev09), 3)
                cost09 = round((float(data1[9][3])*cons09), 8)                               
                cost_house09 = round((float(data1[9][3])*cons_house09), 3)
                cost_ev09 = round((float(data1[9][3])*cons_ev09), 3)                 
            if start10 != None:    
                cons_house10 = round((cons10 - cons_ev10), 3)
                cost10 = round((float(data1[10][3])*cons10), 8)                              
                cost_house10 = round((float(data1[10][3])*cons_house10), 3)
                cost_ev10 = round((float(data1[10][3])*cons_ev10), 3)                 
            if start11 != None:   
                cons_house11 = round((cons11 - cons_ev11), 3)
                cost11 = round((float(data1[11][3])*cons11), 8)                               
                cost_house11 = round((float(data1[11][3])*cons_house11), 3)
                cost_ev11 = round((float(data1[11][3])*cons_ev11), 3)                 
            if start12 != None: 
                cons_house12 = round((cons12 - cons_ev12), 3)
                cost12 = round((float(data1[12][3])*cons12), 8)                                 
                cost_house12 = round((float(data1[12][3])*cons_house12), 3)
                cost_ev12 = round((float(data1[12][3])*cons_ev12), 3)                  
            if start13 != None: 
                cons_house13 = round((cons13 - cons_ev13), 3)
                cost13 = round((float(data1[13][3])*cons13), 8)                                 
                cost_house13= round((float(data1[13][3])*cons_house13), 3)
                cost_ev13 = round((float(data1[13][3])*cons_ev13), 3)                  
            if start14 != None:   
                cons_house14 = round((cons14 - cons_ev14), 3)
                cost14 = round((float(data1[14][3])*cons14), 8)                               
                cost_house14 = round((float(data1[14][3])*cons_house14), 3)
                cost_ev14 = round((float(data1[14][3])*cons_ev14), 3)                  
            if start15 != None:  
                cons_house15 = round((cons15 - cons_ev15), 3)
                cost15 = round((float(data1[15][3])*cons15), 8)                                
                cost_house15 = round((float(data1[15][3])*cons_house15), 3)
                cost_ev15 = round((float(data1[15][3])*cons_ev15), 3)                  
            if start16 != None:  
                cons_house16 = round((cons16 - cons_ev16), 3)
                cost16 = round((float(data1[16][3])*cons16), 8)                                
                cost_house16 = round((float(data1[16][3])*cons_house16), 3)
                cost_ev16 = round((float(data1[16][3])*cons_ev16), 3)                  
            if start17 != None:    
                cons_house17 = round((cons17 - cons_ev17), 3)
                cost17 = round((float(data1[17][3])*cons17), 8)                              
                cost_house17 = round((float(data1[17][3])*cons_house17), 3)
                cost_ev17 = round((float(data1[17][3])*cons_ev17), 3)                  
            if start18 != None:  
                cons_house18 = round((cons18 - cons_ev18), 3)
                cost18 = round((float(data1[18][3])*cons18), 8)                                
                cost_house18 = round((float(data1[18][3])*cons_house18), 3)
                cost_ev18 = round((float(data1[18][3])*cons_ev18), 3)                  
            if start19 != None:
                cons_house19 = round((cons19 - cons_ev19), 3)
                cost19 = round((float(data1[19][3])*cons19), 8)                  
                cost_house19 = round((float(data1[19][3])*cons_house19), 3)
                cost_ev19 = round((float(data1[19][3])*cons_ev19), 3)                  
            if start20 != None:
                cons_house20 = round((cons20 - cons_ev20), 3)
                cost20 = round((float(data1[20][3])*cons20), 8)  
                cost_house20 = round((float(data1[20][3])*cons_house20), 3)
                cost_ev20 = round((float(data1[20][3])*cons_ev20), 3)                 
            if start21 != None:
                cons_house21 = round((cons21 - cons_ev21), 3)
                cost21 = round((float(data1[21][3])*cons21), 8)                  
                cost_house21 = round((float(data1[21][3])*cons_house21), 3)
                cost_ev21 = round((float(data1[21][3])*cons_ev21), 3)                 
            if start22 != None:
                cons_house22 = round((cons22 - cons_ev22), 3)
                cost22 = round((float(data1[22][3])*cons22), 8)                  
                cost_house22 = round((float(data1[22][3])*cons_house22), 3)
                cost_ev22 = round((float(data1[22][3])*cons_ev22), 3)                 
            if start23 != None:
                cons_house23 = round((cons23 - cons_ev23), 3)
                cost23 = round((float(data1[23][3])*cons23), 8)                  
                cost_house23 = round((float(data1[23][3])*cons_house23), 3)
                cost_ev23 = round((float(data1[23][3])*cons_ev23), 3)                 


            ###########################################################
            # Storing updated data in Google storage / on local drive
            ###########################################################

            storage_client = storage.Client()
            bucket = storage_client.get_bucket(my_bucket)        

            if start00 != None:

                date00 = start00[0:10]
                filename00 = start00[0:13]

                collected_data00 = [(date00, start00, stop00, price00, cons00, cost00, filename00)]
                consumption00 = [(date00, start00, cons_house00, cons_ev00, filename00)]
                cost00 = [(date00, start00, cost_house00, cost_ev00, filename00)]                                
                df_collected_data00 = pd.DataFrame((collected_data00), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption00 = pd.DataFrame((consumption00), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost00 = pd.DataFrame((cost00), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data00 = df_collected_data00.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data00.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption00 = df_consumption00.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption00.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost00 = df_cost00.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost00.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start01 != None:

                date01 = start01[0:10]
                filename01 = start01[0:13]

                collected_data01 = [(date01, start01, stop01, price01, cons01, cost01, filename01)]
                consumption01 = [(date01, start01, cons_house01, cons_ev01, filename01)]
                cost01 = [(date01, start01, cost_house01, cost_ev01, filename01)]                                
                df_collected_data01 = pd.DataFrame((collected_data01), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption01 = pd.DataFrame((consumption01), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost01 = pd.DataFrame((cost01), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        
 
                # Storing in Google storage
                df_collected_data01 = df_collected_data01.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data01.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption01 = df_consumption01.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption01.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost01 = df_cost01.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost01.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')


            if start02 != None:

                date02 = start02[0:10]
                filename02 = start02[0:13]

                collected_data02 = [(date02, start02, stop02, price02, cons02, cost02, filename02)]
                consumption02 = [(date02, start02, cons_house02, cons_ev02, filename02)]
                cost02 = [(date02, start02, cost_house02, cost_ev02, filename02)]                                
                df_collected_data02 = pd.DataFrame((collected_data02), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption02 = pd.DataFrame((consumption02), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost02 = pd.DataFrame((cost02), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        
 
                # Storing in Google storage
                df_collected_data02 = df_collected_data02.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data02.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption02 = df_consumption02.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption02.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost02 = df_cost02.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost02.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start03 != None:

                date03 = start03[0:10]
                filename03 = start03[0:13]

                collected_data03 = [(date03, start03, stop03, price03, cons03, cost03, filename03)]
                consumption03 = [(date03, start03, cons_house03, cons_ev03, filename03)]
                cost03 = [(date03, start03, cost_house03, cost_ev03, filename03)]                                
                df_collected_data03 = pd.DataFrame((collected_data03), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption03 = pd.DataFrame((consumption03), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost03 = pd.DataFrame((cost03), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        
 
                # Storing in Google storage
                df_collected_data03 = df_collected_data03.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data03.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption03 = df_consumption03.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption03.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost03 = df_cost03.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost03.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start04 != None:

                date04 = start04[0:10]
                filename04 = start04[0:13]

                collected_data04 = [(date04, start04, stop04, price04, cons04, cost04, filename04)]
                consumption04 = [(date04, start04, cons_house04, cons_ev04, filename04)]
                cost04 = [(date04, start04, cost_house04, cost_ev04, filename04)]                                
                df_collected_data04 = pd.DataFrame((collected_data04), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption04 = pd.DataFrame((consumption04), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost04 = pd.DataFrame((cost04), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data04 = df_collected_data04.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data04.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption04 = df_consumption04.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption04.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost04 = df_cost04.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost04.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')                    

            if start05 != None:

                date05 = start05[0:10]
                filename05 = start05[0:13]

                collected_data05 = [(date05, start05, stop05, price05, cons05, cost05, filename05)]
                consumption05 = [(date05, start05, cons_house05, cons_ev05, filename05)]
                cost05 = [(date05, start05, cost_house05, cost_ev05, filename05)]                                
                df_collected_data05 = pd.DataFrame((collected_data05), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption05 = pd.DataFrame((consumption05), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost05 = pd.DataFrame((cost05), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data05 = df_collected_data05.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data05.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption05 = df_consumption05.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption05.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost05 = df_cost05.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost05.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start06 != None:

                date06 = start06[0:10]
                filename06 = start06[0:13]

                collected_data06 = [(date06, start06, stop06, price06, cons06, cost06, filename06)]
                consumption06 = [(date06, start06, cons_house06, cons_ev06, filename06)]
                cost06 = [(date06, start06, cost_house06, cost_ev06, filename06)]                                
                df_collected_data06 = pd.DataFrame((collected_data06), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption06 = pd.DataFrame((consumption06), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost06 = pd.DataFrame((cost06), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data06 = df_collected_data06.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data06.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption06 = df_consumption06.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption06.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost06 = df_cost06.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost06.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start07 != None:

                date07 = start07[0:10]
                filename07 = start07[0:13]

                collected_data07 = [(date07, start07, stop07, price07, cons07, cost07, filename07)]
                consumption07 = [(date07, start07, cons_house07, cons_ev07, filename07)]
                cost07 = [(date07, start07, cost_house07, cost_ev07, filename07)]                                
                df_collected_data07 = pd.DataFrame((collected_data07), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption07 = pd.DataFrame((consumption07), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost07 = pd.DataFrame((cost07), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data07 = df_collected_data07.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data07.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption07 = df_consumption07.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption07.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost07 = df_cost07.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost07.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start08 != None:

                date08 = start08[0:10]
                filename08 = start08[0:13]

                collected_data08 = [(date08, start08, stop08, price08, cons08, cost08, filename08)]
                consumption08 = [(date08, start08, cons_house08, cons_ev08, filename08)]
                cost08 = [(date08, start08, cost_house08, cost_ev08, filename08)]                                
                df_collected_data08 = pd.DataFrame((collected_data08), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption08 = pd.DataFrame((consumption08), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost08 = pd.DataFrame((cost08), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data08 = df_collected_data08.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data08.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption08 = df_consumption08.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption08.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost08 = df_cost08.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost08.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start09 != None:

                date09 = start09[0:10]
                filename09 = start09[0:13]

                collected_data09 = [(date09, start09, stop09, price09, cons09, cost09, filename09)]
                consumption09 = [(date09, start09, cons_house09, cons_ev09, filename09)]
                cost09 = [(date09, start09, cost_house09, cost_ev09, filename09)]                                
                df_collected_data09 = pd.DataFrame((collected_data09), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption09 = pd.DataFrame((consumption09), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost09 = pd.DataFrame((cost09), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data09 = df_collected_data09.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data09.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption09 = df_consumption09.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption09.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost09 = df_cost09.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost09.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start10 != None:

                date10 = start10[0:10]
                filename10 = start10[0:13]

                collected_data10 = [(date10, start10, stop10, price10, cons10, cost10, filename10)]
                consumption10 = [(date10, start10, cons_house10, cons_ev10, filename10)]
                cost10 = [(date10, start10, cost_house10, cost_ev10, filename10)]                                
                df_collected_data10 = pd.DataFrame((collected_data10), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption10 = pd.DataFrame((consumption10), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost10 = pd.DataFrame((cost10), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data10 = df_collected_data10.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data10.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption10 = df_consumption10.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption10.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost10 = df_cost10.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost10.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start11 != None:

                date11 = start11[0:10]
                filename11 = start11[0:13]

                collected_data11 = [(date11, start11, stop11, price11, cons11, cost11, filename11)]
                consumption11 = [(date11, start11, cons_house11, cons_ev11, filename11)]
                cost11 = [(date11, start11, cost_house11, cost_ev11, filename11)]                                
                df_collected_data11 = pd.DataFrame((collected_data11), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption11 = pd.DataFrame((consumption11), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost11 = pd.DataFrame((cost11), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        
                    
                # Storing in Google storage
                df_collected_data11 = df_collected_data11.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data11.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption11 = df_consumption11.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption11.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost11 = df_cost11.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost11.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start12 != None:

                date12 = start12[0:10]
                filename12 = start12[0:13]

                collected_data12 = [(date12, start12, stop12, price12, cons12, cost12, filename12)]
                consumption12 = [(date12, start12, cons_house12, cons_ev12, filename12)]
                cost12 = [(date12, start12, cost_house12, cost_ev12, filename12)]                                
                df_collected_data12 = pd.DataFrame((collected_data12), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption12 = pd.DataFrame((consumption12), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost12 = pd.DataFrame((cost12), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data12 = df_collected_data12.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data12.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption12 = df_consumption12.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption12.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost12 = df_cost12.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost12.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start13 != None:

                date13 = start13[0:10]
                filename13 = start13[0:13]

                collected_data13 = [(date13, start13, stop13, price13, cons13, cost13, filename13)]
                consumption13 = [(date13, start13, cons_house13, cons_ev13, filename13)]
                cost13 = [(date13, start13, cost_house13, cost_ev13, filename13)]                                
                df_collected_data13 = pd.DataFrame((collected_data13), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption13 = pd.DataFrame((consumption13), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost13 = pd.DataFrame((cost13), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data13 = df_collected_data13.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data13.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption13 = df_consumption13.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption13.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost13 = df_cost13.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost13.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start14 != None:

                date14 = start14[0:10]
                filename14 = start14[0:13]

                collected_data14 = [(date14, start14, stop14, price14, cons14, cost14, filename14)]
                consumption14 = [(date14, start14, cons_house14, cons_ev14, filename14)]
                cost14 = [(date14, start14, cost_house14, cost_ev14, filename14)]                                
                df_collected_data14 = pd.DataFrame((collected_data14), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption14 = pd.DataFrame((consumption14), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost14 = pd.DataFrame((cost14), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data14 = df_collected_data14.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data14.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption14 = df_consumption14.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption14.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost14 = df_cost14.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost14.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start15 != None:

                date15 = start15[0:10]
                filename15 = start15[0:13]

                collected_data15 = [(date15, start15, stop15, price15, cons15, cost15, filename15)]
                consumption15 = [(date15, start15, cons_house15, cons_ev15, filename15)]
                cost15 = [(date15, start15, cost_house15, cost_ev15, filename15)]                                
                df_collected_data15 = pd.DataFrame((collected_data15), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption15 = pd.DataFrame((consumption15), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost15 = pd.DataFrame((cost15), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data15 = df_collected_data15.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data15.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption15 = df_consumption15.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption15.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost15 = df_cost15.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost15.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start16 != None:

                date16 = start16[0:10]
                filename16 = start16[0:13]

                collected_data16 = [(date16, start16, stop16, price16, cons16, cost16, filename16)]
                consumption16 = [(date16, start16, cons_house16, cons_ev16, filename16)]
                cost16 = [(date16, start16, cost_house16, cost_ev16, filename16)]                                
                df_collected_data16 = pd.DataFrame((collected_data16), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption16 = pd.DataFrame((consumption16), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost16 = pd.DataFrame((cost16), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data16 = df_collected_data16.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data16.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption16 = df_consumption16.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption16.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost16 = df_cost16.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost16.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start17 != None:

                date17 = start17[0:10]
                filename17 = start17[0:13]

                collected_data17 = [(date17, start17, stop17, price17, cons17, cost17, filename17)]
                consumption17 = [(date17, start17, cons_house17, cons_ev17, filename17)]
                cost17 = [(date17, start17, cost_house17, cost_ev17, filename17)]                                
                df_collected_data17 = pd.DataFrame((collected_data17), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption17 = pd.DataFrame((consumption17), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost17 = pd.DataFrame((cost17), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data17 = df_collected_data17.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data17.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption17 = df_consumption17.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption17.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost17 = df_cost17.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost17.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start18 != None:

                date18 = start18[0:10]
                filename18 = start18[0:13]

                collected_data18 = [(date18, start18, stop18, price18, cons18, cost18, filename18)]
                consumption18 = [(date18, start18, cons_house18, cons_ev18, filename18)]
                cost18 = [(date18, start18, cost_house18, cost_ev18, filename18)]                                
                df_collected_data18 = pd.DataFrame((collected_data18), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption18 = pd.DataFrame((consumption18), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost18 = pd.DataFrame((cost18), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data18 = df_collected_data18.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data18.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption18 = df_consumption18.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption18.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost18 = df_cost18.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost18.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start19 != None:

                date19 = start19[0:10]
                filename19 = start19[0:13]

                collected_data19 = [(date19, start19, stop19, price19, cons19, cost19, filename19)]
                consumption19 = [(date19, start19, cons_house19, cons_ev19, filename19)]
                cost19 = [(date19, start19, cost_house19, cost_ev19, filename19)]                                
                df_collected_data19 = pd.DataFrame((collected_data19), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption19 = pd.DataFrame((consumption19), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost19 = pd.DataFrame((cost19), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data19 = df_collected_data19.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data19.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption19 = df_consumption19.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption19.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost19 = df_cost19.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost19.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start20 != None:

                date20 = start20[0:10]
                filename20 = start20[0:13]

                collected_data20 = [(date20, start20, stop20, price20, cons20, cost20, filename20)]
                consumption20 = [(date20, start20, cons_house20, cons_ev20, filename20)]
                cost20 = [(date20, start20, cost_house20, cost_ev20, filename20)]                                
                df_collected_data20 = pd.DataFrame((collected_data20), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption20 = pd.DataFrame((consumption20), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost20 = pd.DataFrame((cost20), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data20 = df_collected_data20.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data20.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption20 = df_consumption20.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption20.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost20 = df_cost20.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost20.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start21 != None:

                date21 = start21[0:10]
                filename21 = start21[0:13]

                collected_data21 = [(date21, start21, stop21, price21, cons21, cost21, filename21)]
                consumption21 = [(date21, start21, cons_house21, cons_ev21, filename21)]
                cost21 = [(date21, start21, cost_house21, cost_ev21, filename21)]                                
                df_collected_data21 = pd.DataFrame((collected_data21), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption21 = pd.DataFrame((consumption21), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost21 = pd.DataFrame((cost21), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data21 = df_collected_data21.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data21.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption21 = df_consumption21.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption21.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost21 = df_cost21.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost21.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start22 != None:

                date22 = start22[0:10]
                filename22 = start22[0:13]

                collected_data22 = [(date22, start22, stop22, price22, cons22, cost22, filename22)]
                consumption22 = [(date22, start22, cons_house22, cons_ev22, filename22)]
                cost22 = [(date22, start22, cost_house22, cost_ev22, filename22)]                                
                df_collected_data22 = pd.DataFrame((collected_data22), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption22 = pd.DataFrame((consumption22), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost22 = pd.DataFrame((cost22), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data22 = df_collected_data22.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data22.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption22 = df_consumption22.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption22.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost22 = df_cost22.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost22.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            if start23 != None:

                date23 = start23[0:10]
                filename23 = start23[0:13]

                collected_data23 = [(date23, start23, stop23, price23, cons23, cost23, filename23)]
                consumption23 = [(date23, start23, cons_house23, cons_ev23, filename23)]
                cost23 = [(date23, start23, cost_house23, cost_ev23, filename23)]                                
                df_collected_data23 = pd.DataFrame((collected_data23), columns=['date', 'start', 'stop', 'price', 'cons', 'cost', 'filename'])        
                df_consumption23 = pd.DataFrame((consumption23), columns=['date', 'start', 'cons_house', 'cons_ev', 'filename'])        
                df_cost23 = pd.DataFrame((cost23), columns=['date', 'start', 'cost_house', 'cost_ev', 'filename'])                                        

                # Storing in Google storage
                df_collected_data23 = df_collected_data23.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_collected_data23.iterrows():
                    bucket.blob('Collecteddata/{}.csv'.format(row[7])).upload_from_string(row[1:7].to_csv(header=False, index=False), 'text/csv')

                df_consumption23 = df_consumption23.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_consumption23.iterrows():
                    bucket.blob('Consumption/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

                df_cost23 = df_cost23.reset_index()  # make sure indexes pair with number of rows

                for index, row in df_cost23.iterrows():
                    bucket.blob('Cost/{}.csv'.format(row[5])).upload_from_string(row[1:5].to_csv(header=False, index=False), 'text/csv')

            ############################################################
            # Retrieving files from Google storage for chosen date
            ############################################################

            storage_client = storage.Client()
            bucket = storage_client.get_bucket(my_bucket)        
            my_prefix = "Collecteddata/"
            blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

            list_paths_gcs = []

            for blob in blobs:        
                if(blob.name != my_prefix):
                    paths = blob.name.replace(my_prefix, "")            
                    list_paths_gcs.append(paths)                

            #Filtering out all dates other than current date and making it into a list
            df_paths = pd.DataFrame((list_paths_gcs), columns=['path'])        
            df_paths["date"] = df_paths.path.str[0:10]
            list_chosendata= [chosendate]
            df_paths = df_paths.query('date in @list_chosendata')        
            df_paths = df_paths.drop(columns=['date'])                    
            list_paths2 = df_paths.to_records(index=False)
            list_paths3  = [tupleObj[0] for tupleObj in list_paths2]            
            #list_paths3 = list(list_paths2)

            #########################################################
            # Downloading selected files from Google storage
            #########################################################

            gcs_data_collected = []

            for blob_name in list_paths3:
                gcs_file_collected = bucket.get_blob('Collecteddata/'+blob_name).download_as_text().replace("\r\n", ",")            
                gcs_file_collected = gcs_file_collected.replace("\n", ",")                   
                gcs_data_collected.append(gcs_file_collected)            
        
            #Converting data into a Pandas dataframe
            df_data_collected = pd.DataFrame((gcs_data_collected), columns=['String'])
            df_data_collected[['date', 'start', 'stop', 'price', 'consumption', 'cost', 'Delete']]=df_data_collected["String"].str.split(",", expand=True)
            df_data_collected = df_data_collected.drop(columns=['String', 'Delete'])        

            gcs_data_consumption = []

            for blob_name in list_paths3:
                gcs_file_consumption = bucket.get_blob('Consumption/'+blob_name).download_as_text().replace("\r\n", ",")            
                gcs_file_consumption = gcs_file_consumption.replace("\n", ",")                
                gcs_data_consumption.append(gcs_file_consumption)            
        
            #Converting data into a Pandas dataframe
            df_data_consumption = pd.DataFrame((gcs_data_consumption), columns=['String'])
            df_data_consumption[['date', 'start', 'consumption_house', 'consumption_ev', 'Delete']]=df_data_consumption["String"].str.split(",", expand=True)
            df_data_consumption = df_data_consumption.drop(columns=['String', 'Delete'])        

            gcs_data_cost = []

            for blob_name in list_paths3:
                gcs_file_cost = bucket.get_blob('Cost/'+blob_name).download_as_text().replace("\r\n", ",")            
                gcs_file_cost = gcs_file_cost.replace("\n", ",")  
                gcs_data_cost.append(gcs_file_cost)            
        
            #Converting data into a Pandas dataframe
            df_data_cost = pd.DataFrame((gcs_data_cost), columns=['String'])
            df_data_cost[['date', 'start', 'cost_house', 'cost_ev', 'Delete']]=df_data_cost["String"].str.split(",", expand=True)
            df_data_cost = df_data_cost.drop(columns=['String', 'Delete'])        

            df_data1 = df_data_collected
            df_data2 = df_data_consumption
            df_data3 = df_data_cost

            #Transforming the lists into dataframes, droping unnecessary colums, transforming strings to floats, merger dataframes, rounding the figures to two decimals and transforming the dataframe to a list
            df_data2 = df_data2.drop(columns=['date'])
            df_data3 = df_data3.drop(columns=['date'])
            df_data1_2 = pd.merge(df_data1, df_data2, on="start")
            df_data1_3 = pd.merge(df_data1_2, df_data3, on="start")
            df_data1_3[["date2", "time"]] = df_data1_3.start.str.split("T", expand=True)
            df_data1_3[["hour", "min"]] = df_data1_3.time.str.split(":", expand=True)
            df_data1_3 = df_data1_3.drop(columns=['date2', 'time', 'min'])
            data = df_data1_3.values.tolist()

            #Adjusting the dataframe and aggregating the data by date
            df_aggr1 = df_data1_3.drop(columns=['start','stop', 'price'])
            df_aggr1['consumption'] = df_aggr1['consumption'].astype(float)
            df_aggr1['cost'] = df_aggr1['cost'].astype(float)
            df_aggr1['consumption_house'] = df_aggr1['consumption_house'].astype(float)
            df_aggr1['consumption_ev'] = df_aggr1['consumption_ev'].astype(float)
            df_aggr1['cost_house'] = df_aggr1['cost_house'].astype(float)
            df_aggr1['cost_ev'] = df_aggr1['cost_ev'].astype(float)
            df_aggr2 = df_aggr1.groupby(['date'], as_index=False).sum()
            rounded_df_aggr2 = df_aggr2.round(decimals=2)
            aggr = rounded_df_aggr2.values.tolist()

    return render_template("/updateday.html", data=data, chosendate=chosendate, aggr=aggr)  

@app.route("/viewamonth", methods=["GET", "POST"])
def viewamonth():

    if request.method == "GET":

        date_time = datetime.datetime.now()
        chosenmonth = date_time.strftime("%Y-%m")
        monthtoshow=chosenmonth
        chosenmonth = chosenmonth
        action = None
    
    if request.method == "POST":
        
        req = request.form
        chosenmonth=req.get("chosenmonth")
        chosenmonth2 = req.get("chosenmonth2")
        action=req.get("action")        
        monthtoshow=chosenmonth
        chosenmonth = chosenmonth
    
        if action == 'Renew monthly data':
            chosenmonth = chosenmonth2

    ############################################################
    # Retrieving files from Google storage for chosen date
    ############################################################

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(my_bucket)        

    # Check if monthly data is stored
    my_prefix = "Monthly/"
    blobs_monthly = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

    list_paths_monthly = []

    for blob in blobs_monthly:        
        if(blob.name != my_prefix):
            paths_monthly = blob.name.replace(my_prefix, "")            
            list_paths_monthly.append(paths_monthly)                

    df_monthly = pd.DataFrame((list_paths_monthly), columns=['filename'])        
    df_monthly["month"] = df_monthly.filename.str[0:7]
    list_chosenmonth= [chosenmonth]    
    df_monthly = df_monthly.query('month in @list_chosenmonth')            
    list_monthly = df_monthly.to_records(index=False)
    list_monthly  = [tupleObj[1] for tupleObj in list_monthly]

    if list_monthly != [] and action == None:

        month_csv = bucket.get_blob('Monthly/{}.csv'.format(chosenmonth)).download_as_text()

        #Converting data into a Pandas dataframe
        if '\r\n' in month_csv:
            df_month = pd.DataFrame([x.split(',') for x in month_csv.split('\r\n')])
        else:
            df_month = pd.DataFrame([x.split(',') for x in month_csv.split('\n')])           
        df_month.columns = ['date', 'consumption', 'cost', 'consumption_house', 'consumption_ev', 'cost_house', 'cost_ev']
        df_month = df_month[df_month['consumption'].notna()]
        df_month['consumption'] = df_month['consumption'].astype(float)  
        df_month['cost'] = df_month['cost'].astype(float)  
        df_month['consumption_house'] = df_month['consumption_house'].astype(float)  
        df_month['consumption_ev'] = df_month['consumption_ev'].astype(float)  
        df_month['cost_house'] = df_month['cost_house'].astype(float)  
        df_month['cost_ev'] = df_month['cost_ev'].astype(float)                                          

        #Making dataframe into list to be displayed on webpage
        data = df_month.values.tolist()

        #Adjusting the dataframe and aggregating the data by date
        df_aggr = df_month.drop(columns=['date'])
        df_aggr = df_aggr.agg(['sum'])
        rounded_df_aggr = df_aggr.round(decimals=2)
        aggr = rounded_df_aggr.values.tolist()

        #Retrieving all filenames and making then into a list of months
        my_prefix = "Collecteddata/"
        blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

        list_paths_gcs = []

        for blob in blobs:        
            if(blob.name != my_prefix):
                paths = blob.name.replace(my_prefix, "")            
                list_paths_gcs.append(paths)                

        df_paths = pd.DataFrame((list_paths_gcs), columns=['path'])        
        df_paths["month"] = df_paths.path.str[0:7]
        df_months = df_paths.drop(columns=['path'])
        df_months = df_months.drop_duplicates(subset=['month'])                          
        months = df_months.to_records(index=False)
        months_list = list(months)
        months_list  = [tupleObj[0] for tupleObj in months_list]    
        months_list.sort(reverse=True)

    if list_monthly == [] or action == 'Renew monthly data':

        my_prefix = "Collecteddata/"
        blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

        list_paths_gcs = []

        for blob in blobs:        
            if(blob.name != my_prefix):
                paths = blob.name.replace(my_prefix, "")            
                list_paths_gcs.append(paths)                

        #Retrieving all filenames and making then into a list of months
        df_paths = pd.DataFrame((list_paths_gcs), columns=['path'])        
        df_paths["month"] = df_paths.path.str[0:7]
        df_months = df_paths.drop(columns=['path'])
        df_months = df_months.drop_duplicates(subset=['month'])                          
        months = df_months.to_records(index=False)
        months_list = list(months)
        months_list  = [tupleObj[0] for tupleObj in months_list]    
        months_list.sort(reverse=True)   

        #Retrieving all filenames and making then into a list of available files
        list_chosenmonth= [chosenmonth]
        df_paths = df_paths.query('month in @list_chosenmonth')        
        df_paths = df_paths.drop(columns=['month'])                    
        list_paths2 = df_paths.to_records(index=False)
        list_paths3  = [tupleObj[0] for tupleObj in list_paths2]            
        #list_paths3 = list(list_paths2)    

        #########################################################
        # Downloading selected files from Google storage
        #########################################################

        gcs_data_collected = []

        for blob_name in list_paths3:
            gcs_file_collected = bucket.get_blob('Collecteddata/'+blob_name).download_as_text().replace("\r\n", ",")            
            gcs_file_collected = gcs_file_collected.replace("\n", ",")           
            gcs_data_collected.append(gcs_file_collected)            

        #Converting data into a Pandas dataframe
        df_data_collected = pd.DataFrame((gcs_data_collected), columns=['String'])
        df_data_collected[['date', 'start', 'stop', 'price', 'consumption', 'cost', 'Delete']]=df_data_collected["String"].str.split(",", expand=True)
        df_data_collected = df_data_collected.drop(columns=['String', 'Delete'])        

        gcs_data_consumption = []

        for blob_name in list_paths3:
            gcs_file_consumption = bucket.get_blob('Consumption/'+blob_name).download_as_text().replace("\r\n", ",")            
            gcs_file_consumption = gcs_file_consumption.replace("\n", ",")        
            gcs_data_consumption.append(gcs_file_consumption)            

        #Converting data into a Pandas dataframe
        df_data_consumption = pd.DataFrame((gcs_data_consumption), columns=['String'])
        df_data_consumption[['date', 'start', 'consumption_house', 'consumption_ev', 'Delete']]=df_data_consumption["String"].str.split(",", expand=True)
        df_data_consumption = df_data_consumption.drop(columns=['String', 'Delete'])        

        gcs_data_cost = []

        for blob_name in list_paths3:
            gcs_file_cost = bucket.get_blob('Cost/'+blob_name).download_as_text().replace("\r\n", ",")       
            gcs_file_cost = gcs_file_cost.replace("\n", ",")               
            gcs_data_cost.append(gcs_file_cost)            

        #Converting data into a Pandas dataframe
        df_data_cost = pd.DataFrame((gcs_data_cost), columns=['String'])
        df_data_cost[['date', 'start', 'cost_house', 'cost_ev', 'Delete']]=df_data_cost["String"].str.split(",", expand=True)
        df_data_cost = df_data_cost.drop(columns=['String', 'Delete'])        

        df_data1 = df_data_collected
        df_data2 = df_data_consumption
        df_data3 = df_data_cost

        #Transforming the lists into dataframes, droping unnecessary colums, transforming strings to floats, merger dataframes, rounding the figures to two decimals and transforming the dataframe to a list
        df_data1['cost'] = df_data1['cost'].astype(float)
        df_data2 = df_data2.drop(columns=['date'])
        df_data3 = df_data3.drop(columns=['date'])
        df_data1_2 = pd.merge(df_data1, df_data2, on="start")
        df_data1_2_3 = pd.merge(df_data1_2, df_data3, on="start")
        df_data1_2_3b = df_data1_2_3.drop(columns=['start', 'stop', 'price'])
        df_data1_2_3b['consumption'] = df_data1_2_3b['consumption'].astype(float)
        df_data1_2_3b['cost'] = df_data1_2_3b['cost'].astype(float)
        df_data1_2_3b['consumption_house'] = df_data1_2_3b['consumption_house'].astype(float)
        df_data1_2_3b['consumption_ev'] = df_data1_2_3b['consumption_ev'].astype(float)
        df_data1_2_3b['cost_house'] = df_data1_2_3b['cost_house'].astype(float)
        df_data1_2_3b['cost_ev'] = df_data1_2_3b['cost_ev'].astype(float)
        df_aggr2 = df_data1_2_3b.groupby(['date'], as_index=False).sum()

        rounded_df_aggr2 = df_aggr2.round(decimals=3)
        data = rounded_df_aggr2.values.tolist()

        #Saving data for month in Google storage
        bucket.blob('Monthly/{}.csv'.format(chosenmonth)).upload_from_string(rounded_df_aggr2.to_csv(header=False, index=False), 'text/csv')    

        #Adjusting the dataframe and aggregating the data by date
        df_aggr = rounded_df_aggr2.drop(columns=['date'])
        df_aggr = df_aggr.agg(['sum'])
        rounded_df_aggr = df_aggr.round(decimals=2)
        aggr = rounded_df_aggr.values.tolist()

    return render_template("/viewamonth.html", months_list=months_list, data=data, monthtoshow=monthtoshow, aggr=aggr, chosenmonth=chosenmonth)

@app.route("/totalcostmonth", methods=["GET", "POST"])
def totalcostmonth():

    if request.method == "GET":

        date_time = datetime.datetime.now()
        costmonth = date_time.strftime("%Y-%m")
        monthtoshow=costmonth
        costmonth = costmonth
        action = ""

    if request.method == "POST":
            
        req = request.form   #Lagrer i variablen req data som request lager en fin diconary av
        costmonth = req.get("costmonth")
        action = req.get("action")

        if action[0:9] == "View sele":
            monthtoshow=costmonth
            costmonth = costmonth
        
        if action[0:9] == "View year":
            date_time = datetime.datetime.now()
            costmonth = date_time.strftime("%Y")
            monthssofar = int(date_time.strftime("%m"))
            monthtoshow=costmonth
            costmonth = costmonth


    storage_client = storage.Client()
    bucket = storage_client.get_bucket(my_bucket)    

    #Retrieving all filenames and making then into a list of months
    my_prefix = "Monthly/"
    blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

    list_paths_gcs = []

    for blob in blobs:        
        if(blob.name != my_prefix):
            paths = blob.name.replace(my_prefix, "")            
            list_paths_gcs.append(paths)                

    df_paths = pd.DataFrame((list_paths_gcs), columns=['path'])        
    df_paths["month"] = df_paths.path.str[0:7]
    df_months = df_paths.drop(columns=['path'])
    df_months = df_months.drop_duplicates(subset=['month'])                          
    months = df_months.to_records(index=False)
    months_list = list(months)
    months_list  = [tupleObj[0] for tupleObj in months_list]    
    months_list.sort(reverse=True)

    fixedmontlycost=session.get("fixedmontlycost")
    fixedkwhcost=session.get("fixedkwhcost")    

    #Flashing message if necessary data is not input
    if fixedmontlycost == None and fixedkwhcost == None:
        flash("You have to first input fixed cost to get total costs a given month!")
        return redirect("/setup")

    ############################################################
    # Retrieving files from Google storage for year or month
    ############################################################

    if action[0:9] == "View year":

        my_prefix = "Monthly/"
        blobs_monthly = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

        list_paths_monthly = []

        for blob in blobs_monthly:        
            if(blob.name != my_prefix):
                paths_monthly = blob.name.replace(my_prefix, "")            
                list_paths_monthly.append(paths_monthly)                

        df_monthly = pd.DataFrame((list_paths_monthly), columns=['filename'])        
        list_monthly = df_monthly.to_records(index=False)
        list_monthly  = [tupleObj[0] for tupleObj in list_monthly]

        df_year = pd.DataFrame({"date":[''],"consumption":[0],"cost":[0],"consumption_house":[0],"consumption_ev":[0],"cost_house":[0],"cost_ev":[0]})

        for blob_name in list_monthly:
            month_csv = bucket.get_blob('Monthly/'+blob_name).download_as_text()

            #Converting data into a Pandas dataframe
            if '\r\n' in month_csv:
                df_month = pd.DataFrame([x.split(',') for x in month_csv.split('\r\n')])
            else:
                df_month = pd.DataFrame([x.split(',') for x in month_csv.split('\n')])           
            df_month.columns = ['date', 'consumption', 'cost', 'consumption_house', 'consumption_ev', 'cost_house', 'cost_ev']
            df_month = df_month[df_month['consumption'].notna()]
            df_month['consumption'] = df_month['consumption'].astype(float)  
            df_month['cost'] = df_month['cost'].astype(float)  
            df_month['consumption_house'] = df_month['consumption_house'].astype(float)  
            df_month['consumption_ev'] = df_month['consumption_ev'].astype(float)  
            df_month['cost_house'] = df_month['cost_house'].astype(float)  
            df_month['cost_ev'] = df_month['cost_ev'].astype(float)                                          
            df_year = pd.concat([df_year, df_month], ignore_index=True)
        
        df_year.drop(df_year.index[0], inplace=True)
        df_cost = df_year

    if request.method == "GET" or action[0:9] == "View sele":

        month_csv = bucket.get_blob('Monthly/{}.csv'.format(costmonth)).download_as_text()

        #Converting data into a Pandas dataframe
        if '\r\n' in month_csv:
            df_month = pd.DataFrame([x.split(',') for x in month_csv.split('\r\n')])
        else:
            df_month = pd.DataFrame([x.split(',') for x in month_csv.split('\n')])           
        df_month.columns = ['date', 'consumption', 'cost', 'consumption_house', 'consumption_ev', 'cost_house', 'cost_ev']
        df_month = df_month[df_month['consumption'].notna()]
        df_month['consumption'] = df_month['consumption'].astype(float)  
        df_month['cost'] = df_month['cost'].astype(float)  
        df_month['consumption_house'] = df_month['consumption_house'].astype(float)  
        df_month['consumption_ev'] = df_month['consumption_ev'].astype(float)  
        df_month['cost_house'] = df_month['cost_house'].astype(float)  
        df_month['cost_ev'] = df_month['cost_ev'].astype(float)                                                  
        df_cost = df_month

    #Making dataframe into list to be displayed on webpage
    data = df_cost.values.tolist()

    df_data1_2_3b = df_cost

    #Adjusting the dataframe and aggregating data by date
    df_aggr = df_data1_2_3b.drop(columns=['date'])
    df_aggr = df_aggr.agg(['sum'])
    rounded_df_aggr = df_aggr.round(decimals=2)
    aggr = rounded_df_aggr.values.tolist()

    #Calculating various cost elements
    if request.method == "POST" and action[0:9] == "View year":
        cost_house_ellevio = round((float(aggr[0][2]) * float(fixedkwhcost)) + float(fixedmontlycost)*monthssofar, 2)
    else:
        cost_house_ellevio = round((float(aggr[0][2]) * float(fixedkwhcost)) + float(fixedmontlycost), 2)        
    cost_ev_ellevio = round((float(aggr[0][3]) * float(fixedkwhcost)), 2)
    total_cost_ellevio = round(cost_house_ellevio + cost_ev_ellevio, 2)
    
    total_cost = round(aggr[0][1] + total_cost_ellevio, 2)
    total_cost_tibber_per_kwh = round(aggr[0][1]/aggr[0][0], 2)
    total_cost_per_kwh = round(total_cost/aggr[0][0], 2)

    if aggr[0][3] != 0:
        cost_ev_total = round(aggr[0][5] + cost_ev_ellevio, 2)
        ev_cost_tibber_per_kwh = round(aggr[0][5] / aggr[0][3], 2) 
        cost_ev_per_kwh_total = round(cost_ev_total / aggr[0][3], 2)
    else:
        cost_ev_total = 0
        ev_cost_tibber_per_kwh = 0
        cost_ev_per_kwh_total = 0

    cost_house_total = round(aggr[0][4] + cost_house_ellevio, 2)
    house_cost_tibber_per_kwh = round(aggr[0][4] / aggr[0][2], 2)
    cost_house_per_kwh_total = round(cost_house_total / aggr[0][2], 2)

    return render_template("/totalcostmonth.html", months_list=months_list, aggr=aggr, cost_house_ellevio=cost_house_ellevio, cost_ev_ellevio=cost_ev_ellevio, total_cost_ellevio=total_cost_ellevio, monthtoshow=monthtoshow, total_cost = total_cost, total_cost_tibber_per_kwh = total_cost_tibber_per_kwh, total_cost_per_kwh = total_cost_per_kwh, cost_ev_total = cost_ev_total, ev_cost_tibber_per_kwh = ev_cost_tibber_per_kwh, cost_ev_per_kwh_total = cost_ev_per_kwh_total, cost_house_total = cost_house_total, house_cost_tibber_per_kwh = house_cost_tibber_per_kwh, cost_house_per_kwh_total = cost_house_per_kwh_total)


@app.route("/viewconsumption", methods=["GET", "POST"])
def viewconsumption():

    if request.method == "GET":

        return render_template("viewconsumption.html")

    if request.method == "POST":

        req = request.form 
        action = req.get("action")

        if action[10:12] == "24":
            hourstocollect = 24
        
        if action[10:12] == "48":
            hourstocollect = 48

        if action[10:12] == "72":
            hourstocollect = 72
         
        ###############################################################################################
        #Collecting data from Tibber and saving it in lists that are merged and made into a tuple list
        ###############################################################################################
        account=tibber.Account(tibber_token)
        home = account.homes[0]
        hour_data = home.fetch_consumption("HOURLY", last=hourstocollect)
       
        start=[]
        stop=[]
        price=[]
        cons=[]
        cost=[]

        for hour in hour_data:
            data1=(hour.from_time)
            data2=(hour.to_time)
            data3=(f"{hour.unit_price}{hour.currency}")
            data4=(hour.consumption)
            data5=(hour.cost)
            start.append(data1)
            stop.append(data2)
            price.append(data3)
            cons.append(data4)
            cost.append(data5)        

        #Removing unnecessary info from the date variable
        start = [d[:-13] for d in start]
        stop = [d[:-13] for d in stop]

        #Removing SEK from the list containing prices
        price = ([s.replace('SEK', '') for s in price])

        #Merging all lists of data to one tuple list and transforming it into a dataframe
        def merge(stop,price,cons,cost,start):
            merged_list = [(stop[i], price[i], cons[i], cost[i],start[i]) for i in range(0, len(start))]
            return merged_list
        data = merge(stop,price,cons,cost,start)

    return render_template("/viewconsumption.html", data=data, hourstocollect=hourstocollect)


@app.route("/viewprices", methods=["GET", "POST"])
def viewprices():

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(my_bucket)        

    if request.method == "GET":

        # Retrieving deciles file from Google Storage
        deciles = bucket.get_blob('deciles.csv').download_as_text()

        if '\r\n' in deciles:
            df_deciles = pd.DataFrame([x.split(',') for x in deciles.split('\r\n')])
        else:
            df_deciles = pd.DataFrame([x.split(',') for x in deciles.split('\n')])
        df_deciles.columns = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
        df_deciles = df_deciles.drop(df_deciles.index[[10]])
        df_deciles['max'] = df_deciles['max'].astype(float)  

        #Creating new objects for min and max values
        max_0 = df_deciles.at[0, 'max']
        max_1 = df_deciles.at[1, 'max']        
        max_2 = df_deciles.at[2, 'max']        
        max_3 = df_deciles.at[3, 'max']
        max_6 = df_deciles.at[6, 'max']
        max_7 = df_deciles.at[7, 'max']        
        max_8 = df_deciles.at[8, 'max']

        #Function for rating the energy price
        def rating(value):
            if value <= max_0:
                return "Very cheap"
            elif max_0 < value <= max_1:
                return "Very cheap"
            elif max_1 < value <= max_2:
                return "Cheap"                
            elif max_2 < value <= max_6:
                return "Normal"
            elif max_6 < value <= max_7:
                return "Expensive"
            elif max_7 < value <= max_8:
                return "Very expensive"
            elif value > max_8:
                return "Very expensive"

        decile = ['First', 'Second', 'Third', 'Fourth', 'Fifth', 'Sixth', 'Seventh', 'Eighth', 'Ninth', 'Tenth']
        rate = ['Very cheap', 'Very cheap', 'Cheap', 'Cheap', 'Normal', 'Normal', 'Expensive', 'Expensive', 'Very expensive', 'Very expensive']        
        df_deciles['decile'] = decile
        df_deciles['rating'] = rate

        #Converting the dataframe into a list of tuples
        deciles = df_deciles.values.tolist()

        #Calculating the number of days the rating is based on
        df_days = df_deciles[['count']]
        df_days['count'] = df_days['count'].astype(float)
        df_days = df_days.agg(['sum'])
        days = df_days.values.tolist()        
        numberofdays = round(days[0][0]/24, 0)
       

    if request.method == "POST":

        req = request.form
        numberofdays = float(req["numberofdays"])

        #Calculating the date 180 days prior to today
        fromdate = date.today() - relativedelta(days=+numberofdays)
        fromdate = fromdate.strftime("%Y-%m-%d") 

        ############################################################
        # Retrieving files from Google storage for chosen date
        ############################################################

        my_prefix = "Collecteddata/"
        blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

        list_paths_gcs = []

        for blob in blobs:        
            if(blob.name != my_prefix):
                paths = blob.name.replace(my_prefix, "")            
                list_paths_gcs.append(paths)                

        #Retrieving all filenames and making then into a list of months
        df_paths = pd.DataFrame((list_paths_gcs), columns=['path'])        
        df_paths["date"] = df_paths.path.str[0:10]
        list_fromdate= [fromdate]
        df_paths = df_paths[df_paths.date > fromdate]
        df_paths = df_paths.drop(columns=['date'])                    
        list_paths2 = df_paths.to_records(index=False)
        list_paths3  = [tupleObj[0] for tupleObj in list_paths2]            
        #list_paths3 = list(list_paths2)    

        #########################################################
        # Downloading selected files from Google storage
        #########################################################

        gcs_data_collected = []

        for blob_name in list_paths3:
            gcs_file_collected = bucket.get_blob('Collecteddata/'+blob_name).download_as_text().replace("\r\n", ",")            
            gcs_file_collected = gcs_file_collected.replace("\n", ",")               
            gcs_data_collected.append(gcs_file_collected)            

        #Converting data into a Pandas dataframe
        df_data_collected = pd.DataFrame((gcs_data_collected), columns=['String'])
        df_data_collected[['date', 'start', 'stop', 'price', 'consumption', 'cost', 'Delete']]=df_data_collected["String"].str.split(",", expand=True)
        df_data_collected = df_data_collected.drop(columns=['String', 'Delete'])        

        df_data1 = df_data_collected

        #Converting the list of tuples into a dataframe
        df_allprices = df_data1.drop(columns=['start', 'stop', 'consumption', 'cost'])    
        df_allprices['price'] = df_allprices['price'].astype(float)    
        mean_price = df_allprices['price'].mean()

        #Sorting data by ascending price and deviding into ten groups of equal size
        df_allprices = df_allprices.sort_values('price', ascending=True)
        df_allprices['Tengroups'] = pd.qcut(df_allprices['price'], 10, labels = False)        

        #Calculating the min and max value and more for each group
        df_deciles = df_allprices.set_index("Tengroups").select_dtypes(np.number).stack().groupby(level=0).describe()         

        #Saving rating in Google storage
        bucket.blob('deciles.csv').upload_from_string(df_deciles.to_csv(header=False, index=False), 'text/csv')

        #Creating new objects for min and max values
        max_0 = df_deciles.at[0, 'max']
        max_1 = df_deciles.at[1, 'max']        
        max_2 = df_deciles.at[2, 'max']        
        max_3 = df_deciles.at[3, 'max']
        max_6 = df_deciles.at[6, 'max']
        max_7 = df_deciles.at[7, 'max']        
        max_8 = df_deciles.at[8, 'max']

        #Function for rating the energy price
        def rating(value):
            if value <= max_0:
                return "Very cheap"
            elif max_0 < value <= max_1:
                return "Very cheap"
            elif max_1 < value <= max_2:
                return "Cheap"                
            elif max_2 < value <= max_6:
                return "Normal"
            elif max_6 < value <= max_7:
                return "Expensive"
            elif max_7 < value <= max_8:
                return "Very expensive"
            elif value > max_8:
                return "Very expensive"

        decile = ['First', 'Second', 'Third', 'Fourth', 'Fifth', 'Sixth', 'Seventh', 'Eighth', 'Ninth', 'Tenth']
        rate = ['Very cheap', 'Very cheap', 'Cheap', 'Cheap', 'Normal', 'Normal', 'Expensive', 'Expensive', 'Very expensive', 'Very expensive']        
        df_deciles['decile'] = decile
        df_deciles['rating'] = rate

        #Converting the dataframe into a list of tuples
        deciles = df_deciles.values.tolist()


    ################################################################
    # Collecting energy prices for today and tomorrow from Tibber
    ################################################################        

    account=tibber.Account(tibber_token)
    home = account.homes[0]
    current_subscription = home.current_subscription
    price_now = current_subscription.price_info.current.total
    price_level = current_subscription.price_info.current.level
    price_info_today = current_subscription.price_info.today
    price_info_tomorrow = current_subscription.price_info.tomorrow

    rating_now = rating(price_now)


    # Collecting today's prices
    ##############################

    total_td=[]
    energy_td=[]
    tax_td=[]
    starts_at_td=[]
    currency_td=[]
    level_td=[]

    for hour in price_info_today:
        data1=(hour.total)
        data2=(hour.energy)
        data3=(hour.tax)
        data4=(hour.starts_at)
        data5=(hour.currency)
        data6=(hour.level)   

        total_td.append(data1)
        energy_td.append(data2)
        tax_td.append(data3)
        starts_at_td.append(data4)
        currency_td.append(data5)
        level_td.append(data6)

    #Removing unnecessary info from the date variable
    starts_at_td = [d[11:-13] for d in starts_at_td]

    #Merging all lists of data to one tuple list
    def merge(total_td,energy_td,tax_td,starts_at_td,currency_td,level_td):
        merged_list = [(total_td[i], energy_td[i], tax_td[i], starts_at_td[i],currency_td[i],level_td[i]) for i in range(0, len(total_td))]
        return merged_list
    prices_today = merge(total_td,energy_td,tax_td,starts_at_td,currency_td,level_td)

    #Converting the list of tuples into a dataframe and adding a rating column
    df_prices_today = pd.DataFrame((prices_today), columns=['total', 'energy', 'tax', 'starts_at', 'currency', 'level'])
    df_prices_today['rating'] = df_prices_today['total'].map(rating)

    #Converting the dataframe into a list of tuples
    prices_today = df_prices_today.values.tolist()

    # Collecting tomorrow's prices
    ##############################

    total_tm=[]
    energy_tm=[]
    tax_tm=[]
    starts_at_tm=[]
    currency_tm=[]
    level_tm=[]

    for hour in price_info_tomorrow:
        data1=(hour.total)
        data2=(hour.energy)
        data3=(hour.tax)
        data4=(hour.starts_at)
        data5=(hour.currency)
        data6=(hour.level)   

        total_tm.append(data1)
        energy_tm.append(data2)
        tax_tm.append(data3)
        starts_at_tm.append(data4)
        currency_tm.append(data5)
        level_tm.append(data6)

    #Removing unnecessary info from the date variable
    starts_at_tm = [d[11:-13] for d in starts_at_tm]            

    #Merging all lists of data to one tuple list
    def merge(total_tm,energy_tm,tax_tm,starts_at_tm,currency_tm,level_tm):
        merged_list = [(total_tm[i], energy_tm[i], tax_tm[i], starts_at_tm[i],currency_tm[i],level_tm[i]) for i in range(0, len(total_tm))]
        return merged_list
    prices_tomorrow = merge(total_tm,energy_tm,tax_tm,starts_at_tm,currency_tm,level_tm)

    #Converting the list of tuples into a dataframe and adding a rating column
    df_prices_tomorrow = pd.DataFrame((prices_tomorrow), columns=['total', 'energy', 'tax', 'starts_at', 'currency', 'level'])
    df_prices_tomorrow['rating'] = df_prices_tomorrow['total'].map(rating)

    #Converting the dataframe into a list of tuples
    prices_tomorrow = df_prices_tomorrow.values.tolist()

    return render_template("/viewprices.html", prices_today=prices_today, prices_tomorrow=prices_tomorrow, price_now=price_now, price_level = price_level, deciles=deciles, rating_now=rating_now, numberofdays=numberofdays)

@app.route("/setup", methods=["GET", "POST"])
def setup():

    if request.method == "POST":

        req = request.form 

        fixedmontlycost = req["fixedmontlycost"]
        fixedkwhcost    = req["fixedkwhcost"]

        session["fixedmontlycost"] = fixedmontlycost
        session["fixedkwhcost"] =   fixedkwhcost      

        #Flasher beskjeder dersom input data er lagret
        if fixedmontlycost != '' and fixedkwhcost != '':
            flash("Figures are successfully saved for this session")
            return redirect("/setup")
    
    return render_template("/setup.html")


@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(403)
def forbidden(e):
    return render_template('403.html'), 403