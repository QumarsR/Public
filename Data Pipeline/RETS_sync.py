#! /usr/bin/python3.8
#coding: utf-8

## Imports Libraries
from rets import session
from pymongo import MongoClient
import numpy as np
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import logging
import threading
import concurrent.futures
import configparser
import pandas as pd
import dateutil
from dateutil import parser
from datetime import datetime
from datetime import timedelta
import time
from multiprocessing.pool import ThreadPool
import sys, string, os


## open and read config file

    
for i in range(10):
    print('cycle starting in ',10-i,' seconds')
    time.sleep(1)
####################################################################################################   
read_config = configparser.ConfigParser()
read_config.read("/home/ubuntu/github/Mongo/python_scripts/config.ini")

repeat_time = int(read_config.get("settings","repeat_time"))



res_collection = read_config.get("mongofields","res_collection")
res_history = read_config.get("mongofields","res_history")
condo_collection = read_config.get("mongofields","condo_collection")
condo_history = read_config.get("mongofields","condo_history")
database = read_config.get("mongofields","database")
server_string = read_config.get("mongofields","server_string")

login_url =read_config.get("retsfields","login_url")
username = read_config.get("retsfields","username")
password = read_config.get("retsfields","password")
idx_login_url =read_config.get("retsfields","idx_login_url")
idx_username = read_config.get("retsfields","idx_username")
idx_password = read_config.get("retsfields","idx_password")
version = read_config.get("retsfields","version")
days_back = int(read_config.get("retsfields","days_back"))
dates = read_config.get("retsfields","dates")

bucket = read_config.get("image_upload","bucket")
access_key = read_config.get("image_upload","access_key")
secret_key = read_config.get("image_upload","secret_key")
threads = int(read_config.get("image_upload","threads"))


dates = dates.replace("'","").replace("\n","").replace("\\","").split(',')

################################################################################################

start_time = datetime.now()
log = open("/home/ubuntu/github/python_scripts/rets_logs/log-{}.txt".format(start_time.strftime("%Y-%m-%d--%H%M%S")), "a")

## Connect to MongoDB
log.write('----------MongoDB---------\n')
try:
    connection = MongoClient(server_string)
    exec('db = connection.'+database)
    print(1)
    exec('res_last_state = db.'+str(res_collection))
    print(2)
    exec('res_history = db.'+str(res_history))
    print(3)
    exec('condo_last_state = db.'+ str(condo_collection))
    print(4)
    exec('condo_history = db.'+str(condo_history))
    print('connection successful')
    log.write('connection successful \n')
except Exception as e :
    print(e)
    log.write(str(e)+ '\n')




log.write('----------TRREB---------\n')
## establish connection with TREB
try:
    rets_client = session.Session(login_url, username,password,version, timeout = 600)
    print(1)
    rets_client.login()
    print(2)
    system_data = rets_client.get_system_metadata()
    log.write('system data'+str(system_data)+ '\n')
    
    idx_rets_client =  session.Session(idx_login_url, idx_username,idx_password,version)
    idx_rets_client.login()
    log.write('idx login successful \n')
except Exception as e:
    print(str(e))
    print('connection to TRREB failed')
    log.write(str(e))
    log.close()
    exit()
    #time.sleep(repeat_time * 60)
    

##########################################################################################################################

##get todays datetime
now = datetime.now()
delta = timedelta(days = 1)
#search_date1 = (now).strftime("%Y-%m-%dT%X")
search_date = (now -(delta*days_back)).strftime("%Y-%m-%d")#T%X")
log.write('search date = '+ search_date +'\n')

##########################################################################################################################


##fill list with records from TRREB
rets_list_res = []
rets_list_condo = []

#ResidentialProperty for Res 
#CondoProperty for Condo
print('getting data from TRREB')
print('searching: ', search_date)
start = datetime.now()
# 
pix_list = []
timestamp_list = []
try:
    search_result = rets_client.search(resource='Property',resource_class='ResidentialProperty',dmql_query = "(pix_updt = "+search_date+"+)")
    for i in search_result: 
        pix_list.append(i)
    print('residential data gathered - pix_updt')
    log.write('residential pix_updt gathered successfully \n')
    
    
    search_result = rets_client.search(resource='Property',resource_class='ResidentialProperty',dmql_query = "(timestamp_sql = "+search_date+"+)")
    for i in search_result: 
        timestamp_list.append(i)
    print('residential data gathered - timestamp_sql') 
    log.write('residential timestamp_sql gathered successfully \n')
    
    mls_list = []
    for t in timestamp_list:
        mls_list.append(t['Ml_num'])
        rets_list_res.append(t)
    for p in pix_list:
        if p['Ml_num'] not in mls_list:
            mls_list.append(p['Ml_num'])
            rets_list_res.append(p)
    
    
    
    log.write('total res records gathered ='+ str(len(rets_list_res))+ '\n')
except Exception as e:
    print('issue with getting residential data')
    log.write('issue with getting residential data \n')
    log.write(str(e)+ '\n')
    print(e)

end = datetime.now()
log.write('residential pull time = '+str(end-start)+ '\n')
print(end-start) 
    
start = datetime.now()
pix_list = []
timestamp_list = []
try:
    search_result = rets_client.search(resource='Property',resource_class='CondoProperty',dmql_query = "(pix_updt = "+search_date+"+)")
    for i in search_result: 
        pix_list.append(i)
    print('condo data gathered - pix_updt')
    log.write('condo pix_updt gathered successfully \n')
    
    search_result = rets_client.search(resource='Property',resource_class='CondoProperty',dmql_query = "(timestamp_sql = "+search_date+"+)")
    for i in search_result: 
        timestamp_list.append(i)
    print('condo data gathered - timestamp_sql') 
    log.write('condo timestamp_sql gathered successfully \n')
    
    mls_list = []
    for t in timestamp_list:
        mls_list.append(t['Ml_num'])
        rets_list_condo.append(t)
    for p in pix_list:
        if p['Ml_num'] not in mls_list:
            mls_list.append(p['Ml_num'])
            rets_list_condo.append(p)
    
    log.write('total condo records gathered ='+ str(len(rets_list_condo))+ '\n')
except Exception as e:
    print('issue with getting condo data')
    print(e)    
    log.write('issue with getting condo data \n')
    log.write(str(e)+ '\n')        
end = datetime.now()
print(end-start)  
log.write('condo pull time = '+str(end-start)+ '\n')

### queries for idx_mls numbers 
idx_timestamp_list = []
idx_pix_list = []
idx_res_mls = {}
idx_condo_mls = {}
try:
    search_result = idx_rets_client.search(resource='Property',resource_class='ResidentialProperty',dmql_query = "(pix_updt = "+search_date+"+)")
    for i in search_result: 
        idx_pix_list.append(i)
    print('residential data gathered - pix_updt - IDX')
    
    search_result = idx_rets_client.search(resource='Property',resource_class='ResidentialProperty',dmql_query = "(timestamp_sql = "+search_date+"+)")
    for i in search_result: 
        idx_timestamp_list.append(i)
    print('residential data gathered - timestamp_sql - IDX') 
   
    for t in idx_timestamp_list:
        idx_res_mls[t['Ml_num']] = True
    for p in idx_pix_list:
        idx_res_mls[p['Ml_num']] = True
    
    
    idx_timestamp_list = []
    idx_pix_list = []
    
        
    search_result = idx_rets_client.search(resource='Property',resource_class='CondoProperty',dmql_query = "(pix_updt = "+search_date+"+)")
    for i in search_result: 
        idx_pix_list.append(i)
    print('Condo data gathered - pix_updt - IDX')
    
    search_result = idx_rets_client.search(resource='Property',resource_class='CondoProperty',dmql_query = "(timestamp_sql = "+search_date+"+)")
    for i in search_result: 
        idx_timestamp_list.append(i)
    print('Condo data gathered - timestamp_sql - IDX') 
    
    for t in idx_timestamp_list:
        idx_condo_mls[t['Ml_num']] = True
    for p in idx_pix_list:
        idx_condo_mls[p['Ml_num']] = True
        
    log.write('\n --------IDX Records --------- \n')
    for i in idx_res_mls:
        log.write(str(i)+'\n')
    for i in idx_condo_mls:
        log.write(str(i)+'\n')

except Exception as e:
    print(e)
    log.write(str(e))

##########################################################################################################################

## transform strings in date fields to datetime objects for RES
log.write('\n----------Data Cleansing---------\n')
print('cleansing data')

start = datetime.now()
for record in rets_list_res:
    record['pictures_downloaded'] = False
    record['photo_number_list'] = ""
    record['pic_retry_date'] = ""
    record['oteq_revise_date'] = datetime.now()
    for date in dates:
        try:
            exec("record['"+date+"'] = parser.parse(record['"+date+"'])")
            
        except Exception as e:
            if str(e) == 'Parser must be a string or character stream, not datetime':
                log.write(str(record['Ml_num'])+ " - " + str(date)+" - " +str(e)+ '\n')
            else:
                exec("record['"+date+"'] = None")
    
    

##replaces capital field names with lowercase
for i in range(len(rets_list_res)):
    rets_list_res[i] = dict((k.lower(), v) for k,v in rets_list_res[i].items())
    
end = datetime.now()
print('residential data cleansed')
print(end-start)

## transform strings in date fields to datetime objects for CONDO
start = datetime.now()
for record in rets_list_condo:
    record['pictures_downloaded'] = False
    record['photo_number_list'] = ""
    record['pic_retry_date'] = None
    record['oteq_revise_date'] = datetime.now()
    for date in dates:
        try:
            exec("record['"+date+"'] = parser.parse(record['"+date+"'])")
            
        except Exception as e:
            if str(e) == 'Parser must be a string or character stream, not datetime':
                log.write(str(record['Ml_num'])+ " - " + str(date)+" - " +str(e)+ '\n')
            else:
                exec("record['"+date+"'] = None")
    
    

##replaces capital field names with lowercase
for i in range(len(rets_list_condo)):
    rets_list_condo[i] = dict((k.lower(), v) for k,v in rets_list_condo[i].items())
    
end = datetime.now()
print('condo data cleansed')
print(end-start)    

##########################################################################################################################


def upload(rets_list, col_last_state, col_history,idx_mls):
            
    new = 0
    update = 0
    duplicate = 0
    pix_update = 0
    
    for record in rets_list:
        if record['ml_num'] in idx_mls:
            record['in_idx'] = True
        else:
            record['in_idx'] = False
        ##checks for duplicate
        if col_last_state.count_documents({'ml_num': record['ml_num'],'timestamp_sql': record['timestamp_sql'],
                                       'pix_updt': record['pix_updt']}) >0:
            print('duplicate record')
            log.write(str(record['ml_num']) + " is duplicate \n")
            duplicate +=1
            
             
        ##checks for update 
        elif col_last_state.count_documents({'ml_num': record['ml_num']}) >0:
            
            temp = col_last_state.find({'ml_num':record['ml_num']})
            
            ## checks if the record has a newer timestamp or pix_updt compared to db
            ##pix updated
            if (temp[0]['pix_updt'] is None) and (record['pix_updt'] is not None):
                print('pix update for existing record')
                col_history.insert_one(temp[0])
                col_last_state.insert_one(record)
                col_last_state.delete_one({'ml_num':temp[0]['ml_num'],'timestamp_sql':temp[0]['timestamp_sql'],
                                              'pix_updt':temp[0]['pix_updt']})
                pix_update += 1
                log.write(str(record['ml_num']) + " is a pix update \n")
                
                

                
                
                
            ##timestamp updated
            elif (temp[0]['pix_updt'] is None) and (record['pix_updt'] is None):
                print('update for existing record')
                #print(temp[0])
                col_history.insert_one(temp[0])
                record['pictures_downloaded'] = temp[0]['pictures_downloaded']
                record['photo_number_list'] = temp[0]['photo_number_list']
                col_last_state.insert_one(record)
                col_last_state.delete_one({'ml_num':temp[0]['ml_num'],'timestamp_sql':temp[0]['timestamp_sql'],
                                              'pix_updt':temp[0]['pix_updt']})
                log.write(str(record['ml_num']) + " is an update \n")
                update +=1
               
            
            
            ##pix or timestamp updated
            elif (temp[0]['pix_updt'] is not None) and (record['pix_updt'] is not None):
                ##pix updated
                if (record['pix_updt'] > temp[0]['pix_updt']):
                    print('pix update for existing record')
                    col_history.insert_one(temp[0])
                    col_last_state.insert_one(record)
                    col_last_state.delete_one({'ml_num':temp[0]['ml_num'],'timestamp_sql':temp[0]['timestamp_sql'],
                                              'pix_updt':temp[0]['pix_updt']})
                    pix_update += 1
                    log.write(str(record['ml_num']) + " is a pix update \n")
                  
                ##timestamp updated
                else:
                    print('update for existing record')
                    #print(temp[0])
                    record['pictures_downloaded'] = temp[0]['pictures_downloaded']
                    record['photo_number_list'] = temp[0]['photo_number_list']
                    try:
                        record['pic_retry_date'] = temp[0]['pic_retry_date']
                    except Exception as e:
                        print(i)
                        print(e)                              
                    col_history.insert_one(temp[0])
                    col_last_state.insert_one(record)
                    col_last_state.delete_one({'ml_num':temp[0]['ml_num'],'timestamp_sql':temp[0]['timestamp_sql'],
                                              'pix_updt':temp[0]['pix_updt']})
                    update +=1
                    log.write(str(record['ml_num']) + " is an update \n")
                    
                    
            ##just timestamp updated    
            else:
                print('update for existing record')
               # print(temp[0])
                record['pictures_downloaded'] = temp[0]['pictures_downloaded']
                record['photo_number_list'] = temp[0]['photo_number_list']
                try:
                    record['pic_retry_date'] = temp[0]['pic_retry_date']
                except Exception as e:
                    print(i)
                    print(e)                              
                col_history.insert_one(temp[0])
                col_last_state.insert_one(record)
                col_last_state.delete_one({'ml_num':temp[0]['ml_num'],'timestamp_sql':temp[0]['timestamp_sql'],
                                          'pix_updt':temp[0]['pix_updt']})
                update +=1
                log.write(str(record['ml_num']) + " is an update \n")
            
            
            
            
        ##adds new record to upload_last_state 
        else:
            print('new record')
            col_last_state.insert_one(record)
            log.write(str(record['ml_num']) + " is a new record \n")
            new +=1
            
    print('---------------------------DONE--------------------------')
    return new, update, duplicate, pix_update
    
##########################################################################################################################    
log.write('----------Upload---------\n')


pool = ThreadPool(processes=2)


condo_thread = pool.apply_async(upload, (rets_list_condo,condo_last_state,condo_history,idx_condo_mls))
res_thread = pool.apply_async(upload, (rets_list_res,res_last_state,res_history,idx_res_mls))

#upload_condo_last_state, upload_condo_history, delete_condo_last_state,\
condo_new, condo_update, condo_duplicate, condo_pix_update = condo_thread.get()

#upload_res_last_state, upload_res_history, delete_res_last_state,\
res_new, res_update, res_duplicate , res_pix_update= res_thread.get()    

##########################################################################################################################





log.write('----------Summary---------\n')
log.write(str(condo_new) +" new records for condo last state \n")
log.write(str(condo_update) +" updates for existing condo records \n")
log.write(str(condo_pix_update) +" pix updates for existing condo records \n")
log.write(str(condo_duplicate) +" duplicate condo records \n")

log.write("\n")

log.write(str(res_new) +" new records for res last state \n")
log.write(str(res_update) +" updates for existing residential records \n")
log.write(str(res_pix_update) +" pix updates for existing residential records \n")
log.write(str(res_duplicate) +" duplicate residential records \n")

        
log.write('time for data = '+str(datetime.now()-start_time)+'\n')    
print('done')  


##########################################################################################################################

log.write('----------Image upload---------\n')

now = datetime.now()
delta = timedelta(days = 1)    

res_mls_list = []
condo_mls_list = []
print('getting mls numbers for images')
for record in res_last_state.find({'pictures_downloaded': False, 'pix_updt': {'$gt': now-(delta*60)} }):#'$or': [{'pic_retry_date':{'$lt': now-delta}},{'pic_retry_date':None}]}):
    
    res_mls_list.append(record['ml_num'])
print('res mls numbers obtained')
for record in condo_last_state.find({'pictures_downloaded': False, 'pix_updt': {'$gt': now-(delta*60)} }):#'$or': [{'pic_retry_date':{'$lt': now-delta}},{'pic_retry_date':None}]}):
    
    condo_mls_list.append(record['ml_num'])   
print('condo mls numbers obtained')
    
log.write(str(len(res_mls_list))+' records for residential \n')
log.write(str(len(condo_mls_list))+' records for condo \n')


for i in range(1,threads+1):
    exec("res_mls_list"+str(i)+" = np.array_split(res_mls_list,threads)[i-1]")
for i in range(1,threads+1):
    exec("condo_mls_list"+str(i)+" = np.array_split(condo_mls_list,threads)[i-1]")    

##########################################################################################################################

def upload_images(collection,mls_list):
    boto3_session = boto3.session.Session()
    print(1)
    s3 = boto3_session.resource('s3',aws_access_key_id=access_key, aws_secret_access_key= secret_key)
    print(2)
    ## Upload images to S3
    for i in mls_list:
        upload_successful = False
        iterator = 0
        try:
            with session.Session(login_url, username,password,version) as s:
                print(3)
                unique_listing_id= str(i)
                object_dict_list= s.get_object(resource = 'Property', object_type = 'Photo', content_ids = unique_listing_id)
                    
                
                photo_list = ''
                for ob in object_dict_list:
                    iterator +=1
                    file_name= r"Photo{}-{}.jpeg".format(unique_listing_id, iterator)
                    s3.Bucket(str(bucket)).put_object(Key=file_name,Body= ob['content'],ContentType = "image/jpeg")
                    photo_list += str(iterator)+' '
                    upload_successful = True
                    
                    
            if (upload_successful == True):
                print(i, ' images found')
                log.write(i+'- '+str(iterator)+' images found \n')
                myquery = { "ml_num": i }
                newvalues = { "$set": { "pictures_downloaded": True ,
                                       'photo_number_list': photo_list.rstrip(),
                                       'oteq_revise_date': now
                                      } }
                collection.update_one(myquery,newvalues)
    
            else:
                print(i, ' images not found')
                log.write(i+' -images not found \n')
                myquery = { "ml_num": i }
                newvalues = { "$set": {'pic_retry_date':now } }
                collection.update_one(myquery,newvalues)
                            
            
        except Exception as e:
            print(i,' - ',e)
            log.write(i+' - error:'+ str(e)+ '\n')
            myquery = { "ml_num": i }
            newvalues = { "$set": {'pic_retry_date':now } }
            collection.update_one(myquery,newvalues)
    

for i in range(1,threads+1):
    exec("t"+str(i)+" = threading.Thread(target=upload_images, args = (res_last_state,res_mls_list"+str(i)+",))")
print('res image gather starting')
for i in range(1,threads+1):
    exec("t"+str(i)+".start()")

for i in range(1,threads+1):
    exec("t"+str(i)+".join()")
    
print('residential images done')  
print('condo image gather starting')
      
for i in range(1,threads+1):
    exec("t"+str(i)+" = threading.Thread(target=upload_images, args = (condo_last_state,condo_mls_list"+str(i)+",))") 
for i in range(1,threads+1):
    exec("t"+str(i)+".start()")    
for i in range(1,threads+1):
    exec("t"+str(i)+".join()")
    
print('done getting images')    


log.write("cycle took "+str(datetime.now()-start_time)+" to complete")

log.close()
connection.close()
rets_client.logout()
idx_rets_client.logout()
exec(open('/home/ubuntu/github/python_scripts/sql_sync.py').read())
#print('cycle completed, next cycle in ',repeat_time,' minutes')
print("program is now closed")
sys.stdout.close()
sys.exit(0)
