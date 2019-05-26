# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 10:02:52 2019

@author: Elkoumy
"""

import pandas as pd
import numpy as np
import os
import sys
#dir=r"C:\Gamal Elkoumy\PhD\OneDrive - Tartu Ülikool\Stream Processing\SWAG & Scotty\initial_results"
#algorithms=['CKMS','DoubleHeap','Frugal2u','RedBlackTree','SkipList','VEB']
print(sys.argv[1])
query_time_result=[]
insertion_time_result=[]
dir=r"C:\Gamal Elkoumy\PhD\OneDrive - Tartu Ülikool\Stream Processing\SWAG & Scotty\initial_results"
dir=sys.argv[1]
out_dir=sys.argv[2]
files=[f for f in os.listdir(dir) if  os.path.isfile(os.path.join(dir,f))]
throughput_acc=pd.DataFrame()
for file in files:
    parameters=file.split("_")
    query_time_res=[parameters[0],parameters[1],parameters[2],parameters[3],parameters[5]]
    insertion_time_res=[parameters[0],parameters[1],parameters[2],parameters[3],parameters[5]]
    file_dir = os.path.join(dir,file)

    if file.find("insertionTime")>-1:
        insertion_time=pd.read_csv(file_dir,sep=",", engine="python")
        insertion_time["duration"]=insertion_time.insertion_end -insertion_time.insertion_start
        insertion_time_res.append(insertion_time.duration.mean())
        insertion_time_res.append(insertion_time.duration.median())
        insertion_time_result.append(insertion_time_res)
    elif file.find("queryTime")>-1:
        query_time=pd.read_csv(file_dir,sep=",", engine="python")
        query_time["duration"]=query_time.query_end -query_time.query_start
        query_time_res.append(query_time.duration.mean())
        query_time_res.append(query_time.duration.median())
        query_time_result.append(query_time_res)
    elif file.find("throughput")>-1:
        #        approach,algorithm,tps,data_distribution,experiment_name
        throughput=pd.read_csv(file_dir,sep=",", engine="python")
        throughput=throughput.sort_values(['out_time'])
        throughput['approach']=parameters[0]
        throughput['algorithm']=parameters[1]
        throughput['tps']=parameters[2]
        throughput['data_distribution']=parameters[3]
        throughput['experiment_name']=parameters[5]
        throughput['cdf']=throughput.window_count.cumsum()/throughput.window_count.sum()
        throughput_acc=throughput_acc.append(throughput)


query_time_result=pd.DataFrame(query_time_result,columns=["approach","algorithm","tps","data_distribution","experiment_name","query_time_mean","query_time_median"])
insertion_time_result=pd.DataFrame(insertion_time_result,columns=["approach","algorithm","tps","data_distribution","experiment_name","insertion_time_mean","insertion_time_median"])

if(query_time_result.count>0):
    query_time_result.to_csv(os.path.join(out_dir,"query_time_result.csv"),index_col = False)
if(insertion_time_result.count>0):
    insertion_time_result.to_csv(os.path.join(out_dir,"insertion_time_result.csv"), index_col = False)

if(throughput_acc.count>0):
    throughput_acc.to_csv(os.path.join(out_dir,"insertion_time_result.csv"), index_col = False)



