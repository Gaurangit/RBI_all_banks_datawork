import pandas as pd
import numpy as np
import os
import reverse_geocoder as rg
import matplotlib.pyplot as plt 
import seaborn as sns
import pgeocode
import statistics
import random
import math 
from tqdm.notebook import tqdm
import geopy
import pickle
import re

def dict_update(k, val, D):
    try:
        t=D[k]
        if val not in t:
            
            t.append(val)
            D.update({k:t})
            
    except:
        D.update({k:[val]})

#### Takes a particular year and return the latlong-wise "Rain shock"
rainyear_D={}
def lat_long_to_city(lst):
    results = rg.search(lst, mode=1) # default mode = 2
    #dict(results[0])['admin1'], dict(results[0])
    #return results
    return  dict(results[0])['cc']
def year_rain(year):
    Drn1=Dr[[0,1]+list(range(year-21, year))]
    p=[]
    sh=[]
    for row in Drn1.values:
        std=statistics.stdev(row[2:-1])
        mn=statistics.mean(row[2:-1])

        if (std+mn)>row[-1]>(std-mn):
            p.append(1)
        else:
            p.append(0)

        tr=row[2:-1]
        tr.sort()

        seq=list(tr)
        if row[-1]>=seq[-4]:
            sh.append(1)

        if seq[-4]>row[-1]>seq[3]:
            sh.append(0)

        if row[-1]<=seq[3]:
            sh.append(-1)
    Drn1[["yearly_Rain_shock","yearly_Rain_pos_neg_shock"]]=np.array((p,sh)).T
    
    return Drn1



def month_rain(year, mo):
    lll=[]
    for i in list(range(year-21, year)):
        lll.append(str(i)+'_'+mo)
    Drn2=Dmo[[0,1]+lll]
    p=[]
    sh=[]
    for row in Drn2.values:
        std=statistics.stdev(row[2:-1])
        mn=statistics.mean(row[2:-1])

        if (std+mn)>row[-1]>(std-mn):
            p.append(1)
        else:
            p.append(0)

        tr=row[2:-1]
        tr.sort()

        seq=list(tr)
        if row[-1]>=seq[-4]:
            sh.append(1)

        if seq[-4]>row[-1]>seq[3]:
            sh.append(0)

        if row[-1]<=seq[3]:
            sh.append(-1)
    Drn2[["quat_Rain_shock","quat_Rain_pos_neg_shock"]]=np.array((p,sh)).T
    
    return Drn2


for year in [1997]:
    lines = np.loadtxt("DATA/Rain_DATA/precip."+str(year))
    dfrain=pd.DataFrame(lines)


    ddt=[]
    #pbar=tqdm(total=len(dfrain))


    for row in dfrain.values:
        #pbar.update(1)
        if (6<row[1]<37) and (68<row[0]<97.5):
            if lat_long_to_city([row[1], row[0]])=='IN':
                ddt.append(row)
    Dr=pd.DataFrame(ddt)[[0,1]]

for year in list(range(1993,2018)):
    lines = np.loadtxt("DATA/Rain_DATA/precip."+str(year))
    dfrain=pd.DataFrame(lines)


    ddt=[]
    #pbar=tqdm(total=len(dfrain))


    for row in dfrain.values:
        #pbar.update(1)
        if (6<row[1]<37) and (68<row[0]<97.5):
            if lat_long_to_city([row[1], row[0]])=='IN':
                ddt.append(row[-1])
    Dr[year]=ddt

Dmo=Dr[[0,1]]
for year in list(range(1993,2018)):
    lines = np.loadtxt("DATA/Rain_DATA/precip."+str(year))
    dfrain=pd.DataFrame(lines)


    ddt=[]
    #pbar=tqdm(total=len(dfrain))


    for row in dfrain.values:
        #pbar.update(1)
        if (6<row[1]<37) and (68<row[0]<97.5):
            if lat_long_to_city([row[1], row[0]])=='IN':
                ddt.append([row[2]+row[3]+row[4]+row[5],row[6]+row[7]+row[8]+row[9],row[10]+row[11]+row[12]+row[13]])
    Dmo[[str(year)+'_04',str(year)+'_08',str(year)+'_12']]=np.array(ddt)

#### ----------------------------------------------


for file in os.listdir("DATA/CMIE data/"):  ### Looping over all files to merge the data
    df=pd.read_csv("DATA/CMIE data/"+file)

    
    year=int(file[19:23])
    if year==2017:
        mm=file[32:34]
        if mm in ["12"]:
            year=int(file[19:23])+1
        else:
            year=int(file[19:23])

        dd_d={"12":'08',"08":"04","04":'12'}
        rain_llt_D={}
        rain_data_year=year_rain(year)
        for i in rain_data_year.values:
            rain_llt_D.update({str(i[0])+"_:_"+str(i[1]):i[-2:]})


        rain_mllt_D={}
        if mm=="08":
            mon=month_rain(year+1, dd_d[mm])
        else:
            mon=month_rain(year+1, dd_d[mm])
        for i in mon.values:
            rain_mllt_D.update({str(i[0])+"_:_"+str(i[1]):i[-2:]})





        a=0
        dta=[]
        bad=[]
        for row in df.values:

            try:
                ppp=city_ltln_D[row[3]+" "+row[1]]
                rr=list(row)+closest_rain(ppp, rain_llt_D)+closest_rain(ppp, rain_mllt_D)

                dta.append(rr)


            except:

                bad.append(row)
        print(len(bad),len(dta))

        dr=pd.DataFrame(dta, columns=list(df.columns)+list(rain_data_year.columns)[-2:]+list(mon.columns)[-2:])


        dr.to_csv("done"+file)
