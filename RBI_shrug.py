import pandas as pd
import numpy as np
import os
import math 
import pickle
import random
import statistics 
import re
import time
import reverse_geocoder as rg





with open('add_ltln_dict.obj', 'rb') as fp:
    up_pin_D = pickle.load(fp)
    
with open('shrid_ltln_D.obj', 'rb') as fp:
    shrid_ltln_D = pickle.load(fp)
    
    
with open('bank_ltln_D.obj', 'rb') as fp:
    bank_ltln_D = pickle.load(fp)
    
with open('pin_latln_DICT.obj', 'rb') as fp:
    pin_latln_DICT = pickle.load(fp)
    
with open('partcd_pin_D.obj', 'rb') as fp:
    partcd_pin_D = pickle.load(fp)
    
for i in ['0330402',
 '0330403',
 '0330534',
 '0330538',
 '0330867',
 '0331583',
 '0331612',
 '0331617',
 '0331702']:
    bank_ltln_D.update({i:[30.3343, 78.03999565217389]})


def dict_update(k, val, D):
    try:
        t=D[k]
        if val not in t:
            
            t.append(val)
            D.update({k:t})
            
    except:
        D.update({k:[val]})


##### This part of the code takes 'staff_final.sas7bdat' 
##### and creats a dictionary of yearwise bank-data


dfb = pd.read_sas('staff_final.sas7bdat')

prt=[]
for i in dfb['PART1CD']:
    prt.append(str(i)[2:-1])
dfb['PART1CD_no']=prt


yer=[]

for i in dfb['year']:
    yer.append(int(i))
dfb['year']=yer
bank_yearwise_D={}

for year, dfbn in dfb.groupby('year'):
    bank_yearwise_D.update({year:dfbn})

##### ---------------------------------------------------

##### Takes a particular year and return the latlong-wise "Rain shock"
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
    Drn1[["Rain_shock","Rain_pos_neg_shock"]]=np.array((p,sh)).T
    
    return Drn1
#####-----------------------------------------------------

##### Picks the relevent rain data using lat-long present in india and creat yearwise 

lines = np.loadtxt("DATA/Rain_DATA/precip.2013")
dfrain=pd.DataFrame(lines)
ddt=[]



for row in dfrain.values:
    pbar.update(1)
    if (6<row[1]<37) and (68<row[0]<97.5):
        if lat_long_to_city([row[1], row[0]])=='IN':
            ddt.append(row)

Dr=pd.DataFrame(ddt)[[0,1]]


rainyear_D={}
def lat_long_to_city(lst):
    results = rg.search(lst, mode=1) # default mode = 2
    #dict(results[0])['admin1'], dict(results[0])
    #return results
    return  dict(results[0])['cc']


for year in list(range(1979,2014)):
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



for year in range(2000,2014):     ## Looping over the Years
    

    ### Bank data processing for a particular year

    tem_bnk_D={}

    for row in bank_yearwise_D[year].values:
        tem_bnk_D.update({row[-1]:row[1:-1]})

    dfn=pd.read_sas("DATA/Bank_Data/b2"+str(year)[2:]+"pt1.sas7bdat",encoding= 'unicode_escape')
    DATA=[]
    a=0
    for row in dfn.values:
        try:
            DATA.append(list(row)+list(tem_bnk_D[row[0]]))
        except:
            a=a+1

    

    df=pd.DataFrame(DATA, columns=list(dfn.columns)+list(dfb.columns)[1:-1])
    a=[]
    dn_dist_prt_D={}
    dn_prt_pin_D={}
    for i in DATA:

        if str(i[0]) in bank_ltln_D.keys():
            dict_update(i[5]+i[7], bank_ltln_D[str(i[0])], dn_dist_prt_D)
            dict_update(i[5]+i[7], partcd_pin_D[str(i[0])], dn_prt_pin_D)

        else:
            a.append(i[5]+i[7])

    c=0
    dft=[]
    for i in set(a):
        if i not in dn_dist_prt_D.keys():
            #print(i)
            dft.append(i)
    
    #### ------------------------------------------


    #### Rain data processing for a particular year
    #### lat-long to rain data 
    rain_llt_D={}
    
    rain_year_data=year_rain(year)
    for i in rain_year_data.values:
        rain_llt_D.update({str(i[0])+"_:_"+str(i[1]):i[2:]})
        
    #### ------------------------------------------

        

    #### Bank lat-long to rain data merging
    a=0
    dta=[]
    bad=[]
    for row in DATA:

        try:
            ppp=bank_ltln_D[str(row[0])]
            rr=list(row)+closest_rain(ppp)

            dta.append(rr)


        except:
            try:
                ppp=dn_dist_prt_D[row[5]+row[7]]


                if len(ppp)>0:

                    dta.append(list(row)+closest_rain(ppp[random.randint(0,len(ppp)-1)]))
                else:
                    bad.append(row)
                    a=a+1
            except:
                bad.append(row)
    n=0
    t=0
    for r in bad:
        t=t+1
        if r[5]+r[7] not in dft:
            n=n+1
            
    dr=pd.DataFrame(dta, columns=list(df.columns)+list(rain_year_data.columns)[2:])
    
    ####----------------------------------
    

    #### Adding corrected PINCODES and merging the Night-light data
    a=0
    dta=[]
    bad=[]
    for row in DATA:

        try:
            ppp=partcd_pin_D[str(row[0])]


            dta.append(pp)


        except:
            try:
                ppp=dn_prt_pin_D[row[5]+row[7]]


                if len(ppp)>0:

                    dta.append(ppp[random.randint(0,len(ppp)-1)])
                else:
                    bad.append(row)
                    a=a+1
            except:
                bad.append(row)
    n=0
    t=0
    for r in bad:
        t=t+1
        if r[5]+r[7] not in dft:
            n=n+1
    dr['Pincode']=dta
    
    dta_dt={}
    a=0
    for pin, dt in SS[["total_light"+str(year), "total_light_cal"+str(year),"max_light"+str(year), "Pincodes"]].groupby("Pincodes"):
        dta_dt.update({pin:[dt["total_light"+str(year)].mean(),dt["total_light_cal"+str(year)].mean(),dt["max_light"+str(year)].mean(),
            dt["total_light"+str(year)].median(),dt["total_light_cal"+str(year)].median(),dt["max_light"+str(year)].median()]})
        a=a+1

        
    all_D={}
    a=0
    for i in tempD:
        l=tempD[i]
        a=a+len(l)
        for ii in l:
            all_D.update({ii:l[0]})

            
    in_=[]
    out=[]
    for i in set(dr['Pincode']):
        if i not in all_D.keys():
            l=pin_latln_DICT[i]
            l.append(i)
            out.append(l)
        else:

    #        l.append(i)
            in_.append(pin_latln_DICT[i])



    Y=np.array(in_).astype(float)

    all_DD={}
    for row in out:


        lt=float(row[0])
        ln=float(row[1])

        a=150
        mnlt=[]
        for ii in Y[:,0]:

            if abs(lt-ii)<a:
                mnlt=[]
                mnlt.append(ii)
                a=abs(lt-ii)


        lll=[]
        for i in Y:
            if mnlt[0] == i[0]:
                lll.append(i[1])


        b=150
        mnln=[]
        for ii in lll:
            if abs(ln-ii)<b:
                mnln=[]
                mnln.append(ii)
                b=abs(ln-ii)


    #     l=tempD[i]
    #     a=a+len(l)
    #     for ii in l:
    #         all_D.update({ii:l[0]})
        all_DD.update({row[2]:tempJJ["_:_".join([str(t) for t in [mnlt[0],mnln[0]]])]})

    e=[]
    n_D={}
    for i in all_DD:
        l=all_DD[i]
        p=[]
        for ii in l:
            if ii in list(all_D.keys()):
                p.append(ii)
        n_D.update({i:p[0]})
        
    DDD=[]
    a=0
    bd=[]
    gd=[]
    for pin in dr['Pincode']:
        try:
            try:
                DDD.append(dta_dt[all_D[pin]])
                gd.append(pin)
            except:
                DDD.append(dta_dt[n_D[pin]])
                bd.append(pin)
        except:
            a=a+1
            
            
    dr[["mean_total_lght","mean_total_lght_cal","mean_max_lght","medn_total_lght","medn_total_lght_cal","medn_max_lght"]]=np.array(DDD)

    del dr["Pincode"]

    ####--------------------------------


    dr.to_csv(str(year)+".csv") ### Saving merged file to the disk
    print(year)
