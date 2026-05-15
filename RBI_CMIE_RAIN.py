import pandas as pd
import numpy as np
import os
import reverse_geocoder as rg
from scipy.spatial import cKDTree
from pathlib import Path

# --- Constants & Setup ---
RAIN_PATH = Path("DATA/Rain_DATA")
CMIE_PATH = Path("DATA/CMIE data")
YEAR_RANGE = range(1993, 2018)

def get_india_mask(df_grid):
    """Returns a boolean mask for coordinates within India."""
    # Rough bounding box for speed before expensive geocoding
    mask = (df_grid[1] > 6) & (df_grid[1] < 37) & (df_grid[0] > 68) & (df_grid[0] < 97.5)
    
    def check_in(lat, lon):
        res = rg.search((lat, lon), mode=1)
        return res[0]['cc'] == 'IN'

    # Only geocode points inside the bounding box
    india_coords = df_grid[mask].apply(lambda x: check_in(x[1], x[0]), axis=1)
    return mask & india_coords

# --- Optimized Rain Processing ---

def calculate_shocks(df, value_cols, prefix):
    """Vectorized calculation of rain shocks for any timeframe."""
    data = df[value_cols]
    current = data.iloc[:, -1]
    
    # Stats across the lookback period (excluding current year)
    lookback = data.iloc[:, :-1]
    mn = lookback.mean(axis=1)
    std = lookback.std(axis=1)
    
    # Basic Shock
    df[f'{prefix}_shock'] = ((current < (mn + std)) & (current > (mn - std))).astype(int)
    
    # Pos/Neg Shock (based on ranks)
    # 1 if >= 4th highest, -1 if <= 4th lowest, else 0
    ranks = data.apply(lambda x: np.sort(x), axis=1)
    low_bound = ranks.apply(lambda x: x[3])
    high_bound = ranks.apply(lambda x: x[-4])
    
    df[f'{prefix}_pos_neg_shock'] = 0
    df.loc[current >= high_bound, f'{prefix}_pos_neg_shock'] = 1
    df.loc[current <= low_bound, f'{prefix}_pos_neg_shock'] = -1
    
    return df[[0, 1, f'{prefix}_shock', f'{prefix}_pos_neg_shock']]

# --- Main Execution ---

# 1. Initialize Rain DataFrames
# We load one file to establish the grid
grid_sample = pd.DataFrame(np.loadtxt(RAIN_PATH / "precip.1997"))
india_mask = get_india_mask(grid_sample)

dr_annual = grid_sample[india_mask][[0, 1]].copy()
dr_monthly = grid_sample[india_mask][[0, 1]].copy()

# 2. Single-pass data ingestion
for year in YEAR_RANGE:
    data = np.loadtxt(RAIN_PATH / f"precip.{year}")
    # Annual (sum of all months/cols 2-13)
    dr_annual[year] = data[india_mask, 2:14].sum(axis=1)
    
    # Specific month groups (example logic for your 04, 08, 12 blocks)
    dr_monthly[f"{year}_04"] = data[india_mask, 2:6].sum(axis=1)
    dr_monthly[f"{year}_08"] = data[india_mask, 6:10].sum(axis=1)
    dr_monthly[f"{year}_12"] = data[india_mask, 10:14].sum(axis=1)

# 3. Process CMIE Files
# Build a KDTree for the rain grid for lightning-fast spatial lookups
tree = cKDTree(dr_annual[[1, 0]].values) # Lat, Lon

for file in os.listdir(CMIE_PATH):
    df_cmie = pd.read_csv(CMIE_PATH / file)
    
    # Logic to determine target year/month from filename
    # (Keeping your original logic but cleaned up)
    raw_year = int(file[19:23])
    mm = file[32:34]
    target_year = raw_year + 1 if (raw_year == 2017 and mm == "12") else raw_year
    
    # Calculate shocks for this specific year/quarter
    annual_shocks = calculate_shocks(dr_annual, list(range(target_year-21, target_year+1)), "yearly_Rain")
    
    # Spatial Lookup: Find nearest rain grid index for each CMIE row
    # Assuming city_ltln_D contains [lat, lon]
    # This replaces the 'closest_rain' loop
    city_coords = [city_ltln_D.get(f"{row[3]} {row[1]}", [np.nan, np.nan]) for row in df_cmie.values]
    distances, indices = tree.query(city_coords)
    
    # Merge data using indices
    res_annual = annual_shocks.iloc[indices][['yearly_Rain_shock', 'yearly_Rain_pos_neg_shock']].values
    
    # Combine and save
    df_cmie[['yearly_Rain_shock', 'yearly_Rain_pos_neg_shock']] = res_annual
    df_cmie.to_csv(f"done_{file}", index=False)            sh.append(1)

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
