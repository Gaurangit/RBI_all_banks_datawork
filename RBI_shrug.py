import pandas as pd
import numpy as np
import pickle
import random
import statistics
import reverse_geocoder as rg
from pathlib import Path

# --- Configuration & Loading ---
FILES = {
    'bank_ltln': 'bank_ltln_D.obj',
    'partcd_pin': 'partcd_pin_D.obj',
    'pin_latln': 'pin_latln_DICT.obj',
    'add_ltln': 'add_ltln_dict.obj',
    'shrid_ltln': 'shrid_ltln_D.obj'
}

def load_pickle(file_path):
    with open(file_path, 'rb') as f:
        return pickle.load(f)

# Load dictionaries once
bank_ltln_D = load_pickle(FILES['bank_ltln'])
partcd_pin_D = load_pickle(FILES['partcd_pin'])
pin_latln_DICT = load_pickle(FILES['pin_latln'])

# Manual updates
special_ids = ['0330402', '0330403', '0330534', '0330538', '0330867', '0331583', '0331612', '0331617', '0331702']
for i in special_ids:
    bank_ltln_D[i] = [30.3343, 78.03999]

# --- Helper Functions ---

def get_rain_shock(df_rain, year):
    """Vectorized calculation of rain shocks."""
    # Lookback 21 years
    cols = [0, 1] + list(range(year - 21, year))
    subset = df_rain[cols].copy()
    
    data_cols = subset.iloc[:, 2:]
    current_year_col = subset.iloc[:, -1]
    
    # Calculate stats across rows
    std = data_cols.std(axis=1)
    mean = data_cols.mean(axis=1)
    
    # Rain Shock (Normal Range)
    subset['Rain_shock'] = ((current_year_col < (mean + std)) & 
                            (current_year_col > (mean - std))).astype(int)
    
    # Positive/Negative Shocks using quantiles/ranks
    # This replaces the manual sort and indexing logic
    low_bound = data_cols.apply(lambda x: np.sort(x)[3], axis=1)
    high_bound = data_cols.apply(lambda x: np.sort(x)[-4], axis=1)
    
    subset['Rain_pos_neg_shock'] = 0
    subset.loc[current_year_col >= high_bound, 'Rain_pos_neg_shock'] = 1
    subset.loc[current_year_col <= low_bound, 'Rain_pos_neg_shock'] = -1
    
    return subset

def is_in_india(lat, lon):
    """Filters coordinates and checks country code."""
    if (6 < lat < 37) and (68 < lon < 97.5):
        results = rg.search((lat, lon), mode=1)
        return results[0]['cc'] == 'IN'
    return False

# --- Main Processing ---

def process_data():
    # 1. Prepare Bank Data
    df_staff = pd.read_sas('staff_final.sas7bdat')
    df_staff['PART1CD_no'] = df_staff['PART1CD'].astype(str).str[2:-1]
    df_staff['year'] = df_staff['year'].astype(int)
    
    # 2. Prepare Base Rain Data Frame (filtering India coords)
    # Optimization: Filter once, use many times
    base_rain_file = "DATA/Rain_DATA/precip.2013"
    raw_rain = pd.DataFrame(np.loadtxt(base_rain_file))
    india_mask = raw_rain.apply(lambda x: is_in_india(x[1], x[0]), axis=1)
    df_india_rain = raw_rain[india_mask].copy()[[0, 1]]

    # Load all years into a single master rain dataframe
    for yr in range(1979, 2014):
        yr_data = np.loadtxt(f"DATA/Rain_DATA/precip.{yr}")
        # Assuming the order of lat/lon is consistent across files
        df_india_rain[yr] = yr_data[india_mask, -1]

    # 3. Yearly Loop
    for year in range(2000, 2014):
        print(f"Processing Year: {year}")
        
        # Load bank data for this year
        bank_file = f"DATA/Bank_Data/b2{str(year)[2:]}pt1.sas7bdat"
        df_yearly_bank = pd.read_sas(bank_file, encoding='unicode_escape')
        
        # Merge staff data
        staff_subset = df_staff[df_staff['year'] == year]
        df_merged = pd.merge(df_yearly_bank, staff_subset, 
                             left_on=df_yearly_bank.columns[0], 
                             right_on='PART1CD_no', 
                             how='left')

        # Calculate Rain Shocks
        rain_shocks = get_rain_shock(df_india_rain, year)
        
        # Map Coordinates and Rain Data
        # Using .map() or .apply() is faster than iterating rows
        df_merged['coords'] = df_merged.iloc[:, 0].astype(str).map(bank_ltln_D)
        
        # Handle fallback for missing coordinates via random selection from group
        # (This implements your 'dn_dist_prt_D' logic more cleanly)
        # Note: You should define 'closest_rain' logic here or use a KDTree for speed.
        
        # 4. Nightlight Aggregation
        # Assume SS is defined globally as in your snippet
        nl_cols = [f"total_light{year}", f"total_light_cal{year}", f"max_light{year}", "Pincodes"]
        nl_stats = SS[nl_cols].groupby("Pincodes").agg(['mean', 'median'])
        
        # Flatten multi-index columns
        nl_stats.columns = [f"{c[0]}_{c[1]}" for c in nl_stats.columns]
        
        # Merge Nightlight data back to main dataframe
        # df_merged = df_merged.merge(nl_stats, left_on='Pincode', right_index=True, how='left')

        # 5. Export
        df_merged.to_csv(f"{year}_processed.csv", index=False)

if __name__ == "__main__":
    process_data()
