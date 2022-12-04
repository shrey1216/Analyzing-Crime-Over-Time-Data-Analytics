

import pandas as pd


chunk = pd.read_csv('NYPD_Complaint_Data_Historic.csv',chunksize=10000000)
df_NY = pd.concat(chunk)

df_NY = df_NY.dropna(subset=['CMPLNT_FR_DT'])
df_NY = df_NY.dropna(subset=['RPT_DT'])

df_NY['CMPLNT_FR_DT'] = df_NY['CMPLNT_FR_DT'].str.replace('/','')
df_NY['CMPLNT_FR_DT'] = df_NY['CMPLNT_FR_DT'].astype(str).astype(int)



df_NY['RPT_DT'] = df_NY['RPT_DT'].str.replace('/','')
#df_NY.RPT_DT = df_NY.RPT_DT.[0,3]
df_NY['RPT_DT'] = df_NY.RPT_DT.str.slice(0, -6, 1)
df_NY['RPT_DT'] = df_NY['RPT_DT'].astype(str).astype(int)


df_NY.RPT_DT = df_NY.RPT_DT % 10000
df_NY.CMPLNT_FR_DT = df_NY.CMPLNT_FR_DT % 10000

#CMPLNT_NUM
df_NY.drop('CMPLNT_NUM', axis=1, inplace=True)
#CMPLNT_TO_DT
df_NY.drop('CMPLNT_TO_DT', axis=1, inplace=True)
#CMPLNT_TO_TM
df_NY.drop('CMPLNT_TO_TM', axis=1, inplace=True)
#ADDR_PCT_CD
df_NY.drop('ADDR_PCT_CD', axis=1, inplace=True)


#RPT_DT (check other cities)
#df_NY.drop('Open', axis=1, inplace=True)


#PD_CD
df_NY.drop('PD_CD', axis=1, inplace=True)
#PD_DESC
df_NY.drop('PD_DESC', axis=1, inplace=True)
#BORO_NM
df_NY.drop('BORO_NM', axis=1, inplace=True)
#LOC_OF_OCCUR_DESC
df_NY.drop('LOC_OF_OCCUR_DESC', axis=1, inplace=True)
#PREM_TYP_DESC
df_NY.drop('PREM_TYP_DESC', axis=1, inplace=True)
#JURIS_DESC
df_NY.drop('JURIS_DESC', axis=1, inplace=True)
#JURISDICTION_CODE
df_NY.drop('JURISDICTION_CODE', axis=1, inplace=True)
#PARKS_NM
df_NY.drop('PARKS_NM', axis=1, inplace=True)
#HADEVELOPT
df_NY.drop('HADEVELOPT', axis=1, inplace=True)
#HOUSING_PSA
df_NY.drop('HOUSING_PSA', axis=1, inplace=True)
#X_COORD_CD
df_NY.drop('X_COORD_CD', axis=1, inplace=True)
#Y_COORD_CD
df_NY.drop('Y_COORD_CD', axis=1, inplace=True)
#TRANSIT_DISTRICT
df_NY.drop('TRANSIT_DISTRICT', axis=1, inplace=True)
#Latitude
df_NY.drop('Latitude', axis=1, inplace=True)
#Longitude
df_NY.drop('Longitude', axis=1, inplace=True)
#Lat_Lon
df_NY.drop('Lat_Lon', axis=1, inplace=True)
#PATROL_BORO
df_NY.drop('PATROL_BORO', axis=1, inplace=True)
#STATION_NAME
df_NY.drop('STATION_NAME', axis=1, inplace=True)
#get rid of unnamed
df_NY.drop('Unnamed: 0', axis=1, inplace=True)
#get rid of KY_CD
df_NY.drop('KY_CD', axis=1, inplace=True)
df_NY.drop('CMPLNT_FR_TM', axis=1, inplace=True)



#grab ones after 2015 or delete ones before


df_NY = df_NY[df_NY['CMPLNT_FR_DT'] >= 2015]

chunk = pd.read_csv('LA Crime_Cleared.csv',chunksize=10000000)
#LA's Dataframe section
chunk = pd.read_csv('LA Crime_Cleared.csv', chunksize=10000000)
df_LA = pd.concat(chunk)
print(df_LA.head(5))

df2 = df_LA[['Date Rptd', 'DATE OCC', 'Crm Cd Desc', 'Vict Age', 'Vict Sex', 'Vict Descent']].copy()
print(df2.head(5))

chunk = pd.read_csv('Crimes_-_2001_to_present.csv',chunksize=10000000)
#Chicago's DataFrame section
chunk = pd.read_csv('Crimes_-_2001_to_present.csv', chunksize=10000000)
df_Chi = pd.concat(chunk)
print(df_Chi.head(5))

df3 = df_Chi[['Year', 'Date', 'Primary Type', 'Description', 'Updated On']].copy()
print(df3.head(5))
