

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


#This is NY put into the same format as the LA dataframe 
df_NY = df_NY[['CMPLNT_FR_DT', 'RPT_DT', 'OFNS_DESC', 'VIC_AGE_GROUP', 'VIC_SEX', 'VIC_RACE']].copy()


#LA's Dataframe section
chunk = pd.read_csv('LA Crime_Cleared.csv', chunksize=10000000)
df_LA = pd.concat(chunk)

df2 = df_LA[['DATE OCC', 'Date Rptd', 'Crm Cd Desc', 'Vict Age', 'Vict Sex', 'Vict Descent']].copy()
df2.rename(columns={'DATE OCC': 'DATE_OCC'}, inplace = True)
df2.rename(columns={'Date Rptd': 'Date_Rptd'}, inplace = True)
                   
df2['DATE_OCC'] = df2['DATE_OCC'].str.replace('-','')
df2['DATE_OCC'] = df2['DATE_OCC'].str.replace(':','')
df2['DATE_OCC'] = df2['DATE_OCC'].str.replace(' ','')
df2['DATE_OCC'] = df2.DATE_OCC.str.slice(0, -10, 1)
df2['DATE_OCC'] = df2['DATE_OCC'].astype(str).astype(int)

df2['Date_Rptd'] = df2['Date_Rptd'].str.replace('-','')
df2['Date_Rptd'] = df2['Date_Rptd'].str.replace(':','')
df2['Date_Rptd'] = df2['Date_Rptd'].str.replace(' ','')
df2['Date_Rptd'] = df2.Date_Rptd.str.slice(4, -8, 1)
df2['Date_Rptd'] = df2['Date_Rptd'].astype(str).astype(int)

df2 = df2[df2['DATE_OCC'] >= 2015]


#Chicago's DataFrame section
chunk = pd.read_csv('Crimes_-_2001_to_present.csv', chunksize=10000000)
df_CHI = pd.concat(chunk)

df3 = df_CHI[['Year', 'Date', 'Primary Type', 'Description', 'Updated On']].copy()
df3['Year'] = df3['Year'].astype(str).astype(int)

df3['Date'] = df3['Date'].str.replace('/','')
df3['Date'] = df3['Date'].str.replace(':','')
df3['Date'] = df3['Date'].str.replace(' ','')
df3['Date'] = df3.Date.str.slice(0, -14, 1)
df3['Date'] = df3['Date'].astype(str).astype(int)

df3 = df3[df3['Year'] >= 2015]


df_NY.to_csv('NY UPDATED.csv', encoding='utf-8', index=False)
df2.to_csv('LA UPDATED.csv', encoding='utf-8', index=False)
df3.to_csv('CHICAGO UPDATED.csv', encoding='utf-8', index=False)


#From this point, read csv from the three files saved and make three new dataframes

chunk = pd.read_csv('NY UPDATED.csv',chunksize = 10000000)
df_NY = pd.concat(chunk)

chunk = pd.read_csv('LA UPDATED.csv',chunksize = 10000000)
df_LA = pd.concat(chunk)

chunk = pd.read_csv('CHICAGO UPDATED.csv',chunksize = 10000000)
df_CHI = pd.concat(chunk)


#rename NY
df_NY.rename(columns={'CMPLNT_FR_DT': 'YEAR'}, inplace = True)
df_NY.rename(columns={'RPT_DT': 'MONTH'}, inplace = True)
df_NY.rename(columns={'OFNS_DESC': 'CRIME_DESCRIPTION'}, inplace = True)
#Get rid of harrassment 2
df_NY['CRIME_DESCRIPTION'] = df_NY['CRIME_DESCRIPTION'].str.replace('HARRASSMENT 2','HARRASSMENT')


#rename LA
df_LA.rename(columns={'DATE_OCC': 'YEAR'}, inplace = True)
df_LA.rename(columns={'Date_Rptd': 'MONTH'}, inplace = True)
df_LA.rename(columns={'Crm Cd Desc': 'CRIME_DESCRIPTION'}, inplace = True)

#rename CHICAGO
df_CHI.rename(columns={'Year': 'YEAR'}, inplace = True)
df_CHI.rename(columns={'Date': 'MONTH'}, inplace = True)
df_CHI.rename(columns={'Description': 'CRIME_DESCRIPTION'}, inplace = True)
#drop Updated On
df_CHI.drop('Updated On', axis=1, inplace=True)
#for CHICAGO, choose primary type or crime description as main crime descriptor 

#Trim each down OPTIONAL
df_NY = df_NY[['YEAR', 'MONTH', 'CRIME_DESCRIPTION']].copy()
df_LA = df_LA[['YEAR', 'MONTH', 'CRIME_DESCRIPTION']].copy()
df_CHI = df_CHI[['YEAR', 'MONTH', 'CRIME_DESCRIPTION']].copy()



#WE NEED TO PICK A SUBSET OF CRIMES THAT HAVE THE MOST OCCURENCES (PICK A NUMBER TO FOCUS ON (maybe 10))
#WRITE CODE TO PUT A NUMBER ON EACH CRIME IN THE LIST
#DO THIS FOR ALL THREE CITIES AND PICK THE TOP CRIMES
#ALSO STANDARDIZE THEIR NAMES (FROM THE TOP CRIMES COMBINE ANY THAT ARE CLOSELY RELATED) 

#Counts of NY crimes
#df_NY['CRIME_DESCRIPTION'].value_counts().head(50)

#Counts of LA crimes
#df_LA['CRIME_DESCRIPTION'].value_counts().head(50)

#Counts of CHICAGO crimes
#df_CHI['CRIME_DESCRIPTION'].value_counts().head(50)


#EXAMPLE (WE WOULD RENAME THE CRIMES SO THEY'RE STANDARDIZED FIRST!)
#crimes = ['ASSAULT', 'RAPE']
#PETIT LARCENY IS THE TOP NYC CRIME, HARRASSMENT IS THE SECOND
#crimes = ['PETIT LARCENY', 'HARRASSMENT']

# selecting rows based on condition
#df_NY = df_NY.loc[df_NY['CRIME_DESCRIPTION'].isin(crimes)]

#One slide where we go over general facts
#top crimes (top crime in each city)

#df_NY.head(51)
#df_LA.head(51)
#df_CHI.head(51)
