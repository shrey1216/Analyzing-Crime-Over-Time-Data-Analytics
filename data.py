

import pandas as pd
import numpy as np


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

#drop NAs from crime in NY
df_NY = df_NY.dropna(subset=['CRIME_DESCRIPTION'])

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
df_CHI = df_CHI[['YEAR', 'MONTH', 'Primary Type','CRIME_DESCRIPTION']].copy()


#Change the rows in crime description based on what they are in primary type
df_CHI.loc[df_CHI['Primary Type'] == "BURGLARY", 'CRIME_DESCRIPTION'] = "BURGLARY"
df_CHI.loc[df_CHI['Primary Type'] == "ROBBERY", 'CRIME_DESCRIPTION'] = "ROBBERY"
df_CHI.loc[df_CHI['Primary Type'] == "SEX OFFENSE", 'CRIME_DESCRIPTION'] = "SEXUAL ASSAULT"
df_CHI.loc[df_CHI['Primary Type'] == "CRIM SEXUAL ASSAULT", 'CRIME_DESCRIPTION'] = "SEXUAL ASSAULT"
df_CHI.loc[df_CHI['Primary Type'] == "NARCOTICS", 'CRIME_DESCRIPTION'] = "NARCOTICS"

#TRYING TO SEPARATE OUT 38 DRUGS FROM LA
df_LA.loc[df_LA['CRIME_DESCRIPTION'].str.contains('DRUG'), 'CRIME_DESCRIPTION'] = 'NARCOTICS'
#TRYING TO SEPARATE OUT ARSON FROM CHIRAQ
df_CHI.loc[df_CHI['CRIME_DESCRIPTION'].str.contains('ARSON'), 'CRIME_DESCRIPTION'] = 'ARSON'
#Chicago add hands fist battery to battery
df_CHI.loc[df_CHI['CRIME_DESCRIPTION'].str.contains('BATTERY'), 'CRIME_DESCRIPTION'] = 'BATTERY'
#Separate out HOMICIDE from NY
df_NY.loc[df_NY['CRIME_DESCRIPTION'].str.contains('HOMICIDE'), 'CRIME_DESCRIPTION'] = 'HOMICIDE'


#changed FELONY ASSAULT to BATTERY in NY
df_NY['CRIME_DESCRIPTION'] = df_NY['CRIME_DESCRIPTION'].str.replace("FELONY ASSAULT",'BATTERY')
#changed BURGLAR'S TOOLS to BURGLARY in NY
df_NY['CRIME_DESCRIPTION'] = df_NY['CRIME_DESCRIPTION'].str.replace("BURGLAR'S TOOLS",'BURGLARY')
#changed PETIT LARCENY OF MOTOR VEHICLE to PETIT LARCENY IN NY
df_NY['CRIME_DESCRIPTION'] = df_NY['CRIME_DESCRIPTION'].str.replace("PETIT LARCENY OF MOTOR VEHICLE",'PETIT LARCENY')
#Changed FELONY SEX CRIMES TO SEXUAL ASSAULT
df_NY['CRIME_DESCRIPTION'] = df_NY['CRIME_DESCRIPTION'].str.replace("FELONY SEX CRIMES",'SEXUAL ASSAULT')
#Changed SEX CRIMES TO SEXUAL ASSAULT
df_NY['CRIME_DESCRIPTION'] = df_NY['CRIME_DESCRIPTION'].str.replace("SEX CRIMES",'SEXUAL ASSAULT')
#Changed DANGEROUS DRUGS TO NARCOTICS
df_NY['CRIME_DESCRIPTION'] = df_NY['CRIME_DESCRIPTION'].str.replace("DANGEROUS DRUGS",'NARCOTICS')
#Changed MURDER & NON-NEGL. MANSLAUGHTER TO HOMICIDE
df_NY['CRIME_DESCRIPTION'] = df_NY['CRIME_DESCRIPTION'].str.replace("MURDER & NON-NEGL. MANSLAUGHTER",'HOMICIDE', regex = False)
#Changed RAPE TO SEXUAL ASSAULT
df_NY['CRIME_DESCRIPTION'] = df_NY['CRIME_DESCRIPTION'].str.replace("RAPE",'SEXUAL ASSAULT')



#changed BATTERY - SIMPLE ASSAULT to BATTERY in LA
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("BATTERY - SIMPLE ASSAULT",'BATTERY')
#Changed BATTERY POLICE (SIMPLE) to BATTERY in LA
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("BATTERY POLICE (SIMPLE)","BATTERY",regex = False)
#Changed BATTERY WITH SEXUAL CONTACT to BATTERY in LA
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("BATTERY WITH SEXUAL CONTACT","BATTERY",regex = False)
#changed BURGLARY FROM VEHICLE to BURGLARY in LA
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("BURGLARY FROM VEHICLE",'BURGLARY')
#changed THEFT PLAIN - PETTY ($950 & UNDER) to PETIT LARCENY IN LA
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("THEFT PLAIN - PETTY ($950 & UNDER)","PETIT LARCENY",regex = False)
#changed SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH to SEXUAL ASSAULT
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH","SEXUAL ASSAULT",regex = False)
#changed SEX OFFENDER REGISTRANT OUT OF COMPLIANCE to SEXUAL ASSAULT
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("SEX OFFENDER REGISTRANT OUT OF COMPLIANCE","SEXUAL ASSAULT",regex = False)
#changed SEXUAL PENETRATION W/FOREIGN OBJECT to SEXUAL ASSAULT
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("SEXUAL PENETRATION W/FOREIGN OBJECT","SEXUAL ASSAULT",regex = False)
#changed SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ to SEXUAL ASSAULT
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ","SEXUAL ASSAULT",regex = False)
#changed ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT to DANGEROUS WEAPONS
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT","DANGEROUS WEAPONS",regex = False)
#changed BRANDISH WEAPON to DANGEROUS WEAPONS
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("BRANDISH WEAPON","DANGEROUS WEAPONS",regex = False)
#changed RAPE, FORCIBLE to SEXUAL ASSAULT
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("RAPE, FORCIBLE","SEXUAL ASSAULT",regex = False)
#changed INDECENT EXPOSURE to SEXUAL ASSAULT
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("INDECENT EXPOSURE","SEXUAL ASSAULT",regex = False)
#changed CRIMINAL HOMICIDE to HOMICIDE
df_LA['CRIME_DESCRIPTION'] = df_LA['CRIME_DESCRIPTION'].str.replace("CRIMINAL HOMICIDE","HOMICIDE",regex = False)

        

#changed $500 AND UNDER to PETIT LARCENY IN CHICAGO
df_CHI['CRIME_DESCRIPTION'] = df_CHI['CRIME_DESCRIPTION'].str.replace("$500 AND UNDER","PETIT LARCENY",regex = False)
#changed OVER $500 to PETIT LARCENY IN CHICAGO
df_CHI['CRIME_DESCRIPTION'] = df_CHI['CRIME_DESCRIPTION'].str.replace("OVER $500","PETIT LARCENY",regex = False)
#Changed DOMESTIC BATTERY SIMPLE to BATTERY in CHICAGO
df_CHI['CRIME_DESCRIPTION'] = df_CHI['CRIME_DESCRIPTION'].str.replace("DOMESTIC BATTERY SIMPLE","BATTERY",regex = False)
#Changed AGGRAVATED DOMESTIC BATTERY: OTHER DANG WEAPON to BATTERY in CHICAGO
df_CHI['CRIME_DESCRIPTION'] = df_CHI['CRIME_DESCRIPTION'].str.replace("AGGRAVATED DOMESTIC BATTERY: OTHER DANG WEAPON","BATTERY",regex = False)
#Changed AGGRAVATED DOMESTIC BATTERY: KNIFE/CUTTING INST to BATTERY in CHICAGO
df_CHI['CRIME_DESCRIPTION'] = df_CHI['CRIME_DESCRIPTION'].str.replace("AGGRAVATED DOMESTIC BATTERY: KNIFE/CUTTING INST","BATTERY",regex = False)
#Change AGGRAVATED: OTHER DANG WEAPON to DANGEROUS WEAPONS in Chicago
df_CHI['CRIME_DESCRIPTION'] = df_CHI['CRIME_DESCRIPTION'].str.replace("AGGRAVATED: OTHER DANG WEAPON","DANGEROUS WEAPONS",regex = False)
#Change UNLAWFUL POSS OF HANDGUN to DANGEROUS WEAPONS in Chicago
df_CHI['CRIME_DESCRIPTION'] = df_CHI['CRIME_DESCRIPTION'].str.replace("UNLAWFUL POSS OF HANDGUN","DANGEROUS WEAPONS",regex = False)
#Change FIRST DEGREE MURDER to HOMICIDE in Chicago
df_CHI['CRIME_DESCRIPTION'] = df_CHI['CRIME_DESCRIPTION'].str.replace("FIRST DEGREE MURDER","HOMICIDE",regex = False)







#WE NEED TO PICK A SUBSET OF CRIMES THAT HAVE THE MOST OCCURENCES (PICK A NUMBER TO FOCUS ON (maybe 10))
#WRITE CODE TO PUT A NUMBER ON EACH CRIME IN THE LIST
#DO THIS FOR ALL THREE CITIES AND PICK THE TOP CRIMES
#ALSO STANDARDIZE THEIR NAMES (FROM THE TOP CRIMES COMBINE ANY THAT ARE CLOSELY RELATED) 

#Counts of NY crimes
#print(df_NY['CRIME_DESCRIPTION'].value_counts().head(60))
#print(len(df_NY[df_NY['CRIME_DESCRIPTION'].str.contains('ARSON')]))

#Counts of LA crimes
#print(df_LA['CRIME_DESCRIPTION'].value_counts().head(60))
#print(len(df_LA[df_LA['CRIME_DESCRIPTION'].str.contains('ARSON')]))

#Counts of CHICAGO crimes
#print(df_CHI['CRIME_DESCRIPTION'].value_counts().head(60))
#print(df_CHI['Primary Type'].value_counts().head(60))
#print(len(df_CHI[df_CHI['CRIME_DESCRIPTION'].str.contains('ARSON')]))


#EXAMPLE (WE WOULD RENAME THE CRIMES SO THEY'RE STANDARDIZED FIRST!)
#crimes = ['ASSAULT', 'RAPE']
#PETIT LARCENY IS THE TOP NYC CRIME
#crimes = ['PETIT LARCENY', 'HARRASSMENT']

# selecting rows based on condition
#df_NY = df_NY.loc[df_NY['CRIME_DESCRIPTION'].isin(crimes)]




#LIST OF CRIMES WE WANT TO WORK WITH
#BATTERY FOR SURE (most in LA second most in chicago and NY)
#burglary and robbery
#petit larceny is 1000 in NY, categories for under 500 and over 500 should be combined THEFT PLAIN IN LA 950 UNDER
 


#EIGHT
#PETIT LARCENY DONE DON'T TOUCH
#BATTERY DONE DON'T TOUCH
#BURGLARY DONE DON'T TOUCH
#ROBBERY DONE DON'T TOUCH
#SEXUAL ASSAULT DONE DON'T TOUCH
#NARCOTICS DONE DON'T TOUCH
#ARSON DONE DON'T TOUCH
#DANGEROUS WEAPONS DONE DON'T TOUCH
#HOMICIDE DONE DON'T TOUCH


#EXAMPLE (WE WOULD RENAME THE CRIMES SO THEY'RE STANDARDIZED FIRST!)
crimes = ['PETIT LARCENY', 'BATTERY', 'BURGLARY', 'ROBBERY', 'SEXUAL ASSAULT', 'NARCOTICS', 'ARSON', 'DANGEROUS WEAPONS', 'HOMICIDE']

# selecting rows based on condition
df_NY = df_NY.loc[df_NY['CRIME_DESCRIPTION'].isin(crimes)]
df_LA = df_LA.loc[df_LA['CRIME_DESCRIPTION'].isin(crimes)]
df_CHI = df_CHI.loc[df_CHI['CRIME_DESCRIPTION'].isin(crimes)]


df_NY.to_csv('NY DATAFRAME FINAL.csv', encoding='utf-8', index=False)
df_LA.to_csv('LA DATAFRAME FINAL.csv', encoding='utf-8', index=False)
df_CHI.to_csv('CHICAGO DATAFRAME FINAL.csv', encoding='utf-8', index=False)
