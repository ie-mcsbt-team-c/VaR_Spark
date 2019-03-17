# -*- coding: utf-8 -*-

#%%
import urllib.request as url
import pandas as pd 


#Here, we declare as variables our start and end dates and our api keys for both services

start_date     = '2010-02-21'
end_date       = '2019-03-11'
api_key_alpha  = 'SQ60DKQWSFAU53XH'
api_key_quandl = 'ujUGW2cbgsDmD7sP359j'

#We declare our API endpoints

urlsaGSP        = ['https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=^GSPC&outputsize=full&apikey=']

urlsaNDAQ       = ['https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NDAQ&outputsize=full&apikey=']

urlsqOPEC       = ['https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=' + start_date + '&end_date=' + end_date + '&api_key=']

urlsqUSTREASURY = ['https://www.quandl.com/api/v3/datasets/USTREASURY/YIELD.csv?start_date=' + start_date + '&end_date=' + end_date + '&api_key=']



#We do separate loops to ibtain the csv file because of the
# different API structures

#We will use provisional variable names like b, c, d, e for each factor DF

for link in urlsaGSP: 
    
#    b = GSP Dataframe
    remoteFile = url.urlopen(link + api_key_alpha + '&datatype=csv')
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    b = pd.DataFrame(data=html)
    b = b[0].str.split(",", expand = True)
    b.columns = b.iloc[0]
    b = b[1:] # b = GSP Dataframe
    b.rename(columns={'timestamp': 'Date'}, inplace=True)

for link2 in urlsaNDAQ: 
    
    # c = NASDAQ Dataframe
    remoteFile = url.urlopen(link2 + api_key_alpha + '&datatype=csv')
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    c = pd.DataFrame(data=html)
    c = c[0].str.split(",", expand = True)
    c.columns = c.iloc[0]
    c = c[1:] # c = NASDAQ Dataframe
    c.rename(columns={'timestamp': 'Date'}, inplace=True)

    
    
    # d = OPEC Dataframe
for link3 in urlsqOPEC: 
    remoteFile = url.urlopen(link3 + api_key_quandl)
    print(remoteFile)
    html1 = remoteFile.read().decode('ascii').splitlines()
    d = pd.DataFrame(data=html1)
    d = d[0].str.split(",", expand = True)
    d.columns = d.iloc[0]
    d = d[1:] # d = OPEC Dataframe

    # e = US Treasury Dataframe
for link4 in urlsqUSTREASURY: 
    remoteFile = url.urlopen(link4 + api_key_quandl)
    print(remoteFile)
    html1 = remoteFile.read().decode('ascii').splitlines()
    e = pd.DataFrame(data=html1)
    e = e[0].str.split(",", expand = True)
    e.columns = e.iloc[0]
    
    e = e[1:] # e = US Treasury Dataframe
    



#%%
    
#We pre-process the data from the quandl API in order to have 
#a similar dataframe structure and colum-naming

#we declare a list of clean dfs in order to append later in the loop
dataframes_clean =[]

#we list the unpreocessed dataframes d and e
unprocessed = [d, e]

#here we write the column that we will choose to replace to 'open' and 'close' 
columns = ['Value', '1 MO']

i = 0 
for column in columns: 
    if i < len(unprocessed): 
        
        unprocessed[i] = unprocessed[i].rename(columns={column: 'open'})
        unprocessed[i]['close'] = unprocessed[i]['open']
        dataframes_clean.append(unprocessed[i])
        

    i = i +1 
        
    
#%%

#Can only insert dataframes that have the 'open' and 'close' columns

#We run to identical loops one for each list of dataframes, at the moment
# they are separated in dataframes_clean (d and e) and dataframes (b,c)
    
    
for dataf in dataframes_clean: 
# We use an accumulator to iterate over the number of dataframes
    
    i = 1
    df = []
    for row in dataf.iterrows():
        if i < (len(dataf) - 5):
            
#            
            
            x = dataf.iloc[i]
            x1 = x['close']
            x2 = pd.to_numeric(x1)
            
            
            y = dataf.iloc[i + 4]
            y1 = y['open']
            y2 = pd.to_numeric(y1)
                
#We apply the formula to obtain the return and get a relative value
#instead of an absolute value              
        ret = ((x2 - y2) / y2) 

        df.append(ret)
        
        i = i + 1
        
    df = pd.DataFrame(df)
        
    dataf['return'] = df
    
dataframes = [b,c]

for dataf in dataframes: 
    

    i = 1
    df = []
    for row in dataf.iterrows():
        if i < (len(dataf) - 5):
            
            x = dataf.iloc[i]
            x1 = x['close']
            x2 = pd.to_numeric(x1)
            
            
            y = dataf.iloc[i + 4]
            y1 = y['open']
            y2 = pd.to_numeric(y1)

        ret = ((x2 - y2) / y2) 

        df.append(ret)
        
        i = i + 1
        
    df = pd.DataFrame(df)
        
    dataf['return'] = df               
    

#%%
#Finally, we need to post-process the dataframes and merge them. 

#This is sugar syntax to drop all the columns except the date and return. 
b = dataframes[0][['Date', 'return']]
c = dataframes[1][['Date', 'return']]
d = dataframes_clean[0][['Date', 'return']]
e = dataframes_clean[1][['Date', 'return']]

#We create a provisional dataframe where we will create a range of dates
#that are interesting for us, a "mask" of dates. We call this range of values
#under the name Date. 


ff1 = pd.bdate_range('2008-01-01', '2019-01-31')
ff1 = pd.DataFrame(ff1,columns=['Date'])
#We must invert the order of the date-values in order to have them descending. 
ff1 = ff1.iloc[::-1]

#A number of merges using the date as the index. 
b['Date']= pd.to_datetime(b['Date'])
ff2 = pd.merge(ff1, b, on=['Date'], how='left')

c['Date']= pd.to_datetime(c['Date'])
ff3 = pd.merge(ff2, c, on=['Date'], how='left')

d['Date']= pd.to_datetime(d['Date'])
ff4 = pd.merge(ff3, d, on=['Date'], how='left')

e['Date']= pd.to_datetime(e['Date'])
factors_dataframe = pd.merge(ff4, e, on=['Date'], how='left')

#Finally, we name the columns to keep track of each return
factors_dataframe.columns = ['Date', 'return urlsaGSP', 'return urlsaNDAQ', 'return urlsqOPEC', 'return urlsqUSTREASURY']
#We export it to a csv taking away the index-column. 
factors_dataframe.to_csv('/Users/yotroz/Ironhackers Dropbox/Octavio Ramirez/Work/MDBI_IE/Term_2/DATA_SCIENCE_MUNGING_ANALYTICS/VaR-Spark-Montecarlo/VaR_Spark/factors_returns.csv', index=False)

