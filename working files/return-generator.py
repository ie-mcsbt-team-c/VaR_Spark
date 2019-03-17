# -*- coding: utf-8 -*-

#%%
import urllib.request as url
import pandas as pd 


#Here we declare as variables 

start_date     = '2010-02-21'
end_date       = '2019-03-11'
api_key_alpha  = 'SQ60DKQWSFAU53XH'
api_key_quandl = 'ujUGW2cbgsDmD7sP359j'


urlsaGSP        = ['https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=^GSPC&outputsize=full&apikey=']

urlsaNDAQ       = ['https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NDAQ&outputsize=full&apikey=']

urlsqOPEC       = ['https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=' + start_date + '&end_date=' + end_date + '&api_key=']

urlsqUSTREASURY = ['https://www.quandl.com/api/v3/datasets/USTREASURY/YIELD.csv?start_date=' + start_date + '&end_date=' + end_date + '&api_key=']


for link in urlsaGSP: 
    
    remoteFile = url.urlopen(link + api_key_alpha + '&datatype=csv')
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    b = pd.DataFrame(data=html)
    b = b[0].str.split(",", expand = True)
    b.columns = b.iloc[0]
    b = b[1:] # b = GSP Dataframe
    b.rename(columns={'timestamp': 'Date'}, inplace=True)

for link2 in urlsaNDAQ: 
    
    remoteFile = url.urlopen(link2 + api_key_alpha + '&datatype=csv')
    print(remoteFile)
    html = remoteFile.read().decode('ascii').splitlines()
    c = pd.DataFrame(data=html)
    c = c[0].str.split(",", expand = True)
    c.columns = c.iloc[0]
    c = c[1:] # c = NASDAQ Dataframe
    c.rename(columns={'timestamp': 'Date'}, inplace=True)

#    c.drop(c.index[1])
    
    
    
for link3 in urlsqOPEC: 
    remoteFile = url.urlopen(link3 + api_key_quandl)
    print(remoteFile)
    html1 = remoteFile.read().decode('ascii').splitlines()
    d = pd.DataFrame(data=html1)
    d = d[0].str.split(",", expand = True)
    d.columns = d.iloc[0]
    d = d[1:] # d = OPEC Dataframe


for link4 in urlsqUSTREASURY: 
    remoteFile = url.urlopen(link4 + api_key_quandl)
    print(remoteFile)
    html1 = remoteFile.read().decode('ascii').splitlines()
    e = pd.DataFrame(data=html1)
    e = e[0].str.split(",", expand = True)
    e.columns = e.iloc[0]
    
    e = e[1:] # e = US Treasury Dataframe
    



#%%

dataframes_clean =[]
#here we write the column that we will choose as the open and close 
unprocessed = [d, e]
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

for dataf in dataframes_clean: 
    
    i = 1
    df = []
    for row in dataf.iterrows():
        if i < (len(dataf) - 5):
#            if dataf == (b) | (c): 
            
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
    
dataframes = [b,c]

for dataf in dataframes: 
    

    i = 1
    df = []
    for row in dataf.iterrows():
        if i < (len(dataf) - 5):
#            if dataf == (b) | (c): 
            
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


b = dataframes[0][['Date', 'return']]
c = dataframes[1][['Date', 'return']]
d = dataframes_clean[0][['Date', 'return']]
e = dataframes_clean[1][['Date', 'return']]

ff1 = pd.bdate_range('2008-01-01', '2019-01-31')
ff1 = pd.DataFrame(ff1,columns=['Date'])
ff1 = ff1.iloc[::-1]


b['Date']= pd.to_datetime(b['Date'])
ff2 = pd.merge(ff1, b, on=['Date'], how='left')

c['Date']= pd.to_datetime(c['Date'])
ff3 = pd.merge(ff2, c, on=['Date'], how='left')

d['Date']= pd.to_datetime(d['Date'])
ff4 = pd.merge(ff3, d, on=['Date'], how='left')

e['Date']= pd.to_datetime(e['Date'])
factors_dataframe = pd.merge(ff4, e, on=['Date'], how='left')

factors_dataframe.columns = ['Date', 'return urlsaGSP', 'return urlsaNDAQ', 'return urlsqOPEC', 'return urlsqUSTREASURY']

factors_dataframe.to_csv('/Users/yotroz/Ironhackers Dropbox/Octavio Ramirez/Work/MDBI_IE/Term_2/DATA_SCIENCE_MUNGING_ANALYTICS/factors_data_frame.csv')

