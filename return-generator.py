# -*- coding: utf-8 -*-

#%%
import urllib.request as url
import pandas as pd 

#remoteFile = url.urlopen('https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=2010-02-21&end_date=2019-02-21&api_key=ujUGW2cbgsDmD7sP359j')
#html = remoteFile.read().decode('ascii').splitlines()
#print(html)

#FACTORS : Try to do a for loop for getting all the stock 
#S&P 500 



start_date     = '2010-02-21'
end_date       = '2019-03-11'
api_key_alpha  = 'SQ60DKQWSFAU53XH'
api_key_quandl = 'ujUGW2cbgsDmD7sP359j'


urlsaGSP = ['https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=^GSPC&outputsize=full&apikey=']

urlsaNDAQ = ['https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NDAQ&outputsize=full&apikey=']

urlsqOPEC = ['https://www.quandl.com/api/v3/datasets/OPEC/ORB/data.csv?start_date=' + start_date + '&end_date=' + end_date + '&api_key=']

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

dataframes = [b,c,d,e]

for dataf in dataframes: 
    

    i = 1
    df = []
    for row in dataf.iterrows():
        if i < (len(c) - 5):
            if dataf is b or c: 
            
                x = dataf.iloc[i]
                x1 = x['close']
                x2 = pd.to_numeric(x1)
                
                
                y = dataf.iloc[i + 5]
                y1 = y['open']
                y2 = pd.to_numeric(y1)
                
            elif dataf == d: 
                x = dataf.iloc[i]
                x1 = x['Value']
                x2 = pd.to_numeric(x1)
                
                
                y = dataf.iloc[i + 5]
                y1 = y['Value']
                y2 = pd.to_numeric(y1)
                
            elif dataf == e: 
                x = dataf.iloc[i]
                x1 = x['1 MO']
                x2 = pd.to_numeric(x1)
                
                y = dataf.iloc[i + 5]
                y1 = y['1 MO']
                y2 = pd.to_numeric(y1)

                
                
            ret = ((x2 - y2) / y2) 
    
            df.append(ret)
            
            i = i + 1
        
        df = pd.DataFrame(df)
        
#    print(dataf)
        dataf['return'] = df
