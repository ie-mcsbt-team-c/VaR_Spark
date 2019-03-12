# -*- coding: utf-8 -*-

#%%


i = 1
df = []

for row in c.iterrows():
    if i < (len(c) - 5):  
        
        x = c.iloc[i]
        x1 = x['close']
        x2 = pd.to_numeric(x1)
        
        
        y = c.iloc[i + 5]
        y1 = y['open']
        y2 = pd.to_numeric(y1)
        
        ret = ((x2 - y2) / y2) 

        df.append(ret)
    
        print(i)
        i = i + 1

df = pd.DataFrame(df)

c['return'] = df