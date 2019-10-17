import pandas as pd

df=pd.read_csv("P9-ConsoleGames.csv")
#df1=pd.read_csv("Games Data/P9-ConsoleDates.csv")
df= df.dropna(axis=0, subset=['Name'])
df['Platform'] = df['Platform'].fillna('N/A')
df['Publisher'] = df['Publisher'].fillna('N/A')
df['Year'] = df['Year'].fillna(0.0)
cols=["NA_Sales","EU_Sales","JP_Sales","Other_Sales"]
df[cols]=df[cols].fillna(0.0)
#df1['Comment'] = df1['Comment'].fillna('N/A')
#df1['Discontinued']=df1['Discontinued'].fillna('0')
newdict=df.to_dict('records')
print(newdict)
#dict1=df1.to_dict('records')