#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import json
import csv
import numpy as np
import pickle


# In[2]:


#ZAD. 1


# In[3]:


df1 = pd.read_json(r"proj3_data1.json")

df2 = pd.read_json(r"proj3_data2.json")

df3 = pd.read_json(r"proj3_data3.json")


# In[48]:


df=df1.append(df2, ignore_index=True)
df=df.append(df3, ignore_index=True)


# In[5]:


df.to_json('ex01_all_data.json', orient='records')


# In[6]:


#ZAD 2


# In[7]:


missing_count=0
all_missing=[]
for col in df.columns:
    temp=[]
    missing_count=df[col].isna().sum()
    if missing_count > 0:
        temp.append(col)
        temp.append(missing_count)
        all_missing.append(temp)
    

    
    


# In[8]:


with open("ex02_no_nulls.csv", "w",newline='') as f:
    writer = csv.writer(f)
    for x in all_missing: 
        writer.writerow(x)


# In[9]:


#ZAD. 3
with open(r"proj3_params.json") as value:
    parameters = json.load(value)


# In[10]:


concat_col = []
for col in parameters['concat_columns']:
    concat_col.append(col)
    


# In[11]:


to_df=[]
for i in df.index:
    val=[]
    for col in concat_col:
        val.append(df[col][i])
    to_df.append(' '.join(val))
    
        


# In[12]:


df['description']=to_df


# In[13]:


df.to_json('ex03_descriptions.json', orient='records')


# In[14]:


#ZAD 4


# In[15]:


df_r = pd.read_json(r"proj3_more_data.json")


# In[16]:


df_l=df


# In[17]:


df_join=df_l.merge(df_r, on=parameters['join_column'],how='left')


# In[18]:


df_join.to_json('ex04_joined.json', orient='records')


# In[19]:


#ZAD5


# In[20]:


for index, row in df_join.iterrows():
    data={
        'make':row['make'],
        'model':row['model'],
        'body_type':row['body_type'],
        'doors':row['doors'],
        'top_speed':row['top_speed'],
        'acceleration':row['acceleration'],
        'fuel_consumption':row['fuel_consumption'],
        'engine':row['engine'],
        'displacement':row['displacement'],
        'horsepower':row['horsepower'],
        'fuel_type':row['fuel_type'],
        'cylinders':row['cylinders'],
        'emissions_class':row['emissions_class']
    }
    json_data = json.dumps(data)
    
    file_name = f'ex05_{row["description"].lower().replace(" ", "_")}.json'
    
    with open(file_name, 'w') as f:
        f.write(json_data)





# In[21]:


for index, row in df_join.iterrows():
    data = {
        "make": row['make'] if pd.notna(row['make']) else None,
        "model": row['model'] if pd.notna(row['model']) else None,
        "body_type": row['body_type'] if pd.notna(row['body_type']) else None,
        "doors": int(row['doors']) if pd.notna(row['doors']) else None,
        "top_speed": row['top_speed'] if pd.notna(row['top_speed']) else None,
        "acceleration": row['acceleration'] if pd.notna(row['acceleration']) else None,
        "fuel_consumption": row['fuel_consumption'] if pd.notna(row['fuel_consumption']) else None,
        "engine": row['engine'] if pd.notna(row['engine']) else None,
        "displacement": int(row['displacement']) if pd.notna(row['displacement']) else None,
        "horsepower": int(row['horsepower']) if pd.notna(row['horsepower']) else None,
        "fuel_type": row['fuel_type'] if pd.notna(row['fuel_type']) else None,
        "cylinders": int(row['cylinders']) if pd.notna(row['cylinders']) else None,
        "emissions_class": row['emissions_class'] if pd.notna(row['emissions_class']) else None
    }
    
    json_data = json.dumps(data)

    file_name = f'ex05_int_{row["description"].lower().replace(" ", "_")}.json'

    with open(file_name, 'w') as f:
        f.write(json_data)


# In[22]:


#ZAD6


# In[23]:


min_displacement = df_join['displacement'].min()


# In[24]:


max_displacement = df_join['displacement'].max()


# In[25]:


agg_list = parameters['aggregations']


# In[26]:


agg_res={}

for agg in agg_list:
    col, func = agg
    key = f'{func}_{col}'
    value = df_join[col].agg(func)
    agg_res[key] = value
    


# In[27]:


with open('ex06_aggregations.json', 'w') as f:
    json.dump(agg_res, f, indent=4)


# In[28]:


#ZAD. 7


# In[29]:


group_col = parameters['grouping_column']


# In[30]:


df_group=df_join.groupby(["make"])


# In[31]:


grouping_col = parameters['grouping_column']
num_cols = ['doors', 'top_speed', 'acceleration', 'fuel_consumption','displacement', 'horsepower', 'cylinders']


grouped = df_join.groupby(grouping_col)[num_cols].mean()

grouped = grouped[grouped.index.map(lambda x: len(df_join[df_join[grouping_col] == x]) > 1)]


# In[32]:


grouped.to_csv('ex07_groups.csv', index=True, header=True)


# In[33]:


#zad. 8


# In[34]:


pivot_cols = parameters['pivot_columns']


# In[35]:


pivot_index=parameters['pivot_index']


# In[36]:


pivot_val = parameters['pivot_values']


# In[37]:


df_to_pivot = df_join[[pivot_index,pivot_val,pivot_cols]]


# In[38]:


df_grouped = df_to_pivot.groupby(['make', 'fuel_type'])['fuel_consumption'].apply(lambda x: x.dropna().max())


df_result = df_grouped.reset_index(name='fuel_consumption')


# In[39]:


df_pivot=df_result.pivot_table(values=pivot_val,index=pivot_index,columns=pivot_cols)


# In[40]:


df_pivot.to_pickle('ex08_pivot.pkl')


# In[41]:


id_vars=parameters['id_vars']


# In[42]:


df_long = df_join.melt(id_vars=id_vars)


# In[43]:


df_long=df_long.sort_values(by=id_vars).reset_index()


# In[44]:


df_long.to_csv('ex08_melt.csv', index=False, header=True)


# In[45]:


stats = pd.read_csv(r"proj3_statistics.csv")


# In[46]:


pivot_index = stats.columns[0]

pivot_columns = stats.columns[1:]
new_columns = []
for col in pivot_columns:
    new_columns.append(col.split('_')[0])
    

