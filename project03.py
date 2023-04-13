import pandas as pd
import json
import csv
import numpy as np
import pickle

#ZAD. 1

df1 = pd.read_json(r"proj3_data1.json")
df2 = pd.read_json(r"proj3_data2.json")
df3 = pd.read_json(r"proj3_data3.json")

df=df1.append(df2, ignore_index=True)
df=df.append(df3, ignore_index=True)

df.to_json('ex01_all_data.json', orient='records')

#ZAD 2

missing_count=0
all_missing=[]
for col in df.columns:
    temp=[]
    missing_count=df[col].isna().sum()
    if missing_count > 0:
        temp.append(col)
        temp.append(missing_count)
        all_missing.append(temp)

with open("ex02_no_nulls.csv", "w",newline='') as f:
    writer = csv.writer(f)
    for x in all_missing: 
        writer.writerow(x)

#ZAD. 3
with open(r"proj3_params.json") as value:
    parameters = json.load(value)
    
concat_col = []
for col in parameters['concat_columns']:
    concat_col.append(col)
    
to_df=[]
for i in df.index:
    val=[]
    for col in concat_col:
        val.append(df[col][i])
    to_df.append(' '.join(val))

df['description']=to_df

df.to_json('ex03_descriptions.json', orient='records')

#ZAD 4

df_r = pd.read_json(r"proj3_more_data.json")
df_l=df
df_join=df_l.merge(df_r, on=parameters['join_column'],how='left')
df_join.to_json('ex04_joined.json', orient='records')

#ZAD5

# df_join_drop=df_join.drop(['description'],axis=1)

# for index, row in df_join.iterrows():
#     data = {}
#     for col in df_join_drop.columns:
#         data[col] = row[col]
    
#     json_data = json.dumps(data)
    
#     file_name = f'ex05_{row["description"].lower().replace(" ", "_")}.json'
    
#     with open(file_name, 'w') as f:
#         f.write(json_data)




# for index, row in df_join.iterrows():
#     data = {}
#     for col in df_join_drop.columns:
#         if pd.notna(row[col]):
#             if col in parameters['int_columns']:
#                 data[col] = int(row[col])
#             else:
#                 data[col] = row[col]
#         else:
#             data[col] = None
        
#     json_data = json.dumps(data)

#     file_name = f'ex05_int_{row["description"].lower().replace(" ", "_")}.json'

#     with open(file_name, 'w') as f:
#         f.write(json_data)


for index, row in df_join.iterrows():
    dscrb = row["description"].lower().replace(" ", "_")
    row[df_join.columns != 'description'].to_json(f"ex05_{dscrb}.json")
    row.replace(np.nan,None,inplace=True)
    row[parameters['int_columns']]=row[parameters['int_columns']].astype('Int64')
    row[df_join.columns != 'description'].to_json(f"ex05_int_{dscrb}.json")
    
  

#ZAD6

min_displacement = df_join['displacement'].min()
max_displacement = df_join['displacement'].max()
agg_list = parameters['aggregations']
agg_res={}

for agg in agg_list:
    col, func = agg
    key = f'{func}_{col}'
    value = df_join[col].agg(func)
    agg_res[key] = value

with open('ex06_aggregations.json', 'w') as f:
    json.dump(agg_res, f, indent=4)

#ZAD. 7

group_col = parameters['grouping_column']
df_group=df_join.groupby(["make"])
grouping_col = parameters['grouping_column']
num_cols = ['doors', 'top_speed', 'acceleration', 'fuel_consumption','displacement', 'horsepower', 'cylinders']
grouped = df_join.groupby(grouping_col)[num_cols].mean()
grouped = grouped[grouped.index.map(lambda x: len(df_join[df_join[grouping_col] == x]) > 1)]
grouped.to_csv('ex07_groups.csv', index=True, header=True)

#zad. 8

pivot_cols = parameters['pivot_columns']
pivot_index=parameters['pivot_index']
pivot_val = parameters['pivot_values']
df_to_pivot = df_join[[pivot_index,pivot_val,pivot_cols]]
df_grouped = df_to_pivot.groupby(['make', 'fuel_type'])['fuel_consumption'].apply(lambda x: x.dropna().max())
df_result = df_grouped.reset_index(name='fuel_consumption')
df_pivot=df_result.pivot_table(values=pivot_val,index=pivot_index,columns=pivot_cols)
df_pivot.to_pickle('ex08_pivot.pkl')

id_vars=parameters['id_vars']
df_long = df_join.melt(id_vars=id_vars)
df_long.to_csv('ex08_melt.csv', index=False, header=True)

stats = pd.read_csv(r"proj3_statistics.csv")

pivot_index = stats.columns[0]

pivot_columns = stats.columns[1:]
new_columns = []
for col in pivot_columns:
    new_columns.append(col.split('_')[0])
    
