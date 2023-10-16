import pandas as pd
import logging
import os
import numpy as np
import boto3
from io import StringIO
import pprint
import shutil
import time
import sys
from datetime import datetime, timezone
import readProperties
from pypdf import PdfReader
import re

session=boto3.Session(profile_name='RT')
s3_client=session.client('athena')

pdf_path='c:\\Users\\megala\\Documents\\Quick_Automat.pdf'
Config=readProperties.Readconfig
achive_path=Config.getData("Queries","Archive_path")
output_path=Config.getData("Queries","html_path")
Temp_query=Config.getData("Queries","selected_values")

if Temp_query == 'provider_view':
  header='count'
  merge='header'
  comp1='count'
  comp2='count_id'
  section="<h2>Provider Views & Assesment<h2>"
  sheet_name1='values'
  html=""<table><tr><th>Header</th><th>Count</th><th>Matching Status</th></tr>
elif Temp_query=='Assessed_by_status':
  header='statusdisplay(COUNT)'
  merge='statusdisplay'
  comp1='statusdisplay (count)'
  comp2='assessed_count'
  section="<h2>Assessed by status<h2>"
  sheet_name1='assessed'
  html=""<table><tr><th>statusdisplay</th><th>statusdisplay (COUNT)</th><th>Matching Status</th></tr>
elif Temp_query=='Viewed':
  header='viewedcount'
  merge='vieweddisplay'
  comp1='vieweddisplaycount_dashb'
  comp2='vieweddisplaycount_Athen'
  section="<h2>Viewd by status<h2>"
  sheet_name1='viewed'
  html=""<table><tr><th>vieweddisplay</th><th>vieweddisplay (COUNT)</th><th>Matching Status</th></tr>
def valueconsolidation():
  #give quicksight dashboard pdf

  reader=PdfReader(pdf_path)
  page=reader.pages[0]

  #folder_compw created
  path= './Dashboard_values'

  #check whether directory already exists
  if not os.path.exists(path):
    os.mkdir(path)
    print("Folder %s created!" %path)
  else:
    print(""Folder %s alreadt exists!" %path)

  dict_list={
    'viewed alerts':0
    'viewed conditions':0
    'assesed condition':0
  }

  #creating file
  try:
      with open('output.txt','r'):
                file_Exist=True
except FileNotFoundError:
          file_exist=False
with open('output.txt', 'w' if file_exist else 'w') as file:
        file.write(page.extract_text()_

#collecting element of dashboard

with open('output.txt','r') as file:
  lines=file.readlines() #list datatype
  for l in lines:
          if 'viewed alerts' in l:
                  match=re.search(r'\d+',l) #digit value
                  dict_list['viewed alerts']=int(match.group())
                  print('viewed alerts', int(match.group()))
          elif 'condition viewed' in l:
                  match=re.search(r'\d+',l) #digit value
                  dict_list['condition viewed']=int(match.group())
                  print('condition viewed', int(match.group()))        
          elif 'condition assessed' in l:
                  match=re.search(r'\d+',l) #digit value
                  dict_list['condition assessed']=int(match.group())
                  print('condition assessed', int(match.group()))

#final dataframe of values fetched
string_val = pd.DataFrame(dict_list.items(), columns==['header','count'])
#********2nd integration*******
provider_path="c:\\Users\\Megala\\Automation_pytest\\provider_details_excel"
#os.listdir(provider_path)

providerdetails_df=[]

for a in os.listdir(provider_path):
  k=pd.read_csv(os.path.join(provider_path,a))
  # k ['percent assessed'] = (k['Percent_assessed']/100.appy(lambda x: '{:.1f}%'.format(x))
providerdetails_df.append(k)

table_1=pd.concat(providerdetails_df)
#********3rd integration*******
provider_path="c:\\Users\\Megala\\Automation_pytest\\conditions_viewed"
#os.listdir(provider_path)

conditions_viewed_df=[]

for a in os.listdir(conditions_viewed_path):
  k=pd.read_csv(os.path.join(conditions_viewed_path,a))
  # k ['percent assessed'] = (k['Percent_assessed']/100.appy(lambda x: '{:.1f}%'.format(x))
Conditions_Viewed_df.append(k)

table_2=pd.concat(Conditions_Viewed_df)
#######################################################3

utility_path = "c:\\Users\\Megala\\Automation_pytest\\2_3_4_sections"
utility_list = os.listdir(utility_path)

for i in utility_list:
  if 'conditions_Assessd' in i:
           condition_access_df=pd.read_csv((os.path.join(utility_path,i)))
  elif 'Conditions_Viewed' in i:
           condition_view_df=pd.read_csv((os.path.join(utility_path,i)))
  elif 'Provider_Activity' in i:
           prov_activity_df=pd.read_csv((os.path.join(utility_path,i)))
#writing to xlsx file

try:
  with pd.ExcelWriter(os.path.abspath('Dashboard_Values/final.xlsx')) as writer:
      string_val.sort_values(by=['count']).to_excel(writer, sheet_name='values', index=False)
      condition_access.df.to_excel(writer, sheet_name='condition_assessed', index=False)
      condition_view_df.to_excel(writer, sheet_name='condition_viewed', index=False)

except PermissionError:
  print("Permission Denied: Please check if file is open in excel")

def query_athena():
  #print (s3.client)
  time.sleep(2)
  Temp_Query=config.getData("Queries:,"selected_values")
  Q = s3_client_start_query_Execution(
     QueryString = Config.getData("Queries",Temp_query),
     QueryExecutionContext={'OuputLocation':'s3://folder/abc/'}
     )

  print('going to wait for few sec')
  polling_interval_seconds=10
  timeout_seconds=120
  start_time=time.time()
  while True:
      response=s3_client.get_query_Execution(
          QueryExecutionId=Q['QueryExecutionId']
          )
          query_status = response['QueryExecution']['Status']['State']
          if query_status in ['SUCCEEDED','FAILED','CANCELLED']:
            break
          if time.time()-start_time>timeout_Seconds:
            break
          if query_status in ['QUEUED','RUNNING']:
            polling_interval_seconds=10
          else:
            polling_interval_seconds=100
#print(Q)
time.sleep(polling_interval_seconds)
query_exc_id=(Q['QueryExecutionId'])
#download_athena_results(query_exc_id)
return query_exc_id

def move_file(archive_path, output_path):
  os.makedirs(achive_path, exist_ok=True)
  reports=[file for file in os.listdir(outpur_path)if file.startswith("report_")]
  for report in reports:
    report_path = os.path.join(output_path,report)
    destination_path=os.path.join(archive_path, report)
    shutil.move(report_path,destination_path)

def download_athena_results(in_query_exc_id):
  s3_object = session.client('s3')
  aws_bucket_name="somerandom"
  qry_execuito_id=in_query_Exc_id
  obj_key="somerandom/"+qry_execuito_id+".csc"
  print(obj_key)
  obj = s3_object.get_object(Bucket=aws_bucket_name, Key=obj_key)
  athena_qry_rest_df=pd.read_csv(obj['Body'])  #5'Body' is a key word
  return athena_qey_res_df

def compare_csv(athena_df1):
  df_dash1=pd.excel("c:\\Users\\Megala\\Automation_pytest\\final.xlsx',sheet_name=sheet_name1)
  df_dash=df_dash1.sort_values(by=header,ascending=True)

print(df_dash)
if Temp_query=='Assessed_by_Status' or Temp_query=='Conditions_Viewed' or Temp_query=='Activity_By_Month':
  df_dash.colums=df_dash.columns.str.lower()
  athena_df1.colums=athena_df1.columns.str.lower()
print(athena_df1)

#Merge the DataFrames to compare
merged_df=df_dash.merge(athena_df1, on=merrge, suffixes=('_Dashboard', '_Athena'))
print(merged_df)

merged_df['Result']=merged_df.apply(compare_values, axis=1)
report_html=merged_df.to_html(index=False)
output_file=config.getData("Queries","html_path")

with open(html_path,'w') as f:
f.write(report_html)

def compare_values(row):
 if Temp_query!='Activiy_by_Month' and Temp_query!='Conditions_Viewed' and Temp_query!='Usage_Detail'
   if row[comp1]==row[comp2]:

     return 'match'
   else:
     return 'mismatch'
  elif temp_query=='Activity_by_Month':
    if row[comp1]==row[comp3]:
      if row[comp2]==row[comp4]:
         return 'match'
      else:
         return 'mismatch'
  else:
         return 'mismatch'
  
 elif temp_query=='Conditions_Viewed':
    if row[comp2]==row[comp5]:
      if row[comp3]==row[comp6]:
         return 'match'
      else:
         return 'mismatch'
  else:
         return 'mismatch'

   elif temp_query=='Usage_detail':
    if row[comp1]==row[comp4]:
      if row[comp2]==row[comp5]:
         return 'match'
      else:
         return 'mismatch'
  else:
         return 'mismatch'

def Athena_view_comparison():
  query_wxc_id=query_athena()
  athena_qry_res_df=download_athena_results(query_Exc_id)
  compare_csv(athena_qey_res_df)
  
