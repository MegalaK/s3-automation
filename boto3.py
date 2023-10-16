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
  section="<h2>Provider Views & Pro









