
# coding: utf-8

# In[1]:


import pandas as pd
import pandas_profiling


# In[2]:


# prepare dataframe for analysis
data = pd.read_csv('C:\\Users\\steve\\OneDrive\\Documents\\Python Scripts\\fish_tracker\\processed_data.csv', 
    names=['Date','Time','Duration','Type','Tag ID','Count','Gap', 'Tower'])
data['Date'] = data['Date'].astype('datetime64[ns]')
data['Time'] = pd.to_timedelta(data['Time'])
data['Duration'] = pd.to_timedelta(data['Duration'])


# In[3]:


data.shape


# In[5]:


profile = pandas_profiling.ProfileReport(data)
profile.to_file(outputfile="C:\\Users\\steve\\OneDrive\\Documents\\Python Scripts\\fish_tracker\\pandas_profile.html")

