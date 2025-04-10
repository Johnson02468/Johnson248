#!/usr/bin/env python
# coding: utf-8

# # We begin by importing the required modules and datasets

# In[1]:


import pandas as pd
import sqlite3 
import seaborn as sns


# In[2]:


get_ipython().system('pip3 install ipython-sql')


# In[3]:


cnn = sqlite3.connect('farm.db')


# In[4]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[5]:


get_ipython().run_line_magic('sql', 'sqlite:///farm.db')


# In[6]:


#We read all required CSV files and convert them so that we may perform sql operations


# In[7]:


#We can see that in the cheese, coffee, egg, honey, milk, state_lookup, and yogurt
#datasets have a text data type when it should have an integer data type. 

#Also cheese, coffee, egg, honey, milk


# In[8]:


cheese_production=pd.read_csv('cheese_production.csv')


# In[9]:


cheese_production.to_sql('cheese_production',
                         cnn,
                        if_exists= 'replace')


# In[10]:


get_ipython().run_cell_magic('sql', '', '\nSelect *\nFrom cheese_production\n')


# In[11]:


result = get_ipython().run_line_magic('sql', 'Select * From cheese_production')

cheese_production = result.DataFrame()


# In[12]:


cheese_production.head()


# In[13]:


cheese_production.info()


# In[14]:


cheese_production.nunique()


# In[15]:


cheese_production[cheese_production.duplicated()]


# In[16]:


cheese_production.isna().sum()


# In[17]:


cheese_production.describe().T


# In[18]:


cheese_production['Value']= pd.to_numeric(cheese_production['Value'].str.replace(',',''), errors = 'coerce')


# In[19]:


cheese_production.head()


# In[20]:


cheese_production.describe().T


# In[21]:


pd.set_option('display.float_format', '{:,.0f}'.format)


# In[22]:


cheese_production.describe().T


# In[23]:


cheese_production.to_sql('cheese_production', cnn, index=False, if_exists='replace')


# In[24]:


get_ipython().run_cell_magic('sql', '', '\nSelect Max(value) \nFrom cheese_production\n')


# In[25]:


get_ipython().run_cell_magic('sql', '', '\nSelect AVG(value) \nFrom cheese_production\n')


# In[26]:


cheese_production.hist(figsize=(20,20))


# In[27]:


cheese_production[['Value', 'State_ANSI', 'Year']].sort_values(by='Value', ascending=False)


# In[28]:


cheese_production.boxplot('Value')


# In[29]:


get_ipython().run_cell_magic('sql', '', '\nPRAGMA table_info(cheese_production);\n')


# In[30]:


get_ipython().run_cell_magic('sql', '', '\nSelect * \nFrom cheese_production\nWhere Value > 2200000000\n')


# In[31]:


get_ipython().run_cell_magic('sql', '', "\nUPDATE cheese_production \nSET value = REPLACE(value, ',', '')\n")


# In[32]:


get_ipython().run_cell_magic('sql', '', '\nALTER TABLE cheese_production\nADD Value2 int(32)\n')


# In[33]:


get_ipython().run_cell_magic('sql', '', '\nUpdate cheese_production\nSET Value2 = Value\n')


# In[34]:


get_ipython().run_cell_magic('sql', '', '\nALTER TABLE cheese_production\nDROP COLUMN Value\n')


# In[35]:


get_ipython().run_cell_magic('sql', '', '\nSelect *\nFrom cheese_production\n')


# In[36]:


get_ipython().run_cell_magic('sql', '', '\nSelect Count(State_ANSI)\nFrom cheese_production\nWhere State_ANSI is not null\n')


# In[37]:


#Note We may have to rename the column but sqlite3 doesn't offer a single command code that will rename columns
#We will have to copy the table with the columns name and rename it in a method similar to changing the data type


# In[41]:


get_ipython().run_cell_magic('sql', '', '\nSelect (Value2)\nFrom cheese_production\nWhere Value2 is null\n')


# # Coffee Production

# In[39]:


coffee_production=pd.read_csv('coffee_production.csv')


# In[40]:


coffee_production.to_sql('coffee_production',
                         cnn,
                        if_exists= 'replace')


# In[41]:


get_ipython().run_cell_magic('sql', '', '\nSelect *\nFrom coffee_production\n')


# In[42]:


result1 = get_ipython().run_line_magic('sql', 'Select * From coffee_production')
coffee_production = result1.DataFrame()


# In[43]:


coffee_production.head()


# In[44]:


coffee_production.info()


# In[45]:


coffee_production.nunique()


# In[46]:


coffee_production[coffee_production.duplicated()]


# In[47]:


coffee_production.isna().sum()


# In[48]:


coffee_production.describe().T


# In[49]:


coffee_production['Value']= pd.to_numeric(coffee_production['Value'].str.replace(',',''), errors = 'coerce')


# In[50]:


coffee_production.head()


# In[51]:


coffee_production.describe().T


# In[52]:


coffee_production.to_sql('coffee_production', cnn, index=False, if_exists='replace')


# In[53]:


get_ipython().run_cell_magic('sql', '', '\nSelect AVG(Value)\nFrom coffee_production\n')


# In[54]:


coffee_production.hist(figsize=(20,20))


# In[55]:


coffee_production[['Value', 'State_ANSI', 'Year']].sort_values(by='Value', ascending=False)


# In[56]:


coffee_production.boxplot('Value')


# In[57]:


get_ipython().run_cell_magic('sql', '', '\nPRAGMA table_info(coffee_production);\n')


# In[58]:


get_ipython().run_cell_magic('sql', '', '\nSelect *\nFrom coffee_production\nWHERE Value is null\n')


# # Egg Production

# In[59]:


egg_production=pd.read_csv('egg_production.csv')


# In[60]:


egg_production.to_sql('egg_production',
                         cnn,
                        if_exists= 'replace')


# In[61]:


get_ipython().run_cell_magic('sql', '', '\nSelect * \n\nFrom egg_production\n')


# In[62]:


result2 = get_ipython().run_line_magic('sql', 'Select * From egg_production')
egg_production = result2.DataFrame()


# In[63]:


egg_production.head()


# In[64]:


egg_production.info()


# In[65]:


egg_production.nunique()


# In[66]:


egg_production[egg_production.duplicated()]


# In[67]:


egg_production.isna().sum()


# In[68]:


egg_production.describe().T


# In[69]:


egg_production['Value']= pd.to_numeric(egg_production['Value'].str.replace(',',''), errors = 'coerce')


# In[70]:


egg_production.head()


# In[71]:


egg_production.describe().T


# In[72]:


egg_production.to_sql('egg_production', cnn, index=False, if_exists='replace')


# In[73]:


egg_production.hist(figsize=(20,20))


# In[74]:


egg_production[['Value', 'State_ANSI', 'Year']].sort_values(by='Value', ascending=False)


# In[75]:


egg_production.boxplot('Value')


# In[76]:


get_ipython().run_cell_magic('sql', '', '\nPRAGMA table_info(egg_production);\n')


# # honey production

# In[77]:


honey_production=pd.read_csv('honey_production.csv')


# In[78]:


honey_production.to_sql('honey_production',
                         cnn,
                        if_exists= 'replace')


# In[79]:


get_ipython().run_cell_magic('sql', '', '\nSelect * \nFrom honey_production\n')


# In[80]:


result3 = get_ipython().run_line_magic('sql', 'Select * From honey_production')
honey_production = result3.DataFrame()


# In[81]:


honey_production.head()


# In[82]:


honey_production.info()


# In[83]:


honey_production.nunique()


# In[84]:


honey_production[honey_production.duplicated()]


# In[85]:


honey_production.isna().sum()


# In[86]:


honey_production.describe().T


# In[87]:


honey_production['Value']= pd.to_numeric(honey_production['Value'].str.replace(',',''), errors = 'coerce')


# In[88]:


honey_production.head()


# In[89]:


honey_production.describe().T


# In[90]:


honey_production.to_sql('honey_production', cnn, index=False, if_exists='replace')


# In[91]:


honey_production.hist(figsize=(20,20))


# In[92]:


honey_production[['Value', 'State_ANSI', 'Year']].sort_values(by='Value', ascending=False)


# In[93]:


honey_production.boxplot('Value')


# In[94]:


get_ipython().run_cell_magic('sql', '', '\nPRAGMA table_info(honey_production);\n')


# # milk production

# In[95]:


milk_production=pd.read_csv('milk_production.csv')


# In[96]:


milk_production.to_sql('milk_production',
                         cnn,
                        if_exists= 'replace')


# In[97]:


get_ipython().run_cell_magic('sql', '', '\nSelect * \n\nFrom milk_production\n')


# In[98]:


result4 = get_ipython().run_line_magic('sql', 'Select * From milk_production')
milk_production = result4.DataFrame()


# In[99]:


milk_production.head()


# In[100]:


milk_production.info()


# In[101]:


milk_production.nunique()


# In[102]:


milk_production[milk_production.duplicated()]


# In[103]:


milk_production.isna().sum()


# In[104]:


milk_production.describe().T


# In[105]:


milk_production['Value']= pd.to_numeric(milk_production['Value'].str.replace(',',''), errors = 'coerce')


# In[106]:


milk_production.head()


# In[107]:


milk_production.describe().T


# In[108]:


milk_production.to_sql('milk_production', cnn, index=False, if_exists='replace')


# In[109]:


milk_production.hist(figsize=(20,20))


# In[110]:


milk_production[['Value', 'State_ANSI', 'Year']].sort_values(by='Value', ascending=False)


# In[111]:


milk_production.boxplot('Value')


# In[ ]:





# In[ ]:





# In[112]:


get_ipython().run_cell_magic('sql', '', '\nPRAGMA table_info(milk_production);\n')


# # state lookup

# In[113]:


state_lookup=pd.read_csv('state_lookup.csv')


# In[114]:


state_lookup.to_sql('state_lookup',
                         cnn,
                        if_exists= 'replace')


# In[115]:


get_ipython().run_cell_magic('sql', '', '\nSelect * \n\nFrom state_lookup\n')


# In[116]:


get_ipython().run_cell_magic('sql', '', '\nPRAGMA table_info(state_lookup);\n')


# # yogurt production

# In[117]:


yogurt_production=pd.read_csv('yogurt_production.csv')


# In[118]:


yogurt_production.to_sql('yogurt_production',
                         cnn,
                        if_exists= 'replace')


# In[119]:


get_ipython().run_cell_magic('sql', '', '\nSelect * \n\nFrom yogurt_production\n')


# In[120]:


result5 = get_ipython().run_line_magic('sql', 'Select * From yogurt_production')
yogurt_production = result5.DataFrame()


# In[121]:


yogurt_production.head()


# In[122]:


yogurt_production.info()


# In[123]:


yogurt_production.nunique()


# In[124]:


yogurt_production[yogurt_production.duplicated()]


# In[125]:


yogurt_production.isna().sum()


# In[126]:


yogurt_production.describe().T


# In[127]:


yogurt_production['Value']= pd.to_numeric(yogurt_production['Value'].str.replace(',',''), errors = 'coerce')


# In[128]:


yogurt_production.head()


# In[129]:


yogurt_production.describe().T


# In[130]:


yogurt_production.to_sql('yogurt_production', cnn, index=False, if_exists='replace')


# In[131]:


yogurt_production.hist(figsize=(20,20))


# In[132]:


yogurt_production[['Value', 'State_ANSI', 'Year']].sort_values(by='Value', ascending=False)


# In[133]:


yogurt_production.boxplot('Value')


# In[134]:


get_ipython().run_cell_magic('sql', '', '\nPRAGMA table_info(yogurt_production);\n')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# ## Scenario:

# 1.
# Question 1
# Can you find out the total milk production for 2023? Your manager wants this information for the yearly report.
# 
# What is the total milk production for 2023?

# In[137]:


get_ipython().run_cell_magic('sql', '', '\nSelect Sum(Value)\nFrom milk_production\nwhere Year = 2023\n')


# 2.
# Question 2
# Which states had cheese production greater than 100 million in April 2023? The Cheese Department wants to focus their marketing efforts there. 
# 
# How many states are there?

# In[148]:


get_ipython().run_cell_magic('sql', '', "\nSelect *\nFrom cheese_production\nWhere Value2 > 100000000\nAnd Year = 2023\nAnd Period = 'APR'\n")


# 3.
# Question 3
# Your manager wants to know how coffee production has changed over the years. 
# 
# What is the total value of coffee production for 2011?

# In[139]:


get_ipython().run_cell_magic('sql', '', '\nSelect Sum(value)\nFrom coffee_production\nWhere Year = 2011\n')


# 4.
# Question 4
# There's a meeting with the Honey Council next week. Find the average honey production for 2022 so you're prepared.

# In[140]:


get_ipython().run_cell_magic('sql', '', '\nSelect AVG(value)\nFrom honey_production\nWhere Year = 2022\n')


# 5.
# Question 5
# The State Relations team wants a list of all states names with their corresponding ANSI codes. Can you generate that list?
# 
# What is the State_ANSI code for Florida?

# In[141]:


get_ipython().run_cell_magic('sql', '', '\nSelect * \nFrom state_lookup\n')


# 6.
# Question 6
# For a cross-commodity report, can you list all states with their cheese production values, even if they didn't produce any cheese in April of 2023?
# 
# What is the total for NEW JERSEY?

# In[150]:


get_ipython().run_cell_magic('sql', '', "\nSelect \n    cp.Value2 AS cheese_prod_value, \n    cp.State_ANSI AS cheese_State_ANSI, \n    cp.Year AS cheese_prod_year,\n    cp.Period AS cheese_prod_period,\n    sl.State_ANSI AS SL_State_ANSI, \n    sl.State AS State\nFrom cheese_production cp\nLeft Join state_lookup sl on\ncp.State_ANSI = sl.State_ANSI\nWhere \n    cp.Year = 2023 AND\n    cp.Period = 'APR' AND\n    sl.State = 'NEW JERSEY'\n")


# 7.
# Question 7
# Can you find the total yogurt production for states in the year 2022 which also have cheese production data from 2023? This will help the Dairy Division in their planning.

# In[142]:


get_ipython().run_cell_magic('sql', '', '\nSelect \n    SUM(yp.Value)\nFrom \n    yogurt_production yp\nWhere \n    yp.Year = 2022\nAND \n    yp.State_Ansi IN (\n        SELECT DISTINCT \n            cp.State_Ansi \n        From \n            cheese_production cp\n        Where cp.Year = 2023\n                        )\n')


# 8.
# Question 8
# List all states from state_lookup that are missing from milk_production in 2023.
# 
# How many states are there?

# In[143]:


get_ipython().run_cell_magic('sql', '', '\nSelect\n    DISTINCT Count(State_ANSI)\nFrom state_lookup\n')


# In[168]:


get_ipython().run_cell_magic('sql', '', '\nSELECT s.State\nFROM state_lookup s\nLEFT JOIN milk_production m ON s.State_ANSI = m.State_ANSI AND m.Year = 2023\nWHERE m.State_ANSI IS NULL;\n')


# In[152]:


get_ipython().run_cell_magic('sql', '', '\nSelect\n    DISTINCT Count(State_ANSI)\nFrom\n(Select\n    mp.Value,\n    sl.State,\n    mp.State_ANSI,\n    mp.Period, \n    mp.Year\nFrom \n    milk_production mp \nLeft Join \n    state_lookup sl on\n    mp.State_ANSI = sl.State_ANSI\nWhere \n    Year = 2023\nORDER BY\n    mp.State_ANSI DESC)\n')


# 9.
# Question 9
# List all states with their cheese production values, including states that didn't produce any cheese in April 2023.
# 
# Did Delaware produce any cheese in April 2023?

# In[151]:


get_ipython().run_cell_magic('sql', '', "\nSelect\n    cp.Value2 AS cheese_prod_value,\n    sl.State AS State,\n    cp.Year AS cheese_prod_year,\n    cp.Period,\n    sl.StatE_ANSI\nFrom \n    cheese_production cp \nLeft Join \n    state_lookup sl on\n    cp.State_ANSI = sl.State_ANSI\nWhere \ncp.Period = 'APR'\nAND\ncp.Year = 2023\n")


# 10.
# Question 10
# Find the average coffee production for all years where the honey production exceeded 1 million.

# In[146]:


get_ipython().run_cell_magic('sql', '', '\nSelect\n    AVG(c.Value)\nFrom \n    coffee_production c \nWhere \n    c.Year IN (\n                Select \n                    h.Year \n                From\n                    honey_production h \n                Where h.Value > 1000000)\n')


# In[ ]:




