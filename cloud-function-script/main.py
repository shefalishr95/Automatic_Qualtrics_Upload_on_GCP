#!/usr/bin/env python
# coding: utf-8

# # **BigQuery <> Qualtrics set up**

# ### 1. Extract survey results from Qualtrics

# In[31]:


from QualtricsAPI.Setup import Credentials

# Call the qualtrics_api_credentials() method (Non-XM Directory Users)
Credentials().qualtrics_api_credentials(token = 'YOUR TOKEN HERE',data_center='DATA CENTER NUMBER HERE')

from QualtricsAPI.Survey import Responses

# Get Survey Responses 
df = Responses().get_survey_responses(survey="SURVEY ID HERE")

# Check data
pd.set_option('display.max_columns', None) #Allows us to see all columns


# In[107]:


# Check data 
df.head(30)

# Check all column names
print(df.columns.tolist()) 


# ### 2. Data transformation: Clean the data and remove unnecessary variables

# #### Step 2.2: We remove the responses collected during Qualtrics' 'preview' mode and incomplete responses

# In[36]:


# Drop unnecessary values
try:
    df.drop(df[df.DistributionChannel == "preview"].index, inplace=True)
    df.drop(df[df.Finished == "0"].index, inplace=True)
    except:
        pass # Not recommended, but necessary here.


# #### Step 2.1: We keep all the main variables (i.e., all questions), as well as any variable that may help us remove 'insincere' responses such as start/end date, duration etc.

# In[38]:


# Drop unnecessary columns
df = df.drop(["Status", "IPAddress", "Progress", "RecipientLastName", "RecipientFirstName", "RecipientEmail",
         "ExternalReference", "LocationLatitude", "LocationLongitude", "UserLanguage"
        ],
        axis=1)


# In[40]:


# Check results
df.head(30)


# #### Step 2.2: Combine teacher and leader responses to create a single dataset (w/o any blank rows)

# In[41]:


# Set conditions (Role 1 or Role 2)
role_conditions = [df['role'] == '1', df['role'] == '2'] #Role 1 is '1' and Role 2 is '2'


# In[45]:


# Define list of 'choices' 
choices_curriculum_name = [df['curriculum_name'],df['leader_curriculum']]
choices_use = [df['use'],df['leader_engaging']]
choices_not_use = [df['not_use'],df['leader_not_engage']]
choices_use_level = [df['teacher_use'],df['leader_engage_explai']]
choices_modifications = [df['modifications'],df['leader_modification']]
choices_modifications_type = [df['modifications_type'],df['leader_mod_types']]
choices_modification_example = [df['modification_example'],df['leader_mod_example']]
choices_length_curriculum = [df['length_curriculum'],df['leader_length']]
choices_units = [df['units'],df['leader_units']]
choices_units_open = [df['units_open'],df['leader_use']]
choices_feel = [df['feel'],df['leader_feel']]
choices_feel_explain = [df['feel_explain'],df['leader_feel_explain']]
choices_current_role = [df['current_role'],df['leader_role']]
choices_teaching_length = [df['teaching_length'],df['leader_role_length']]
choices_school_length = [df['school_length'],df['leader_years']]
choices_gender = [df['gender'],df['leader_gender']]
choices_race_ethnicity = [df['race_ethnicity'],df['leader_race']]
choices_opt_in = [df['opt_in_6'],df['leader_opt-in_4']]
choices_feel_statements_1 = [df['feel_statements_1'],df['leader_statements_1']]
choices_feel_statements_2 = [df['feel_statements_30'],df['leader_statements_21']]
choices_feel_statements_3 = [df['feel_statements_31'],df['leader_statements_22']]
choices_feel_statements_4 = [df['feel_statements_32'],df['leader_statements_23']]
choices_feel_statements_5 = [df['feel_statements_33'],df['leader_statements_24']]
choices_feel_statements_6 = [df['feel_statements_34'],df['leader_statements_25']]
choices_feel_statements_7 = [df['feel_statements_35'],df['leader_statements_26']]
choices_feel_statements_8 = [df['feel_statements_36'],df['leader_statements_27']]
choices_feel_statements_9 = [df['feel_statements_37'],df['leader_statements_28']]
choices_feel_statements_10 = [df['feel_statements_38'],df['leader_statements_29']]
choices_feel_statements_11 = [df['feel_statements_39'],df['leader_statements_30']]
choices_feel_statements_12 = [df['feel_statements_40'],df['leader_statements_31']]
choices_feel_statements_13 = [df['feel_statements_41'],df['leader_statements_32']]
choices_feel_statements_14 = [df['feel_statements_42'],df['leader_statements_33']]
choices_feel_statements_15 = [df['feel_statements_43'],df['leader_statements_34']]
choices_feel_statements_16 = [df['feel_statements_44'],df['leader_statements_35']]
choices_feel_statements_17 = [df['feel_statements_45'],df['leader_statements_36']]
choices_feel_statements_18 = [df['feel_statements_46'],df['leader_statements_37']]
choices_feel_statements_19 = [df['feel_statements_47'],df['leader_statements_38']]
choices_feel_statements_20 = [df['feel_statements_48'],df['leader_statements_39']]


# In[46]:


# Create new columns for the combined dataset (we'll populare the choices in these new columns)

df = df.assign(**{col: df.get(col, '')
                  for col in ['combined_curriculum_name','combined_use','combined_not_use',
                              'combined_use_level','combined_modifications','combined_modifications_type',
                              'combined_modification_example', 'combined_length_curriculum', 'combined_units',
                              'combined_units_open', 'combined_feel', 'combined_feel_explain', 
                              'combined_current_role', 'combined_teaching_length', 'combined_school_length', 
                              'combined_gender', 'combined_race_ethnicity', 'combined_opt_in',
                             'combined_feel_statements_1', 'combined_feel_statements_2', 
                              'combined_feel_statements_3', 'combined_feel_statements_4',
                              'combined_feel_statements_5', 'combined_feel_statements_6',
                              'combined_feel_statements_7', 'combined_feel_statements_8',
                              'combined_feel_statements_9', 'combined_feel_statements_10',
                              'combined_feel_statements_11', 'combined_feel_statements_12',
                              'combined_feel_statements_13', 'combined_feel_statements_14',
                              'combined_feel_statements_15', 'combined_feel_statements_16',
                              'combined_feel_statements_17', 'combined_feel_statements_18',
                              'combined_feel_statements_19','combined_feel_statements_20']}
              )


# In[47]:


# Populate data in the new columns created above
df['combined_curriculum_name'] = np.select(role_conditions,choices_curriculum_name, default='')
df['combined_use'] = np.select(role_conditions,choices_use, default='')
df['combined_not_use'] = np.select(role_conditions,choices_not_use, default='')
df['combined_use_level'] = np.select(role_conditions,choices_use_level, default='')
df['combined_modifications'] = np.select(role_conditions,choices_modifications, default='')
df['combined_modifications_type'] = np.select(role_conditions,choices_modifications_type, default='')
df['combined_modification_example'] = np.select(role_conditions,choices_modification_example, default='')
df['combined_length_curriculum'] = np.select(role_conditions,choices_length_curriculum, default='')
df['combined_units'] = np.select(role_conditions,choices_units, default='')
df['combined_units_open'] = np.select(role_conditions,choices_units_open, default='')
df['combined_feel'] = np.select(role_conditions,choices_feel, default='')
df['combined_feel_explain'] = np.select(role_conditions,choices_feel_explain, default='')
df['combined_current_role'] = np.select(role_conditions,choices_current_role, default='')
df['combined_teaching_length'] = np.select(role_conditions,choices_teaching_length, default='')
df['combined_school_length'] = np.select(role_conditions,choices_school_length, default='')
df['combined_gender'] = np.select(role_conditions,choices_gender, default='')
df['combined_race_ethnicity'] = np.select(role_conditions,choices_race_ethnicity, default='')
df['combined_opt_in'] = np.select(role_conditions,choices_opt_in, default='')
df['combined_feel_statements_1'] = np.select(role_conditions,choices_feel_statements_1, default='')
df['combined_feel_statements_2'] = np.select(role_conditions,choices_feel_statements_2, default='')
df['combined_feel_statements_3'] = np.select(role_conditions,choices_feel_statements_3, default='')
df['combined_feel_statements_4'] = np.select(role_conditions,choices_feel_statements_4, default='')
df['combined_feel_statements_5'] = np.select(role_conditions,choices_feel_statements_5, default='')
df['combined_feel_statements_6'] = np.select(role_conditions,choices_feel_statements_6, default='')
df['combined_feel_statements_7'] = np.select(role_conditions,choices_feel_statements_7, default='')
df['combined_feel_statements_8'] = np.select(role_conditions,choices_feel_statements_8, default='')
df['combined_feel_statements_9'] = np.select(role_conditions,choices_feel_statements_9, default='')
df['combined_feel_statements_10'] = np.select(role_conditions,choices_feel_statements_10, default='')
df['combined_feel_statements_11'] = np.select(role_conditions,choices_feel_statements_11, default='')
df['combined_feel_statements_12'] = np.select(role_conditions,choices_feel_statements_12, default='')
df['combined_feel_statements_13'] = np.select(role_conditions,choices_feel_statements_13, default='')
df['combined_feel_statements_14'] = np.select(role_conditions,choices_feel_statements_14, default='')
df['combined_feel_statements_15'] = np.select(role_conditions,choices_feel_statements_15, default='')
df['combined_feel_statements_16'] = np.select(role_conditions,choices_feel_statements_16, default='')
df['combined_feel_statements_17'] = np.select(role_conditions,choices_feel_statements_17, default='')
df['combined_feel_statements_18'] = np.select(role_conditions,choices_feel_statements_18, default='')
df['combined_feel_statements_19'] = np.select(role_conditions,choices_feel_statements_19, default='')
df['combined_feel_statements_20'] = np.select(role_conditions,choices_feel_statements_20, default='')


# In[48]:


# Extract the relevant columns from dataframe 'df' to created a new dataset: 'df_combined' by copying the relevant columns

df_combined = df[['StartDate', 'EndDate','Duration (in seconds)','Finished','RecordedDate','ResponseId',
                  'DistributionChannel','name_6','name_7','email','role','task','combined_curriculum_name',
                  'combined_use','combined_not_use','combined_use_level','combined_modifications',
                  'combined_modifications_type','combined_modification_example','combined_length_curriculum',
                  'combined_units','combined_units_open','combined_feel','combined_feel_explain',
                  'combined_current_role','combined_teaching_length','combined_school_length',
                  'combined_gender','combined_race_ethnicity','combined_opt_in','combined_feel_statements_1',
                  'combined_feel_statements_2','combined_feel_statements_3','combined_feel_statements_4',
                  'combined_feel_statements_5','combined_feel_statements_6','combined_feel_statements_7',
                  'combined_feel_statements_8','combined_feel_statements_9','combined_feel_statements_10',
                  'combined_feel_statements_11','combined_feel_statements_12','combined_feel_statements_13',
                  'combined_feel_statements_14','combined_feel_statements_15','combined_feel_statements_16',
                  'combined_feel_statements_17','combined_feel_statements_18','combined_feel_statements_19',
                  'combined_feel_statements_20','IP_1','IP_2','IP_3','IP_4','IP_5','IP_6','IP_7','IP_8',
                  'IP_9','IP_10','IP_11','IP_12'
                 ]].copy()


# In[51]:


df_combined.head(30) #Check results


# ### 3. Categorize each respondent based on their responses (use level, feelings, concern types and change archtypes)

# #### Step 3.1: Create new columns for recording categories

# In[52]:


# Create four new columns to store calculated fields
df_combined = df_combined.assign(**{col: df_combined.get(col, '')
                  for col in ['use_level', 'feelings', 'concern_type', 'change_archtype']}
              )


# #### Step 3.2: Categorize respondents by their **use level**

# In[53]:


# Set static conditions and choices for determining use level of respondents
conditions_uselevel = [df_combined['combined_use_level'] == '1', df_combined['combined_use_level'] == '2', 
                       df_combined['combined_use_level'] == '3', df_combined['combined_use_level'] == '4']
                    
choices_uselevel = ['Emerging', 'Building', 'Managing', 'Maximizing'] 


# In[54]:


# Apply function to populate use level categories. Default is 'Dormant' category
df_combined['use_level'] = np.select(conditions_uselevel, choices_uselevel, default="Dormant")


# In[ ]:


df_combined.head(10) #Check results


# #### Step 3.3: Categorize respondents by their **feelings**

# In[55]:


# Set static conditions and choices for assigning feelings of respondents based on statement selected
conditions_feelings = [df_combined['combined_feel'] == '1', df_combined['combined_feel'] == '2', 
                       df_combined['combined_feel'] == '3']

choices_feelings = ['Positive', 'Neutral', 'Negative'] 


# In[56]:


# Apply function to populate feelings in the 'feelings' column. Default is an empty string
df_combined['feelings'] = np.select(conditions_feelings, choices_feelings, default='')


# In[ ]:


df_combined.head(10) # check


# #### Step 3.3: Categorize respondents by their **concern types**

# In[57]:


# Change the column type to integer to perform numeric operations
subset_cols = df_combined.loc[:, 'combined_feel_statements_1':'combined_feel_statements_20'].columns

for col in subset_cols:
    df_combined[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')


# In[58]:


df_combined.info() # Check results


# In[59]:


# Calculate means for each respondent's concern type based on the score selecetd in statement variables
df_combined['unconcerned_cat'] = df_combined[['combined_feel_statements_3', 'combined_feel_statements_6',
                                          'combined_feel_statements_9', 'combined_feel_statements_15',
                                          'combined_feel_statements_18']].mean(axis=1)
df_combined['self_cat'] = df_combined[['combined_feel_statements_4', 'combined_feel_statements_5',
                                   'combined_feel_statements_11', 'combined_feel_statements_16',
                                   'combined_feel_statements_20']].mean(axis=1)
df_combined['task_cat'] = df_combined[['combined_feel_statements_1', 'combined_feel_statements_7',
                                   'combined_feel_statements_12', 'combined_feel_statements_14',
                                   'combined_feel_statements_17']].mean(axis=1)
df_combined['impact_cat'] = df_combined[['combined_feel_statements_2', 'combined_feel_statements_8',
                                     'combined_feel_statements_10', 'combined_feel_statements_13',
                                     'combined_feel_statements_19']].mean(axis=1)


# In[60]:


# Store max. means in concern types column
df_combined['concern_type'] = df_combined[['unconcerned_cat', 'self_cat', 'task_cat', 'impact_cat']].max(axis=1)


# In[61]:


# Set static conditions and choices to assign category based on the max. mean for each respondent
conditions_concerntype = [
    (df_combined['concern_type'] == df_combined['unconcerned_cat']),
    (df_combined['concern_type'] == df_combined['self_cat']),
    (df_combined['concern_type'] == df_combined['task_cat']),
    (df_combined['concern_type'] == df_combined['impact_cat'])
]

choices_concerntype = ['Unconcerned', 'Self', 'Task', 'Impact']


# In[62]:


# Apply function. Default value is an empty string
df_combined['concern_type'] = np.select(conditions_concerntype, choices_concerntype, default='')


# In[64]:


df_combined.head(30) # Check results


# #### Step 3.4: Categorize respondents by their **quality of use** (IP data)

# In[65]:


# Create new columns to store IP score and categories
df_combined = df_combined.assign(**{col: df_combined.get(col, '')
                  for col in ['quality_use', 'IP_score']}
              )


# In[66]:


df_combined.columns.get_indexer(['IP_4', 'IP_6', 'IP_8', 'IP_10']) # get index of the column for next function


# In[67]:


# Change columns with IP scores to integer values
subset_cols2 = df_combined.loc[:, ['IP_4', 'IP_6', 'IP_8', 'IP_10']].columns

for col in subset_cols2:
    df_combined[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')


# In[70]:


df_combined.info() # Check results


# In[71]:


# Store mean of IP scores in a single column
df_combined['IP_score'] = df_combined.iloc[:,[53, 55, 57, 59]].mean(axis = 1)


# In[72]:


# Set static conditions and choices for assigning categories for quality of use of each respondent
conditions_quality = [
    (df_combined['IP_score'] >= 1) & (df_combined['IP_score'] <= 1.75),
    (df_combined['IP_score'] > 1.75) & (df_combined['IP_score'] <= 2.5),
    (df_combined['IP_score'] > 2.5) & (df_combined['IP_score'] <= 3.25),
    (df_combined['IP_score'] > 3.25) & (df_combined['IP_score'] <= 4)
]

choices_quality = ['No Evidence of Use', 'Novice', 'Experienced', 'Expert']


# In[74]:


# Apply function. Default is 'NA', as these respondents need to be explicitly identifiable in dashboards and next calculated field (next step).
df_combined['quality_use'] = np.select(conditions_quality, choices_quality, default='NA')


# In[76]:


df_combined.head(40) # Check results


# In[77]:


# Create a new IP dataframe
df_IP = df_combined[['IP_1','IP_2','IP_3','IP_4','IP_5','IP_6','IP_7','IP_8',
                  'IP_9','IP_10','IP_11','IP_12', 'quality_use', 'IP_score'
                 ]].copy()


# In[78]:


# Drop NaN rows to clean the data set. Not necessary.
df_IP.dropna(subset=['IP_1'], how = 'all', inplace=True)


# In[80]:


df_IP.head(20) # Check results


# In[82]:


# Create a new dataframe without IP variables by copying all columns from df_combined. Alternatively,
# we can drop the IP columns from existing 'df_combined' dataframe.
df_new = df_combined[
    ['StartDate', 'EndDate', 'Duration (in seconds)', 'Finished', 'RecordedDate', 'ResponseId', 'DistributionChannel', 'name_6', 'name_7', 'email', 'role', 'task', 
     'combined_curriculum_name', 'combined_use', 'combined_not_use', 'combined_use_level', 'combined_modifications', 'combined_modifications_type', 'combined_modification_example', 
     'combined_length_curriculum', 'combined_units', 'combined_units_open', 'combined_feel', 'combined_feel_explain', 'combined_current_role', 'combined_teaching_length', 'combined_school_length', 
     'combined_gender', 'combined_race_ethnicity', 'combined_opt_in', 'combined_feel_statements_1', 'combined_feel_statements_2', 'combined_feel_statements_3', 'combined_feel_statements_4', 
     'combined_feel_statements_5', 'combined_feel_statements_6', 'combined_feel_statements_7', 'combined_feel_statements_8', 'combined_feel_statements_9', 'combined_feel_statements_10', 
     'combined_feel_statements_11', 'combined_feel_statements_12', 'combined_feel_statements_13', 'combined_feel_statements_14', 'combined_feel_statements_15', 'combined_feel_statements_16', 
     'combined_feel_statements_17', 'combined_feel_statements_18', 'combined_feel_statements_19', 'combined_feel_statements_20', 
     'use_level', 'feelings', 'concern_type', 'change_archtype', 'unconcerned_cat', 'self_cat', 'task_cat', 'impact_cat']].copy()


# In[83]:


# Get ready to merge (left join)

# Create a unique identifier for merge. We used email in test version; in actuality, each respondent will have a unique identifier code.
df_IP = df_IP.rename(columns={"IP_3":"email"}) 


# In[63]:


# Set 'email' as index
df_try.set_index('email', inplace=True)


# In[86]:


# Merge using email as the index and left join i.e., all values from df_new, matching values from df_IP
df_merged = pd.merge(df_new, df_IP, on = 'email', how='left') #all from df_new, only matching from df_IP ('left' join)


# In[88]:


df_merged.head(20) # Check results


# #### Step 3.4: Categorize respondents by their **change archtypes**

# In[104]:


# Set static conditions and choices for assigning change archtype categories

conditions_archtype = [
    ((df_merged['use_level'] == 'Managing') | (df_merged['use_level'] == 'Maximizing')) & 
    (df_merged['feelings'] == 'Positive') &
    ((df_merged['concern_type'] == 'Task') | (df_merged['concern_type'] == 'Impact')) &
    ((df_merged['quality_use'] == 'Expert') | pd.isna(df_merged['quality_use'])), #Champions
    
    ((df_merged['use_level'] == 'Managing') | (df_merged['use_level'] == 'Maximizing') | (df_merged['use_level'] == 'Building')) &
    (df_merged['feelings'] == 'Positive') & 
    ((df_merged['concern_type'] == 'Unconcerned') | (df_merged['concern_type'] == 'Self') | (df_merged['concern_type'] == 'Task') |
     (df_merged['concern_type'] == 'Impact')) &
    ((df_merged['quality_use'] == 'Expert') | (df_merged['quality_use'] == 'Experienced') | pd.isna(df_merged['quality_use'])), #Supporters
    
    ((df_merged['use_level'] == 'Managing') | (df_merged['use_level'] == 'Maximizing')) & 
    (df_merged['feelings'] == 'Neutral') & 
    ((df_merged['concern_type'] == 'Unconcerned') | (df_merged['concern_type'] == 'Self') | (df_merged['concern_type'] == 'Task') |
     (df_merged['concern_type'] == 'Impact')) &
    ((df_merged['quality_use'] == 'Expert') | (df_merged['quality_use'] == 'Experienced') | pd.isna(df_merged['quality_use'])), #Adopters
    
    ((df_merged['use_level'] == 'Managing') | (df_merged['use_level'] == 'Maximizing')) &
    ((df_merged['feelings'] == 'Positive') | (df_merged['feelings'] == 'Neutral') | (df_merged['feelings'] == 'Negative')) & 
    ((df_merged['concern_type'] == 'Unconcerned') | (df_merged['concern_type'] == 'Self') | (df_merged['concern_type'] == 'Task') |
     (df_merged['concern_type'] == 'Impact')) &
    ((df_merged['quality_use'] == 'No Evidence of Use') | (df_merged['quality_use'] == 'Novice') | pd.isna(df_merged['quality_use'])), #Navigators
    
    ((df_merged['use_level'] == 'Emerging') | (df_merged['use_level'] == 'Building')) & 
    ((df_merged['feelings'] == 'Neutral') | (df_merged['feelings'] == 'Positive')) & 
    ((df_merged['concern_type'] == 'Unconcerned') | (df_merged['concern_type'] == 'Self')| (df_merged['concern_type'] == 'Task') | 
     (df_merged['concern_type'] == 'Impact')) &
    ((df_merged['quality_use'] == 'No Evidence of Use') | (df_merged['quality_use'] == 'Novice') | pd.isna(df_merged['quality_use'])), #Rookies
    
    (df_merged['use_level'] == 'Dormant') &
    ((df_merged['feelings'] == 'Neutral') | (df_merged['feelings'] == 'Positive')) & 
    ((df_merged['concern_type'] == 'Unconcerned') | (df_merged['concern_type'] == 'Self') | (df_merged['concern_type'] == 'Task') |
     (df_merged['concern_type'] == 'Impact')) &
    ((df_merged['quality_use'] == 'No Evidence of Use') | (df_merged['quality_use'] == 'Novice') | pd.isna(df_merged['quality_use'])), #Stragglers
    
    ((df_merged['use_level'] == 'Dormant') | (df_merged['use_level'] == 'Emerging') | (df_merged['use_level'] == 'Building')) &
    (df_merged['feelings'] == 'Negative') & 
    ((df_merged['concern_type'] == 'Unconcerned') | (df_merged['concern_type'] == 'Self') | (df_merged['concern_type'] == 'Task') | 
     (df_merged['concern_type'] == 'Impact')) &
    ((df_merged['quality_use'] == 'No Evidence of Use') | (df_merged['quality_use'] == 'Novice') | pd.isna(df_merged['quality_use'])), #Skeptics
    
    ((df_merged['use_level'] == 'Managing') | (df_merged['use_level'] == 'Maximizing')) & 
     (df_merged['feelings'] == 'Negative') & 
    ((df_merged['concern_type'] == 'Unconcerned') | (df_merged['concern_type'] == 'Self') | (df_merged['concern_type'] == 'Task') |
     (df_merged['concern_type'] == 'Impact')) &
    ((df_merged['quality_use'] == 'Expert') | (df_merged['quality_use'] == 'Experienced') | pd.isna(df_merged['quality_use'])) #Challengers
   ]
# Explicitly specified 'NA' values to be considered in conditions.

choices_archtype = ['Category1', 'Category2','Category3','Category4','Category5','Category6','Category7','Category7']


# In[92]:


# Apply function. Default is 'Other' (as requested for dashboards)

df_merged['change_archtype'] = np.select(conditions_archtype, choices_archtype, default='Other')


# In[93]:


# Data cleaning measure -  drop first two rows with label names and qualtrics question ID.
df_merged = df_merged.drop([0, 1],
        axis=0) 


# In[106]:


df_merged.head(30) # Check results


# ### 4. Load data to BigQuery (via Cloud Function/Scheduler)

# In[97]:


# Get the current time (to be stored in file name for version history check)

today = datetime.datetime.now().strftime('%Y-%m-%d')
today


# In[98]:


# Define variables for Cloud Functions

bucket_name = 'YOUR BUCKET NAME'
project_name = 'YOUR PROJECT NAME'
dataset_name = 'YOUR DATASET NAME'
table_name = 'YOUR TABLE NAME'


# In[99]:


# Convert the DataFrame to a CSV string
csv_string = df_merged.to_csv(index=False)


# In[103]:


# Upload CSV file to Cloud Storage

client = storage.Client()
bigquery_client = bigquery.Client()
bucket = client.get_bucket(bucket_name)
blob = bucket.blob(f'FOLDER_NAME/DATASET_NAME_{today}.csv')
blob.upload_from_string(csv_string)


# In[ ]:


# Define function to check for table or create a table (if it doesn't exist) in GCP
def create_dataset_if_not_exists(client, dataset_name):
    dataset_ref = client.dataset(dataset_name) #GCP dataset
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f'Dataset "{dataset.dataset_id}" already exists.')
    except Exception as e:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        dataset = client.create_dataset(dataset)
        print(f'Dataset "{dataset.dataset_id}" created.')
    return dataset


# In[ ]:


# Run function
dataset = create_dataset_if_not_exists(bigquery_client, dataset_name)


# In[ ]:


# Load to BQ
dataset_ref = bigquery_client.dataset(dataset.dataset_id) #BQ dataset
table_ref = dataset_ref.table(table_name)

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    source_format=bigquery.SourceFormat.CSV,
    write_disposition='WRITE_TRUNCATE' 
    
job_config.allow_quoted_newlines = True

uri = f"gs://{bucket_name}/{blob.name}"
load_job = bigquery_client.load_table_from_uri(uri, table_ref, job_config=job_config)
load_job.result()


# In[ ]:


# Check status of table
destination_table = bigquery_client.get_table(table_ref) 
print("Loaded {} rows.".format(destination_table.num_rows))


# ### 4. Cloud Function Code

# In[ ]:


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    print(pubsub_message)
    generate_data()


def generate_data():
    pass


if __name__ == "__main__":
    # This block will only be executed when the function is run locally.
    hello_pubsub("data", "context")

