import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from mpl_toolkits.basemap import Basemap
import folium
import folium.plugins as plugins
import warnings
warnings.filterwarnings('ignore')

pd.options.display.max_rows =100

plt.style.use('fivethirtyeight')
plt.style.use('bmh')

import plotly
import plotly.offline as py
import plotly.tools as tls
import plotly.graph_objs as go
import plotly.tools as tls
import plotly.figure_factory as fig_fact
plotly.tools.set_config_file(world_readable=True, sharing='public')

from matplotlib.colors import rgb2hex, Normalize
from matplotlib.patches import Polygon
from matplotlib.colorbar import ColorbarBase

from google.cloud import bigquery
from bq_helper import BigQueryHelper

hist_aq = BigQueryHelper(active_project='cs519-zhouxiangmeng',dataset_name='cs540')
hist_aq.list_tables()



#hist_aq = BigQueryHelper(active_project='bigquery-public-data',dataset_name='epa_historical_air_quality')


# Helper object


#bq_assistant = BigQueryHelper("bigquery-public-data", "epa_historical_air_quality")

#bigquery-public-data.epa_historical_air_quality

states = {'AL': 'Alabama',
'AK': 'Alaska',
'AZ':'Arizona',
'AR':'Arkansas',
'CA':'California',
'CO':'Colorado',
'CT':'Connecticut',
'DE':'Delaware',
'FL':'Florida',
'GA':'Georgia',
'HI':'Hawaii',
'ID':'Idaho',
'IL':'Illinois',
'IN':'Indiana',
'IA':'Iowa',
'KS':'Kansas',
'KY':'Kentucky',
'LA':'Louisiana',
'ME':'Maine',
'MD':'Maryland',
'MA':'Massachusetts',
'MI':'Michigan',
'MN':'Minnesota',
'MS':'Mississippi',
'MO':'Missouri',
'MT':'Montana',
'NE':'Nebraska',
'NV':'Nevada',
'NH':'New Hampshire',
'NJ':'New Jersey',
'NM':'New Mexico',
'NY':'New York',
'NC':'North Carolina',
'ND':'North Dakota',
'OH':'Ohio',
'OK':'Oklahoma',
'OR':'Oregon',
'PA':'Pennsylvania',
'RI':'Rhode Island',
'SC':'South Carolina',
'SD':'South Dakota',
'TN':'Tennessee',
'TX':'Texas',
'UT':'Utah',
'VT':'Vermont',
'VA':'Virginia',
'WA':'Washington',
'WV':'West Virginia',
'WI':'Wisconsin',
'WY':'Wyoming'}

################ avg aqi for year ########################
query = """SELECT AVG(AQI) as `Average`,
        EXTRACT (YEAR FROM Date)as `year`
        FROM `bigquery-public-data.epa_historical_air_quality.air_quality_annual_summary`
        GROUP BY year
        ORDER BY year
        """
p1 = hist_aq.query_to_pandas_safe(query)

plt.figure(figsize=(14,4))
sns.barplot(p1['year'], p1['Average'], palette=sns.color_palette('hot',len(p1)))
plt.xticks(rotation=45);
plt.show()


############# avg aqi for state #################
query_aqi = """SELECT state_name, avg(avg_aqi_1) AS avg_aqi
        FROM (SELECT State_Name AS state_name, CBSA_Name
        FROM `cs519-zhouxiangmeng.cs540.co`
        GROUP BY CBSA_Name, state_name) a
        LEFT JOIN
        (SELECT avg(AQI) AS avg_aqi_1,CBSA
        FROM `bigquery-public-data.epa_historical_air_quality.aqi`
        GROUP BY CBSA) b
        ON a.CBSA_Name = b.CBSA
        GROUP BY state_name
        ORDER BY avg_aqi
        """

df_aqi_states = hist_aq.query_to_pandas_safe(query_aqi)

plt.subplots(figsize=(15,7))
sns.barplot(x='avg_aqi',y='state_name',data=df_aqi_states,palette='RdYlGn_r',edgecolor=sns.color_palette('dark',7))
plt.ylabel('Air Quality Index', fontsize=20)
plt.xticks(rotation=90)
plt.xlabel('Year', fontsize=20)
plt.title('Average AQI in different states', fontsize=24)
plt.show()

############# avg co for state #################
query_co = """SELECT state_name, avg(Arithmetic_Mean) AS avg_co
        FROM `bigquery-public-data.epa_historical_air_quality.co`
        GROUP BY state_name
        ORDER BY avg_co
        """

df_co_states = hist_aq.query_to_pandas_safe(query_co)

plt.subplots(figsize=(15,7))
sns.barplot(x='avg_co',y='state_name',data=df_co_states,palette='RdYlGn_r',edgecolor=sns.color_palette('dark',7))
plt.ylabel('Air Quality Index', fontsize=20)
plt.xticks(rotation=90)
plt.xlabel('Year', fontsize=20)
plt.title('Average CO in different states', fontsize=24)
plt.show()


################ avg co,so2,no2,ozone,pm25 for state #################
query_all = """SELECT g.State_Name AS state_name, avg_no2, avg_co, avg_ozone, avg_pm25, avg_so2
                FROM (SELECT e.State_Name AS State_Name, avg_no2, avg_co, avg_ozone, avg_pm25
                      FROM (SELECT c.State_Name AS State_Name, avg_no2, avg_co, avg_ozone
                            FROM (SELECT a.State_Name AS State_Name, avg_no2, avg_co
                                  FROM (SELECT State_Name, avg(Arithmetic_Mean) AS avg_no2
                                        FROM `bigquery-public-data.epa_historical_air_quality.no2`
                                        GROUP BY State_Name) a
                                        LEFT JOIN
                                       (SELECT State_Name, avg(Arithmetic_Mean) AS avg_co
                                        FROM `bigquery-public-data.epa_historical_air_quality.co`
                                        GROUP BY State_Name
                                        ORDER BY State_Name) b
                                        ON a.State_Name = b.State_Name) c
                                  LEFT JOIN
                                 (SELECT State_Name, avg(Arithmetic_Mean) AS avg_ozone
                                  FROM `bigquery-public-data.epa_historical_air_quality.ozone`
                                  GROUP BY State_Name) d
                                  ON c.State_Name = d.State_Name) e
                            LEFT JOIN
                           (SELECT State_Name, avg(Arithmetic_Mean) AS avg_pm25
                            FROM `bigquery-public-data.epa_historical_air_quality.pm25`
                            GROUP BY State_Name) f
                            ON e.State_Name = f.State_Name) g
                      LEFT JOIN
                     (SELECT State_Name, avg(Arithmetic_Mean) AS avg_so2
                      FROM `bigquery-public-data.epa_historical_air_quality.so2`
                      GROUP BY State_Name) h
                      ON g.State_Name = h.State_Name
            """
df_all_states = hist_aq.query_to_pandas_safe(query_all)

state_list = df_all_states['state_name']
avg_no2 = df_all_states['avg_no2']
avg_co = df_all_states['avg_co']
avg_ozone = df_all_states['avg_ozone']
avg_pm25 = df_all_states['avg_pm25']
avg_so2 = df_all_states['avg_so2']

x = np.arange(len(state_list))
width = 0.5
width_1 = 0.2
fig, ax = plt.subplots()
ax.bar(x, avg_no2, width, alpha = 0.8, label = 'avg_no2')
ax.bar(x, avg_co, width, alpha = 0.8, label = 'avg_co')
ax.bar(x, avg_ozone, width, alpha = 0.8, label = 'avg_ozone')
ax.bar(x, avg_pm25, width, alpha = 0.8, label = 'avg_pm25')
ax.bar(x, avg_so2, width, alpha = 0.8, label = 'avg_so2')

ax.set_xticks(x+ width_1)
ax.set_xticklabels(state_list)
plt.xticks(rotation=90)
plt.legend()
plt.show()


################## map of AVG PM25, CO, NO2, SO2, OZONE ################

QUERY_1 = """
    SELECT a.State_Name, co_avg, no_avg
    FROM (SELECT State_Name, avg(Arithmetic_Mean) AS co_avg
          FROM `bigquery-public-data.epa_historical_air_quality.cs540.co`
          GROUP BY State_Name) a
          LEFT JOIN 
         (SELECT State_Name,avg(Arithmetic_Mean) AS no_avg
          FROM `bigquery-public-data.epa_historical_air_quality.cs540.no2` 
          GROUP BY State_Name
          ORDER BY State_Name) b
          ON a.State_Name = b.State_Name
        """

df_states_gas = hist_aq.query_to_pandas_safe(QUERY_1)

QUERY_2 = """
    SELECT State_Name,avg(Arithmetic_Mean) AS ozone_avg
    FROM `bigquery-public-data.epa_historical_air_quality.ozone` 
    GROUP BY State_Name
    ORDER BY State_Name
        """
df_states_gas_ozone = hist_aq.query_to_pandas_safe(QUERY_2)

QUERY_3 = """
    SELECT State_Name,avg(Arithmetic_Mean) AS so2_avg
    FROM `bigquery-public-data.epa_historical_air_quality.so2` 
    GROUP BY State_Name
    ORDER BY State_Name
        """
df_states_gas_so2 = hist_aq.query_to_pandas_safe(QUERY_3)

QUERY_4 = """
   SELECT State_Name, avg(Arithmetic_Mean) AS pm25_avg
    FROM `bigquery-public-data.epa_historical_air_quality.pm25`
    GROUP BY State_Name
    ORDER BY State_Name
        """
df_states_gas_aqi = hist_aq.query_to_pandas_safe(QUERY_4)

df_states_gas['pm25_avg'] = df_states_gas['State_Name'].map(df_states_gas_aqi.set_index('State_Name')['pm25_avg'])
df_states_gas['ozone_avg'] = df_states_gas['State_Name'].map(df_states_gas_ozone.set_index('State_Name')['ozone_avg'])
df_states_gas['so2_avg'] = df_states_gas['State_Name'].map(df_states_gas_so2.set_index('State_Name')['so2_avg'])


df_states = pd.DataFrame.from_dict(states, orient='index').reset_index()
df_states.columns = ['Code', 'Code_Name']
df_states_gas['State_Code'] = df_states_gas['State_Name'].map(df_states.set_index('Code_Name')['Code'])

scl = [[0.0, 'rgb(242,240,247)'], [0.2, 'rgb(218,218,235)'], [0.4, 'rgb(188,189,220)'], [0.6, 'rgb(158,154,200)'],
       [0.8, 'rgb(117,107,177)'], [1.0, 'rgb(84,39,143)']]

data = [dict(
    type='choropleth',
    colorscale=scl,
    autocolorscale=False,
    locations=df_states_gas['State_Code'],
    z=df_states_gas['pm25_avg'].astype(float),
    locationmode='USA-states',
    text =  'Average NO: ' + df_states_gas['no_avg'].astype(str) + '<br>' + 'Average CO: ' + df_states_gas['co_avg'].astype(str) + '<br>' + 'Average SO2: ' + df_states_gas['so2_avg'].astype(str) + '<br>' + 'Average OZONE: ' + df_states_gas['ozone_avg'].astype(str),
    marker=dict(
        line=dict(
            color='rgb(255,255,255)',
            width=2
        )),
    colorbar=dict(
        title="AVG PM2.5 In USA")
)]

layout = dict(
    title='The average air quality index of some dangerous element in different US states',
    geo=dict(
        scope='usa',
        projection=dict(type='albers usa'),
        showlakes=True,
        lakecolor='rgb(255, 255, 255)'),
)

fig = dict(data=data, layout=layout)

py.plot(fig, filename='map')

