# -*- coding: utf-8 -*-
import os
from io import StringIO
import time
from datetime import datetime as dt, timedelta, date
import dash
import dash_table
import dash_html_components as html
import dash_core_components as dcc
import dash_daq as daq
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import numpy as np
from app import app

from theme import dark_theme, light_theme, colors_bg, table_header_dark

def get_hour():
    time_h = (time.strftime("%d/%m/%Y %H:%M:%S"))
    return time_h

def get_day():
    time_d = (time.strftime("%d-%m-%Y"))
    return time_d

#Return client for Azure DatalakeService    
def get_client():
    service_client = DataLakeServiceClient(account_url="https://xxxxxx.dfs.core.windows.net",
    credential= "*******")
    file_system_client = service_client.get_file_system_client(file_system="xxxx")
    directory_client = file_system_client.get_directory_client("users/mve/")
    return directory_client

client = get_client()

#Read files on Datalake and return pandas df
def read_cs_file(client):
    file_client = client.get_file_client("cs_circulation.csv")
    bytes_data= file_client.read_file()
    s=str(bytes_data,'utf-8')
    data = StringIO(s) 
    df=pd.read_csv(data, sep =";")
    return df

# query oracle and reformat columns with pandas
def display_circulation(client):
    file_client = client.get_file_client("rt_circulation.csv")
    bytes_data= file_client.read_file()
    s=str(bytes_data,'utf-8')
    data = StringIO(s) 
    df=pd.read_csv(data, sep =";")
    
    df_cs = read_cs_file(client)
    df = df[['CODSEG', 'LIBTYP', 'NUMTYP', 'CODSILLON', 'CODLIGNE', 'CLOSING AF', 'CLOSING CO', 'DEPART','ARRIVEE', 'MAD AF', 'MAD CO', 'TX_REMPL_AF', 'TX_REMPL_CO', 'DEP_REE', 'ARR_REE', 'RETDEP',
    'RETARR', 'DATE DEB DECHGT', 'DATE FIN DECHGT']]

    # Format date for nicer display
    df['DATE_INI'] = df["DEPART"].apply(lambda x: dt.strptime(x, "%d-%m-%Y %H:%M:%S"))
    df['DATE'] = df["DEPART"].apply(lambda x: dt.strptime(x, "%d-%m-%Y %H:%M:%S"))
    df['DATE'] = df["DATE"].apply(lambda x: dt.strftime(x, "%d/%m/%Y"))
    df['CLOSING AF'] = df["CLOSING AF"].apply(lambda x: dt.strptime(x, "%d-%m-%Y %H:%M:%S"))
    df['CLOSING CO'] = df["CLOSING CO"].apply(lambda x: dt.strptime(x, "%d-%m-%Y %H:%M:%S"))
    df['DEPART'] = df["DEPART"].apply(lambda x: dt.strptime(x, "%d-%m-%Y %H:%M:%S"))

    df['DEP_REE'] = df["DEP_REE"].apply(lambda x: '-' if pd.isnull(x) else dt.strptime(x, "%d-%m-%Y %H:%M:%S"))
    df['ARR_REE'] = df["ARR_REE"].apply(lambda x: '-' if pd.isnull(x) else dt.strptime(x, "%d-%m-%Y %H:%M:%S"))

    df['ARRIVEE'] = df["ARRIVEE"].apply(lambda x: dt.strptime(x, "%d-%m-%Y %H:%M:%S"))
    df['DAY_CIRC'] = df['ARRIVEE'].dt.day -  df['DEPART'].dt.day
 
    df['MAD AF'] = df["MAD AF"].apply(lambda x: dt.strptime(x, "%d-%m-%Y %H:%M:%S"))
    df['MAD CO'] = df["MAD CO"].apply(lambda x: dt.strptime(x, "%d-%m-%Y %H:%M:%S")) 
    df['TX_REMPL_AF'] = df['TX_REMPL_AF'].astype(str) + '%'
    df['TX_REMPL_CO'] = df["TX_REMPL_CO"].astype(str) + '%'
    df = df.sort_values(by='DEPART', ascending=True)

    df['CLOSING AF'] = df["CLOSING AF"].apply(lambda x: dt.strftime(x, "%H:%M"))
    df['CLOSING CO'] = df["CLOSING CO"].apply(lambda x: dt.strftime(x, "%H:%M"))
    df['DEPART'] = df["DEPART"].apply(lambda x: dt.strftime(x, "%H:%M"))
    df['DEP_REE'] = df["DEP_REE"].apply(lambda x: '-' if x == '-' else dt.strftime(x, "%H:%M"))
    df['ARR_REE'] = df["ARR_REE"].apply(lambda x: '-' if x == '-' else dt.strftime(x, "%H:%M"))
    df['ARRIVEE'] = df["ARRIVEE"].apply(lambda x: dt.strftime(x, "%H:%M")) 

    df['MAD AF'] = df["MAD AF"].apply(lambda x: dt.strftime(x, "%H:%M"))
    df['MAD CO'] = df["MAD CO"].apply(lambda x: dt.strftime(x, "%H:%M"))
    df['RETARD_TR'] = '-'

    #df['RETDEP'] =  df['RETDEP'] .apply(lambda x: 0  if pd.isnull(x) else int(x))
    df = df.fillna('-')

    # CONDITION 1 Statut Livraison
    m1 = (df.NUMTYP == 5) & (df['DATE DEB DECHGT']!= '-') &(df['DATE FIN DECHGT'] =='-')
    m2 = (df.NUMTYP == 5) & (df['DATE DEB DECHGT'] != '-') & (df['DATE FIN DECHGT'] !='-')
    m3 = (df.NUMTYP == 5) & (df['DATE DEB DECHGT'] == '-') & (df['DATE FIN DECHGT'] == '-')
  
    df['MAD REV AF'] = '-'
    df['MAD REV CO'] = '-'
    df['STATUT_LIV'] = np.select([m1, m2, m3], ['En Livraison', 'Livré', 'En Attente Livraison'], 
                           default='-') 

   # CONDITION 2 Statut Livraison + Circulation
    m4 = (df['STATUT_LIV'] != '-')
    m5 = (df['STATUT_LIV'] == '-')
    
    df['LIBTYP'] =   np.select([m4, m5], [df['STATUT_LIV'], df['LIBTYP']],
                           default=df['LIBTYP'])                

   # CONDITION 3 Modification Heure Depart
    m6 = (df['DEP_REE'] != '-') 
    m7 = (df['DEP_REE'] == '-')
    
    df['DEPART'] =   np.select([m6, m7], [df['DEP_REE'], df['DEPART']],
                           default=df['DEPART'])     

   # CONDITION 4 Modification Heure arrivee
    m6 = (df['ARR_REE'] != '-') &  (df['DAY_CIRC'] == 0)
    m7 = (df['ARR_REE'] == '-') &  (df['DAY_CIRC'] == 0)
    
    m8 = (df['ARR_REE'] == '-') &  (df['DAY_CIRC'] != 0)
    m9 = (df['ARR_REE'] != '-') &  (df['DAY_CIRC'] != 0)

    df['ARRIVEE'] =  np.select([m6, m7, m8, m9], 
                        [df['ARR_REE'], 
                        df['ARRIVEE'],
                        (df['ARRIVEE'].astype(str) +  ' +' + df['DAY_CIRC'].astype(str) + 'j'),
                        (df['ARR_REE'].astype(str) +  ' +' + df['DAY_CIRC'].astype(str) + 'j')],
                        default=df['ARRIVEE'])     

     # Condition 5
    m10 = (df['DAY_CIRC'] == 0)
    m11 = (df['DAY_CIRC'] != 0)

    df['MAD AF'] =  np.select([m10, m11], 
                        [df['MAD AF'], 
                        (df['MAD AF'].astype(str) +  ' +' + df['DAY_CIRC'].astype(str) + 'j')],
                        default=df['MAD AF'])   

    df['MAD CO'] =  np.select([m10, m11], 
                        [df['MAD CO'], 
                        (df['MAD CO'].astype(str) +  ' +' + df['DAY_CIRC'].astype(str) + 'j')],
                        default=df['MAD CO'])     
    df_cs = df_cs.rename(columns={'Train': 'CODSILLON', 'Date': 'DATE_INI'})
    df_cs['DATE_INI'] = pd.to_datetime(df_cs.DATE_INI, format='%Y-%m-%d %H:%M:%S')
  
    dff = pd.merge(df, df_cs[['CODSILLON', 'Circulation', 'DATE_INI']], how='left', on = ['CODSILLON', 'DATE_INI'])

    dff = dff[['DATE', 'CODSEG', 'LIBTYP', 'NUMTYP', 'Circulation', 'CODSILLON', 'CODLIGNE', 
    'CLOSING AF', 'CLOSING CO', 
    'DEPART','ARRIVEE', 
    'MAD AF', 'MAD CO', 
    'TX_REMPL_AF', 'TX_REMPL_CO', 
    'DEP_REE', 'ARR_REE', 
    'RETDEP', 'RETARR', 'DATE DEB DECHGT', 'DATE FIN DECHGT', 'MAD REV AF', 'MAD REV CO', 'STATUT_LIV', 'DATE_INI']]

    dff.columns = ['Date', 'Train', 'Statut', 'NumStatut',  'Retard Actuel', 'Sillon', 'Ligne', 
    'Closing AF', 'Closing CO',
    'Depart', 'Arrivee',
    'MAD AF', 'MAD CO',
    'Taux Remplissage AF', 'Taux Remplissage CO', 
    'Depart Reel', 'Arrivee Reelle',
    'Retard Depart', 'Retard Arrivee',  'Debut Dechgt', 'Fin Dechgt',  'New MAD AF', 'New MAD CO', 'Statut Livraison', 'DATE_INI']
    
    dff = dff[((dff['Statut'].str.contains('En circulation')) | 
    (dff['Statut'].str.contains('En Attente Livraison')) | 
    (dff['Statut'].str.contains('En Livraison'))) |  
    ((dff['DATE_INI'] > (date.today() - timedelta(hours=12)).strftime('%Y-%m-%d %H:%M:%S')) & (dff['Date'] < (date.today() + timedelta(days=12)).strftime('%Y-%m-%d %H:%M:%S')))]
    #dff = dff[~dff['Statut Livraison'].str.contains('Livré')]
    dff = dff.drop_duplicates(subset=['Date', 'Train'], keep='first')
    #Delete file after query

    del client
    del file_client
    del bytes_data
    del data
    return dff


df = display_circulation(client)

#LAYOUT DASH
app.layout = html.Div(style={'backgroundColor': colors_bg['background']}, children=[
    html.Div([
        html.Div([    
        daq.ToggleSwitch(
            id='toggle-theme',
            label=['Light', 'Dark'],
            value=True
        )], style={'width': '20%', 'display': 'inline-block'}),
        html.Div([  
        dcc.Interval(id='interval2', interval= 120 * 1000, n_intervals=0),
        html.P(id='time_file', children=''),
        ], style={'width': '80%', 'display': 'inline-block'}),
    ], style={'marginBottom': 2, 'marginTop': 0, 'color': 'white', 'fontSize': 14}),
    

html.Div([
    html.Div([
        dcc.Interval(id='interval1', interval= 120 * 1000, n_intervals=0),
        dash_table.DataTable( 
        id='table',
        style_data={'whiteSpace': 'normal', 'border': '0.1px dashed black'},
        style_header= table_header_dark,
        style_cell={'minWidth': '20px', 'maxWidth': '200px',
                'whiteSpace': 'normal'
                    },
        css=[{
            'selector': '.dash-cell div.dash-cell-value',
            'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
        }],
        columns=[{"name": i, "id": i, "deletable": False, "selectable": True, 'hideable' : True} for i in df.columns],

        data=df.to_dict("rows"),
        editable=True,
        sort_action="native",
        style_data_conditional=dark_theme,
        hidden_columns=['Sillon', 'NumStatut', 'Debut Dechgt', 'Fin Dechgt',  'Retard Actuel',
        'Statut Livraison', 'Depart Reel', 'Retard Depart', 'Retard Arrivee', 'Arrivee Reelle'],
        style_as_list_view=False,
        )], className="twelve columns"),
    ], className="row")
])

# Write Update Time 
@app.callback(dash.dependencies.Output('time_file', 'children'),
    [dash.dependencies.Input('interval2', 'n_intervals')])
def update_interval(n):
    client = get_client()
    try:
        file_client = client.get_file_client("rt_circulation.csv")
    except Exception as e:
        print(e)
    meta_data= file_client.get_file_properties()
    print(meta_data)
    date_f = (meta_data['last_modified']).time()
    print(date_f)
    return "Last Update at " + str(date_f)

#Call data of with interval 2min
@app.callback(dash.dependencies.Output('table', 'data'),
    [dash.dependencies.Input('interval1', 'n_intervals')],
    [dash.dependencies.State('table', 'data'),
    dash.dependencies.State('table', 'columns')])
def update_circulation(n1, row, col):
    df_new = display_circulation(client)
    row2 = df_new.to_dict('records')
    return row2

#Callback to change theme for table rows with
@app.callback(dash.dependencies.Output('table', 'style_header'),
    [dash.dependencies.Input('toggle-theme', 'value')])
    
def update_header(val):
    if val == True:
        style_header = {
                    'backgroundColor': 'rgb(30, 30, 30)',
                    'color': 'white',
                    'fontWeight': 'bold',
                    'border': '0px solid black'
                    }
    else:
        style_header =  {
                    'backgroundColor': 'white',
                    'color': 'black',
                    'fontWeight': 'bold',
                    'border': '0px solid black'
                    }      
    return style_header

@app.callback(dash.dependencies.Output('table', 'style_data_conditional'),
    [dash.dependencies.Input('toggle-theme', 'value')])
def update_row_theme(val):
    if val == True:
        theme = dark_theme
    else:
        theme = light_theme
    return theme


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=80)

