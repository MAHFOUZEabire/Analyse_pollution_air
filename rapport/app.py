import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas 
import postgreSQL_functions
import os
import csv
import psycopg2
import matplotlib.pyplot as plt
import plotly.graph_objects as go

#connecter au base de donnees:
ma_base_donnees = "pollution_BD"
utilisateur = "postgres"
mot_passe = os.environ.get('pg_psw')

conn = postgreSQL_functions.ouvrir_connection(ma_base_donnees, utilisateur, mot_passe)
##############
def requete_fonction(id_polluant,condition_date_niveau):
    requete=f"SELECT station.code_station,nom_station,nom_commun,nom_departement,typologie,niveau,date,etat FROM mesure_journallier LEFT JOIN station ON mesure_journallier.code_station=station.code_station WHERE id_polluant={id_polluant} AND {condition_date_niveau};"
    return(requete)
###########


external_stylesheets = ['style.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
##############
def generate_table(dataframe, max_rows):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])
########################
# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
#### Verfier le depassement de seuil recommander PM10 au cours des deux dernières années
filtre_seuils_recommander_PM10=requete_fonction(5,'niveau>=50')
depassement_seuils_recommanderPM10 = pandas.read_sql_query(filtre_seuils_recommander_PM10, conn)
x = depassement_seuils_recommanderPM10['nom_station']
y = depassement_seuils_recommanderPM10['niveau']
text=depassement_seuils_recommanderPM10['date']
max_rows=len(depassement_seuils_recommanderPM10)

def bar_plot(x,y,text,seuil_recommande):
    fig = go.Figure(data=[go.Bar(x=x, y=y,hovertext=text)])                                 
    fig.update_layout(
        title={
            'text': "Depassement PM10 au cours de deux dernier annees",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
       xaxis_title="Nom de station",
       yaxis_title="Niveau de polluant")
    fig.update_xaxes(tickangle=-45)
    fig.add_shape(type='line',
              x0=-0.5,
              y0=seuil_recommande,
              x1=15,
              y1=seuil_recommande,
              line=dict(color='Red'))
    fig.add_annotation(x=2, y=seuil_recommande,
            text="Seuil recommande",
            showarrow=True,
            arrowhead=1,
            font=dict(
            size=16,
            color="red"
            ),
            arrowcolor="red",
            ax=20,ay=-60)
    return(fig)

fig=bar_plot(x,y,text,50)

#############
station_donnes_csv=pandas.read_csv('data\station_nom.csv',sep=",")
list_nom_station=station_donnes_csv.drop_duplicates(["code_station"])
list_nom_station=list_nom_station["nom_station"]
###############
# fonction pour visualiser ....
def fig_plot(dataframe,title,x_annontation,xline0,xline1):
    fig=px.line(dataframe, x="date", y="niveau", color="nom_station",line_group="nom_station",
            hover_name="nom_station",title=title)
    fig.update_xaxes(title_text='Date')
    fig.update_yaxes(title_text='Niveau de polluant',range=[0,70])
    fig.add_annotation(x=pandas.Timestamp(x_annontation),y=50,
            text="Seuil recommande",
            showarrow=True,
            arrowhead=1,
            font=dict(
            size=16,
            color="red"
            ),
            arrowcolor="red",
            ax=20,ay=-60)
    fig.add_shape(type='line',
              x0=xline0,
              y0=50,
              x1=xline1,
              y1=50,
              line=dict(color='Red'))
    
    return(fig)
#######################    


qualite_air_PM10_2019=requete_fonction(5,"date>='2019-01-01' AND date<='2019-12-30'")
qualite_airPM10_2019 = pandas.read_sql_query(qualite_air_PM10_2019, conn)
fig2=fig_plot(qualite_airPM10_2019,"Qualite de l'air au niveau de PM10 en 2019",'2019-01-30','2019-01-01','2019-02-01')

qualite_air_PM10_2020=requete_fonction(5,"date>='2020-01-01' AND date<='2020-12-30'")
qualite_airPM10_2020 = pandas.read_sql_query(qualite_air_PM10_2020, conn)
fig3 = fig_plot(qualite_airPM10_2020,"Qualite de l'air au niveau de PM10 en 2020",'2020-01-30','2020-01-01','2020-02-01')
fig3.add_shape(type='line',
               x0=pandas.Timestamp('2020-03-17'),
               y0=0,
               x1=pandas.Timestamp('2020-03-17'),
               y1=70,
               line=dict(color='Red'))
fig3.add_shape(type='line',
               x0=pandas.Timestamp('2020-05-11'),
               y0=0,
               x1=pandas.Timestamp('2020-05-11'),
               y1=70,
               line=dict(color='Red'))
fig3.add_annotation(x=pandas.Timestamp('2020-03-30'),y=68,
            text="Confinement",
            showarrow=False,
            font=dict(
            size=16,
            color="red"))
fig3.add_shape(type='line',
               x0=pandas.Timestamp('2020-10-30'),
               y0=0,
               x1=pandas.Timestamp('2020-10-30'),
               y1=70,
               line=dict(color='Red'))
fig3.add_shape(type='line',
               x0=pandas.Timestamp('2020-12-15'),
               y0=0,
               x1=pandas.Timestamp('2020-12-15'),
               y1=70,
               line=dict(color='Red'))
fig3.add_annotation(x=pandas.Timestamp('2020-11-13'),y=68,
            text="Confinement",
            showarrow=False,
            font=dict(
            size=16,
            color="red"))

qualite_air_PM10_2021=requete_fonction(5,"date>='2021-01-01' AND date<='2021-12-30'")
qualite_airPM10_2021 = pandas.read_sql_query(qualite_air_PM10_2021, conn)
fig4 = fig_plot(qualite_airPM10_2021,"Qualite de l'air au niveau de PM10 en 2021",'2021-01-06','2021-01-01','2021-01-10')
fig4.add_shape(type='line',
               x0=pandas.Timestamp('2021-01-14'),
               y0=0,
               x1=pandas.Timestamp('2021-01-14'),
               y1=70,
               line=dict(color='Red'))
fig4.add_annotation(x=pandas.Timestamp('2021-01-17'),y=65,
            text="Couvret feu 14 jan",
            showarrow=False,
            font=dict(
            size=16,
            color="red"))

depassementPM10_confinement=requete_fonction(5,"niveau>=50 AND date>='2020-03-17' AND date<='2020-05-10'")
qualite_airePM10_confinement = pandas.read_sql_query(depassementPM10_confinement, conn)
nrow_qualite_airePM10_confinement=len(qualite_airePM10_confinement)
x = depassement_seuils_recommanderPM10['nom_station']
y = depassement_seuils_recommanderPM10['niveau']
text=depassement_seuils_recommanderPM10['date']
fig5=bar_plot(x,y,text,50)

depassementPM10_confinement2=requete_fonction(5,"niveau>=50 AND date>='2020-10-30' AND date<='2020-12-15'")
qualite_airPM10_confinement2 = pandas.read_sql_query(depassementPM10_confinement2, conn)
nrow_qualite_airPM10_confinement2=len(qualite_airPM10_confinement2)
# Analyse la qualite air NO2:
def fig_plot2(dataframe,title):
    fig=px.line(dataframe, x="date", y="niveau", color="nom_station",line_group="nom_station",
            hover_name="nom_station",title=title)
    fig.update_xaxes(title_text='Date')
    fig.update_yaxes(title_text='Niveau de polluant',range=[0,80])
    return(fig)

qualit_air_NO2=pandas.read_csv('data\mesures_NO2_annuelle.csv',sep=";")
nrow_qualit_air_NO2=len(qualit_air_NO2)
x = qualit_air_NO2['nom_station']
y = qualit_air_NO2['valeur']
text=qualit_air_NO2['date_debut']
fig6=bar_plot(x,y,text,40)

qualite_air_NO2_2019=requete_fonction(8,"date>='2019-01-01' AND date<='2019-12-30'")
qualite_airNO2_2019 = pandas.read_sql_query(qualite_air_NO2_2019, conn)
fig7=fig_plot2(qualite_airNO2_2019,"Qualite de l'air au niveau de NO2 en 2019")

qualite_air_NO2_2020=requete_fonction(8,"date>='2020-01-01' AND date<='2020-12-30'")
qualite_airNO2_2020 = pandas.read_sql_query(qualite_air_NO2_2020, conn)
fig8=fig_plot2(qualite_airNO2_2020,"Qualite de l'air au niveau de NO2 en 2020")
fig8.add_shape(type='line',
               x0=pandas.Timestamp('2020-03-17'),
               y0=0,
               x1=pandas.Timestamp('2020-03-17'),
               y1=70,
               line=dict(color='Red'))
fig8.add_shape(type='line',
               x0=pandas.Timestamp('2020-05-11'),
               y0=0,
               x1=pandas.Timestamp('2020-05-11'),
               y1=70,
               line=dict(color='Red'))
fig8.add_annotation(x=pandas.Timestamp('2020-03-30'),y=68,
            text="Confinement",
            showarrow=False,
            font=dict(
            size=16,
            color="red"))
fig8.add_shape(type='line',
               x0=pandas.Timestamp('2020-10-30'),
               y0=0,
               x1=pandas.Timestamp('2020-10-30'),
               y1=70,
               line=dict(color='Red'))
fig8.add_shape(type='line',
               x0=pandas.Timestamp('2020-12-15'),
               y0=0,
               x1=pandas.Timestamp('2020-12-15'),
               y1=70,
               line=dict(color='Red'))
fig8.add_annotation(x=pandas.Timestamp('2020-11-13'),y=68,
            text="Confinement",
            showarrow=False,
            font=dict(
            size=16,
            color="red"))

qualite_air_NO2_2021=requete_fonction(8,"date>='2021-01-01' AND date<='2021-12-30'")
qualite_airNO2_2021 = pandas.read_sql_query(qualite_air_NO2_2021, conn)
fig9=fig_plot2(qualite_airNO2_2021,"Qualite de l'air au niveau de NO2 en 2020")
fig9.add_shape(type='line',
               x0=pandas.Timestamp('2021-01-14'),
               y0=0,
               x1=pandas.Timestamp('2021-01-14'),
               y1=70,
               line=dict(color='Red'))
fig9.add_annotation(x=pandas.Timestamp('2021-01-17'),y=65,
            text="Couvret feu 14 jan",
            showarrow=False,
            font=dict(
            size=16,
            color="red"))
##############
app.layout = html.Div(children=[
    html.H3(children='Analyse de la pollution de l’air dans la région des Pays de la Loire'),
    
    html.Div([
        dcc.Markdown('''
**Contexte du projet**

Le 10 juillet 2020, le Conseil d'Etat a rendu publique une décision historique pour contraindre l'Etat Français à prendre des mesures immédiates en faveur de la qualité de l'air.
Les seuils fixés par l'Europe pour les particules PM10 et le dioxyde d'azote NO2 doivent être respectés sous peine de lourdes amendes.

-La région des Pays de la Loire souhaite savoir s’il y a eu des dépassements de concentration sur des stations locales au cours des deux dernières années.

-La région souhaite aussi visualiser l’influence des confinements sur la qualité de l’air dans la région en fonction des départements et de deux types de polluants.

Pour répondre au demande des élus locaux , sur ces deux problèmes: dépassement des concentrations et influence des confinements,
on a importé des données en format CSV, qui représente les mesures de la pollution de l’air pour les deux particules PM10, NO2 dans les différentes stations de la région.
Ces données sont accessibles sur le site http://www.airpl.org/Air-exterieur/mesures-en-direct

Puis, on a créé une base de donnée postgres, contient trois tables, où les données importer ont inséré.
    ''')],style={'font-size':'20'}),
    html.Div([
        dcc.Markdown('''
    Analyse le dépassement du seuil recommandé pour le particule PM10 au cours de deux dernière années :''')],style={'font-size':'24','color':'red'}),
    dcc.Graph(
        id='example-graph',
        figure=fig
    ),
    html.Div([
        dcc.Markdown('''
**Détails sur les stations où il y a eu de dépassement de seuil recommande :** ''')],style={'font-size':'20','color':'darkblue'}),
    html.Div([
        generate_table(depassement_seuils_recommanderPM10,max_rows)]),

    html.H5("Analyse la qualité de l'air au niveau de PM10 a fin de visualiser linfluence de confinement:",style={'color' :'red'}),
    dcc.Graph(
            id='figure2',
            figure=fig2
        ),
   dcc.Graph(
            id='figure3',
            figure=fig3
        ),
    
   dcc.Graph(
            id='figure4',
            figure=fig4
        ),
    
    html.Div([
        dcc.Markdown('''
**Détails sur les stations où il y a eu de dépassement de seuil recommande PM10 pendant la premiere période de confinement:** ''')],style={'font-size':'20','color':'darkblue'}),
    dcc.Graph(
            id='figure5',
            figure=fig5
        ),
    generate_table(qualite_airePM10_confinement,nrow_qualite_airePM10_confinement),
   html.Div([
        dcc.Markdown('''
**Détails sur les stations où il y a eu de dépassement de seuil recommande PM10 pendant la deuxième période de confinement:** ''')],style={'font-size':'20','color':'darkblue'}), 
    
    generate_table(qualite_airPM10_confinement2,nrow_qualite_airPM10_confinement2),
    html.Div([
        dcc.Markdown('''
    Analyse le dépassement du seuil recommandé pour le particule NO2 au cours de deux dernière années :''')],style={'font-size':'24','color':'red'}),
    dcc.Graph(
            id='figure6',
            figure=fig6
        ),
    html.H5("Analyse la qualité de l'air au niveau de NO2, a fin de visualiser linfluence de confinement:",style={'color' :'red'}),
    dcc.Graph(
            id='figure7',
            figure=fig7
        ),
    dcc.Graph(
            id='figure8',
            figure=fig8
        ),
    dcc.Graph(
            id='figure9',
            figure=fig9
        )
])

if __name__ == '__main__':
    app.run_server(debug=True)
