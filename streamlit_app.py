import streamlit as st
import pandas as pd
import numpy as np
import requests

col1, col2, col3 = st.columns([2.5, 6, 1])
with col1:
    st.write("")
with col2:
    st.title('Patrones de consumo en Almería (2015)')
with col3:
    st.write("")


DATA_URL = ('http://dbc7-35-229-110-173.ngrok.io')

def recoger(url):
    response = requests.get(url)
    return response.json()
    

@st.cache
def load_data_sectores():
    datos = recoger((DATA_URL +'/api/v1/sectores'))
    data = pd.DataFrame(datos)
    data = data.iloc[:, ::-1]
    return data

@st.cache
def load_data_meses():
    datos = recoger((DATA_URL +'/api/v1/meses'))
    data = pd.DataFrame(datos)
    data = data.iloc[:, ::-1]
    return data

@st.cache
def load_data_summer():
    datos = recoger((DATA_URL +'/api/v1/summer'))
    data = pd.DataFrame(datos)
    data = data.iloc[:, ::-1]
    return data       

@st.cache
def load_data_winter():
    datos = recoger((DATA_URL +'/api/v1/winter'))
    data = pd.DataFrame(datos)
    data = data.iloc[:, ::-1]
    return data

@st.cache
def load_data_taltas():
    datos = recoger((DATA_URL +'/api/v1/taltas'))
    data = pd.DataFrame(datos)
    data = data.iloc[:, ::-1]
    return data

colSectores, colMeses = st.columns(2)

with colSectores:
    st.subheader('Gasto total por sector (€)')
    data_sectores = load_data_sectores()
    #st.write(data_sectores)
    #Bar Chart
    st.vega_lite_chart(data_sectores, {
        'mark': {'type': 'bar', 'tooltip': True},
        'height': 500,
        'width': 700,
        'encoding' : {
            'y' : {'field': 'SECTOR'},
            'x' : {'field': 'GASTO TOTAL', 'type': 'quantitative'},
            'color' : {'field': 'SECTOR'}
        },
        'config': {
            'background': "rgba(0, 0, 0, 0)",
            'axis': {
                'labelColor': '#FFF',
                'titleColor': '#FFF',
                'gridColor': '#FFF'
            },
            'title': {
                'color': '#FFF',
                'subtitleColor': '#FFF'
            },
            'domain': {
                'domainColor': "#FFF"
            },
            'ticks': {
                'tickColor': "#FFF"
            },
            'grid': {
                'gridColor': "#FFF"
            },
            'legend': {
                'disable': True
            }
        }
    })

with colMeses:
    st.subheader('Gasto total por mes (€)')
    data_meses = load_data_meses()
    #st.write(data_sectores)
    #Bar Chart
    st.vega_lite_chart(data_meses, {
        'mark': {'type': 'bar', 'tooltip': True, 'width': 20},
        'height': 500,
        'width': 700,
        'encoding' : {
            'x' : {'field': 'MES'},
            'y' : {'field': 'IMPORTES', 'type': 'quantitative'},
            'color' : {'field': 'MES', 'scale': {
                'scheme': 'spectral'
            }}
        },
        'config': {
            'background': "rgba(0, 0, 0, 0)",
            'axis': {
                'labelColor': '#FFF',
                'titleColor': '#FFF',
                'gridColor': '#FFF'
            },
            'title': {
                'color': '#FFF',
                'subtitleColor': '#FFF'
            },
            'domain': {
                'domainColor': "#FFF"
            },
            'ticks': {
                'tickColor': "#FFF"
            },
            'grid': {
                'gridColor': "#FFF"
            },
            'legend': {
                'disable': True
            }
        }
    })

colSummer, colWinter = st.columns(2)

with colSummer:
    st.subheader('Gastos según franjas horarias durante el verano (€)')
    data_summer = load_data_summer()
    #st.write(data_sectores)
    #Bar Chart
    st.vega_lite_chart(data_summer, {
        'mark': {'type': 'bar', 'tooltip': True},
        'height': 500,
        'width': 700,
        'encoding' : {
            'x' : {'field': 'FRANJA_HORARIA'},
            'y' : {'field': 'IMPORTE TOTAL', 'type': 'quantitative'},
            'color' : {'field': 'FRANJA_HORARIA', 'scale': {
                'scheme': 'goldorange'
            }}
        },
        'config': {
            'background': "rgba(0, 0, 0, 0)",
            'axis': {
                'labelColor': '#FFF',
                'titleColor': '#FFF',
                'gridColor': '#FFF'
            },
            'title': {
                'color': '#FFF',
                'subtitleColor': '#FFF'
            },
            'domain': {
                'domainColor': "#FFF"
            },
            'ticks': {
                'tickColor': "#FFF"
            },
            'grid': {
                'gridColor': "#FFF"
            },
            'legend': {
                'disable': True
            }
        }
    })

with colWinter:
    st.subheader('Gastos según franjas horarias durante el invierno (€)')
    data_winter = load_data_winter()
    #st.write(data_sectores)
    #Bar Chart
    st.vega_lite_chart(data_winter, {
        'mark': {'type': 'bar', 'tooltip': True},
        'height': 500,
        'width': 700,
        'encoding' : {
            'x' : {'field': 'FRANJA_HORARIA'},
            'y' : {'field': 'IMPORTE TOTAL', 'type': 'quantitative'},
            'color' : {'field': 'FRANJA_HORARIA', 'scale': {
                'scheme': 'teals'
            }}
        },
        'config': {
            'background': "rgba(0, 0, 0, 0)",
            'axis': {
                'labelColor': '#FFF',
                'titleColor': '#FFF',
                'gridColor': '#FFF'
            },
            'title': {
                'color': '#FFF',
                'subtitleColor': '#FFF'
            },
            'domain': {
                'domainColor': "#FFF"
            },
            'ticks': {
                'tickColor': "#FFF"
            },
            'grid': {
                'gridColor': "#FFF"
            },
            'legend': {
                'disable': True
            }
        }
    })

col4, colTemp, col6 = st.columns([2.5, 6, 1])
with col4:
    st.write("")
with colTemp:
    st.subheader('Gastos en días de temperaturas altas (€)')
    data_taltas = load_data_taltas()
    st.vega_lite_chart(data_taltas, {
        'mark': {'type': 'bar', 'tooltip': True},
        'width': 800,
        'encoding' : {
            'y' : {'field': 'DIA'},
            'x' : {'field': 'GASTOS', 'type': 'quantitative'},
            'color': {'field': 'TMed', 'type': 'quantitative', 'scale': {
                'scheme': 'orangered'
            }},
        },
        'config': {
            'background': "rgba(0, 0, 0, 0)",
            'axis': {
                'labelColor': '#FFF',
                'titleColor': '#FFF',
                'gridColor': '#FFF'
            },
            'title': {
                'color': '#FFF',
                'subtitleColor': '#FFF'
            },
            'domain': {
                'domainColor': "#FFF"
            },
            'ticks': {
                'tickColor': "#FFF"
            },
            'grid': {
                'gridColor': "#FFF"
            },
            'legend': {
                'labelColor': '#FFF',
                'titleColor': '#FFF'
            }
        }
    })
with col6:
    st.write("")
