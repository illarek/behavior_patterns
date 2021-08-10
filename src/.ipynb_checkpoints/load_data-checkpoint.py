# Lectura de los archivos tranmsaccionales

import pandas as pd
import os, sys, logging, warnings, time
from multiprocessing import Pool

global path
path = os.getcwd()

global session_data_g


def session_data(font = None):
    ''' seleccionamos la fuente que se procesara,
        si se añade una nueva se tiene que codificar.
        Params:
            case_font: nombre de la fuente
            
    '''
    if font == 'bank_trx':
        infile = path+'/data/bank_trx/consulta_original.csv'
        data = pd.read_csv(infile)
        data =data[['client_id','date','año','mes','dia','hora',
                    'merchant_departement', 'merchant_province',
                    'merchant_district','mccg','mccg_name','mcc',
                    'quantity','amount_usd','amount_sol']]
        # return 'Procesando datos "bank_trx"...'
        return data
    
    return "Fuente no encontrada ..."

def footprints(font = None):
    
    if font == 'bank_trx':
        sessions_data = session_data(font=font)
        session_data_g = sessions_data
        footprints_data = footprint_bank_transactions(data=sessions_data)
        
        return footprints_data
    return "Datos no encontrados ..."


def footprint_bank_transactions(data=None):
    ''' Funtion to create footprint, 
    it depend of the form on the sessions'''
    
    def time_window(hora):
        tw = -1
        if hora >=0:
            tw = 1       # Madrugada
        if hora >=6:
            tw = 2      # Mañana
        if hora >=12:
            tw = 3      # Tarde
        if hora >=18:
            tw = 4      # Noche
        return tw   

    data['date'] = pd.to_datetime(data['date'])
    data['week'] = data['date'].dt.week
    data['weekday'] = data['date'].dt.weekday
    data['day'] = data['date'].dt.day_name()
    data['turn'] = data.apply(lambda row: time_window(row[5]), axis=1)
    
    data = session_data_g

    users = list(np.unique(data['client_id']))
    print('User quantity:',len(users))
    
    with Pool(20) as pool:
        resp_pool = pool.map(footprint_user, users)    
    
    profiles = {}
    for elem in resp_pool:
        profiles[elem[0]]=elem[1]                     # cargamos lista de indice "uid" con la data del cliente(json)  
        
    footprints_data = profiles
    return footprints_data


def footprint_user(tupla):
    ''' Procesador de perfiles - PARALLEL'''
    data = session_data_g
    user = tupla
    user_data = data[data['client_id'] == user]
    years = set(list(user_data['año']))              # Lista los años en que se tiene TXs registradas
    footprint_dict = {year:{} for year in list(years)}    # definimos 'year' como una lista 

    for index, row in user_data.iterrows():
        # print(row['año'], row['turn'], row['week'], row['weekday'], row['mccg'])
        
        año = row['año']
        turn = row['turn']
        week = row['week']
        weekday = row['weekday']
        categoria = row['mccg']
    
        # Si la semana no existe en el año
        if not(week in footprint_dict[año]):
            footprint_dict[año][week] = {}
        # Si el mccg no existe en la semana y año
        if not (categoria in footprint_dict[año][week]):
            footprint_dict[año][week][categoria]={}  #NUMERO DE MCCGs VARIABLES
        # Si el turno no existe en el mccg,semana y año
        if not (turn in footprint_dict[año][week][categoria]):
            footprint_dict[año][week][categoria][turn]=np.array([0]*7)  #CUATRO TURNOS
        
        monto=False  
        if monto:
            # suma montos "importancia por gastos"
            footprint_dict[año][week][categoria][turn][weekday]+=row['amount_sol']
        else:
            # suma cantidades "importancia por compras"
            footprint_dict[año][week][categoria][turn][weekday]+=row['quantity']
        
    return (user, footprint_dict)