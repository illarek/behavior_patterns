# Lectura de los archivos tranmsaccionales

import pandas as pd
import numpy as np
import os, sys, logging, warnings, time
from multiprocessing import Pool

global path
path = os.getcwd()

global_session = pd.DataFrame()


def footprints(font = None,sessions):
    
    if font == 'bank_trx':
        footprints_data = footprint_bank_transactions(data=global_session)
        
        return footprints_data
    return "Datos no encontrados ..."


def footprint_bank_transactions(data=None):
    ''' Funtion to create footprint, 
    it depend of the form on the sessions'''
    

    users = list(np.unique(global_session['client_id']))
    print('User quantity:',len(users))
    
    
    print('Procesando en parallelo ...')
    with Pool(38) as pool:
        resp_pool = pool.map(footprint_user, users)    
    
    profiles = {}
    for elem in resp_pool:
        profiles[elem[0]]=elem[1]    # cargamos lista de indice "uid" con la data del cliente(json)  
                
    return profiles


def footprint_user(tupla):
    ''' Procesador de perfiles - PARALLEL'''
    user = tupla
    user_data = global_session[global_session['client_id'] == user]
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
        
        monto=True  
        if monto:
            # suma montos "importancia por gastos"
            footprint_dict[año][week][categoria][turn][weekday]+=row['amount_sol']
        else:
            # suma cantidades "importancia por compras"
            footprint_dict[año][week][categoria][turn][weekday]+=row['quantity']
        
    return (user, footprint_dict)