# Lectura de los archivos tranmsaccionales

import pandas as pd
import os, sys, logging, warnings, time

global path
path = os.getcwd()


def read_data(font = None):
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
        sessions_data = read_data(font=font)
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
    data['turn'] = data.apply(lambda row: time_window(row[5]), axis=1)
    
    footprints_data = data
    return footprints_data