# Lectura de los archivos tranmsaccionales

import pandas as pd
import numpy as np
import os, sys, logging, warnings, time
from multiprocessing import Pool

global path
path = os.getcwd()

global_session = pd.DataFrame()
count_session_events = False

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

        # data = data[data['client_id']=='+TNzsXNd57o=']
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
        # data['week'] = data['date'].dt.isocalendar().week
        data['week'] = data['date'].dt.week
        data['weekday'] = data['date'].dt.weekday
        data['day'] = data['date'].dt.day_name()
        data['turn'] = data.apply(lambda row: time_window(row[5]), axis=1)
        
        global global_session
        global_session = data
        return data
    
    return "Fuente no encontrada ..."

def footprints(font = None, count_session = False):
    
    global count_session_events
    count_session_events = count_session
    
    if font == 'bank_trx':
        
        file = path+'/data/bank_trx/'
        if count_session:
            file=file+'footprints_qty.data'
        else:
            file=file+'footprints_sum.data'
            
        try:
            print('Procesando archivo')
            footprints_data = pd.read_csv(file, low_memory=False)
            print('Archivo encontrado')
        except:
            print('Creando archivo nuevo')
            footprints_data = footprint_bank_transfers(file_name=file)
        return footprints_data
    
    return "Datos no encontrados ..."


def footprint_bank_transfers(file_name=None):
    ''' Funtion to create footprint, 
    it depend of the form on the sessions'''
    

    users = list(np.unique(global_session['client_id']))
    print('User quantity:',len(users))
    
    
    print('Procesando en parallelo ...')
    with Pool(38) as pool:
        resp_pool = pool.map(footprint_bt_user_parallel, users)    
    
    profiles = {}
    for elem in resp_pool:
        profiles[elem[0]]=elem[1]    # cargamos lista de indice "uid" con la data del cliente(json)  

    print('Creando footprints...')
    categories = list(np.unique(global_session['mccg']))
    turns = list(np.unique(global_session['turn']))
    day_of_week = list(np.unique(global_session['weekday']))

    ## Creamos la cabecera dinámica donde se guardaran todos los footprints generados
    cabecera = 'user_id,year,week,profile_id,category,turn,size'
    for i in categories:
        for j in turns:                # numero de turnos
            for k in day_of_week:      # numero de dias
                #cabecera = cabecera+','+'c'+str(categories.index(i))+'t'+ \
                #str(turns.index(j))+'d'+str(day_of_week.index(k))
                cabecera = cabecera+','+'c'+str(i)+'t'+str(j)+'d'+str(k)
    cabecera = cabecera+'\n'

    print()
    print('Categorias:', categories)
    print('Turnos:', turns)
    print('Days of week:', day_of_week)
    print()

    lt = len(turns)
    lc = len(categories)
    lw = len(day_of_week)
    
    print('Guardando archivo:', file_name)
    fw=open(file_name,'w')  

    fw.write(cabecera)                    # Escribimos la cabecera
    footprints_c=0 

    for uid in profiles.keys():   
        profile_id=0
        for year in profiles[uid]:   
            for week in profiles[uid][year]:                        
                footprint_temp = np.zeros(lc*lt*lw)

                for category in profiles[uid][year][week]:
                    position_category = categories.index(category)
                    footprint_temp2 = np.zeros(lt*lw)

                    for turn in profiles[uid][year][week][category]:
                        position_turn = turns.index(turn)
                        days=profiles[uid][year][week][category][turn]
                        #print(year,category,turn,week,days)
   
                        for k in range(position_turn*lw,(position_turn+1)*lw):
                            footprint_temp2[k] += days[k-(position_turn*lw)]

                    for j in range(position_category*lt*lw,(position_category+1)*lt*lw):
                        footprint_temp[j] = footprint_temp2[j-(position_category*lt*lw)]

                # Escribimos los datos del primer comportamiento (Tensor)    
                txt = ''+str(uid)+','+str(year)+','+str(week)+','+str(profile_id)+','+ \
                      str(category)+','+str(turn)+','+str(sum(footprint_temp))
                
                for i in range(len(footprint_temp)):
                    txt = txt +','+str(footprint_temp[i])
                fw.write(txt +'\n')

                profile_id += 1    # perfil cambia cada unidad de fecha diferente
                footprints_c += 1
        fw.flush()
    fw.close()
    print ("Number of footprint: "+str(footprints_c))  

    data = pd.read_csv(file_name, low_memory=False)
    
    print('Limpiando columna sin datos')
    new_columns = []
    for col in data.columns:
        unicos = np.unique(data[col])
        if(len(unicos)>1):
            new_columns.append(col)
    data = data[new_columns]
    data['footprint_id'] = data['user_id'].map(str) +'-'+ data['year'].map(str) +'-'+ data['week'].map(str)
    data.insert(0, 'footprint_id', data.pop('footprint_id'))
    
    data.to_csv(file_name, index = False, header=True)
    #data.to_csv(file_name, header=True)
    
    return data


def footprint_bt_user_parallel(tupla):
    ''' Procesador de perfiles - PARALLEL'''
    user = tupla
    user_data = global_session[global_session['client_id'] == user]
    day_of_week = list(np.unique(global_session['weekday']))
    
    years = set(list(user_data['año']))              # Lista los años en que se tiene TXs registradas
    footprint_dict = {year:{} for year in list(years)}    # definimos 'year' como una lista 

    for index, row in user_data.iterrows():
        # print(row['año'], row['turn'], row['week'], row['weekday'], row['mccg'])
        
        año = row['año']
        turn = row['turn']
        week = row['week']
        weekday = row['weekday']
        weekday = day_of_week.index(weekday)
        categoria = row['mccg']
    
        # Si la semana no existe en el año
        if not(week in footprint_dict[año]):
            footprint_dict[año][week] = {}
        # Si el mccg no existe en la semana y año
        if not (categoria in footprint_dict[año][week]):
            footprint_dict[año][week][categoria]={}  #NUMERO DE MCCGs VARIABLES
        # Si el turno no existe en el mccg,se1ana y año
        if not (turn in footprint_dict[año][week][categoria]):
            # footprint_dict[año][week][categoria][turn]=np.array([0]*len(day_of_week))
            footprint_dict[año][week][categoria][turn]=np.zeros(len(day_of_week)) 
        
        if count_session_events:
            # suma montos "importancia por gastos"
            footprint_dict[año][week][categoria][turn][weekday]+=float(row['quantity'])
        else:
            # suma cantidades "importancia por compras"
            footprint_dict[año][week][categoria][turn][weekday]+=float(row['amount_sol'])
        
    return (user, footprint_dict)