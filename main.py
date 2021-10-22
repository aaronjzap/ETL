import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime



#Convert to uppercase
def Open_normalization(name):
  df = pd.read_csv(name, delimiter=';', dtype = 'string' )
  df = df.astype({
  'fecha_nacimiento': 'datetime64[ns]',
  'fecha_vencimiento':  'datetime64[ns]', 
  'deuda' : 'int',
  'altura': 'int',
  'peso': 'int', 
  'prioridad': 'int'
  }, errors='ignore')

  df = df.applymap(lambda s:s.upper() if type(s) == str else s)
  return df

#Change names
def Rename(df):
  df = df.rename(columns={
  'fecha_nacimiento': 'birth_date',
  'fecha_vencimiento':  'due_date', 
  'deuda' : 'due_balance',
  'direccion': 'address', 
  'correo': 'email',
  'estatus_contacto': 'status',
  'prioridad': 'priority',
  'telefono': 'phone'
  })
  return df
   
# Calculate age
def Age(df):
  now = datetime.now()
  df['age'] = now.year-df['birth_date'].dt.year 
  -(now.month<df['birth_date'].dt.month) 
  -((now.month==df['birth_date'].dt.month) & (now.day<df['birth_date'].dt.day))
  return df  

#Days with debt
def Delinquency(df):  
  df['delinquency'] = (datetime.now() -df['due_date']).dt.days
  return df  

#Define gruop of age.

def Group(df):
  age = df['age']
  conditions = [
  (age<= 20),
  (age > 20) & (age <= 30),
  (age > 30) & (age <= 40),
  (age > 40) & (age <= 50),
  (age > 50) & (age <= 60),
  (age > 60)
  ]  
  
  choices = [1,2,3,4,5,6]
  df['age_group'] = np.select(conditions, choices, default='0')
  return df

#Save to a sql database
def To_sql(df): 
  import sqlite3
  conn = sqlite3.connect('output/database.db3')
  c = conn.cursor()
  c.execute('CREATE TABLE IF NOT EXISTS clientes (first_name varchar, last_name text, gender text, fecha_nacimiento date, due_data date, deu_balance int, adress txt, altura int, peso int, email text, status text,  priority int, phone  text, age int, age_group int) ')
  conn.commit()

  df.to_sql('clientes', conn, if_exists='replace', index = False)

  c.execute('''  
		CREATE TABLE  IF NOT EXISTS  customers  as SELECT fiscal_id, first_name,last_name, gender, birth_date, age, age_group, due_date, delinquency, due_balance, address 
    FROM clientes
          ''')
    
  c.execute('''  
		CREATE TABLE  IF NOT EXISTS  emails  as SELECT fiscal_id,  email, status, priority FROM clientes
          ''')
           
  c.execute('''  
		CREATE TABLE  IF NOT EXISTS  phones  as SELECT fiscal_id,  phone, status, priority FROM clientes
          ''')
  c.execute(''' DROP TABLE  IF  EXISTS clientes''')

  c.close()




if __name__ == "__main__":
  name = input("Intoduzca la direccion del archivo *.csv: \t")
  if len(name)<1:
      print("Compruebe la dirección")
  else:
    df = Open_normalization(name)
    df = Rename(df)
    df = Age(df)
    df = Group(df)
    Delinquency(df)
    To_sql(df)
    print("Datos guardados en \'output/database.db3\'")










