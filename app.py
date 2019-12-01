from flask import Flask, jsonify
from sqlalchemy import create_engine
import pandas as pd 

app = Flask(__name__)

#engine = create_engine('sqlite:///hawaii.sqlite')
#con = engine.connect()

@app.route('/')
def home():
    return (
        'Welcome to the Hawaii Climate API! <br/>'
        'Available routes: <br/>'
        'localhost:5000/api/v1.0/precipitation<br/>'
        'localhost:5000/api/v1.0/stations<br/>'
        'localhost:5000/api/v1.0/tobs<br/>'
        'localhost:5000/api/v1.0/<start><br/>'
        'localhost:5000/api/v1.0/<start>/<end>'
    )

@app.route('/api/v1.0/precipitation')
def precipitation():
    engine = create_engine('sqlite:///hawaii.sqlite')
    con = engine.connect()
    sql = '''
    Select date, prcp from measurement
    order by date 
    '''
    data = pd.read_sql(sql=sql,con=con)
    responce = {}
    for index, row in data.iterrows():
        responce["{}".format(row[0])] = row[1]
    return jsonify(responce)


@app.route('/api/v1.0/stations')
def stations():
    engine = create_engine('sqlite:///hawaii.sqlite')
    con = engine.connect()
    sql = 'Select * from station'
    data = pd.read_sql(sql=sql,con=con)
    responce = {}
    for index, row in data.iterrows():
        responce["{}".format(row[1])] = [
            {'name':row[2]},
            {'location':[
                {'lat':row[3]},
                {'lng':row[4]},
                {'elevation':row[5]}
            ]}
        ]
    return jsonify(responce)

@app.route('/api/v1.0/tobs')
def tobs():
    engine = create_engine('sqlite:///hawaii.sqlite')
    con = engine.connect()
    sql ='''
    Select date, tobs 
    From measurement
    order by date
    '''
    data = pd.read_sql(sql=sql,con=con)
    data['date'] = pd.to_datetime(data['date'])
    last_date = data['date'].max()
    year_back = last_date - pd.DateOffset(years=1)
    data_responce = data.loc[((data['date'] <= last_date) & (data['date'] >= year_back)),['date','tobs']]
    responce = {}
    for index,row in data_responce.iterrows():
        responce["{}".format(row[0])] = row[1]
    return jsonify(responce)


@app.route('/api/v1.0/<start>')
def summary(start):
    engine = create_engine('sqlite:///hawaii.sqlite')
    con = engine.connect()
    sql ='''
    Select date, tobs 
    From measurement
    order by date
    '''
    start_date = pd.to_datetime(start)
    data = pd.read_sql(sql=sql,con=con)
    data['date'] = pd.to_datetime(data['date'])
    responce_df = data.loc[data['date']>=start_date,:]
    responce = {}
    responce['TAVG'] = responce_df['tobs'].mean()
    responce['TMIN'] = responce_df['tobs'].min()
    responce['TMAX'] = responce_df['tobs'].max()
    return responce

@app.route('/api/v1.0/<start>/<end>')
def start_end_sum(start,end):
    engine = create_engine('sqlite:///hawaii.sqlite')
    con = engine.connect()
    sql ='''
    Select date, tobs 
    From measurement
    order by date
    '''
    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)
    data = pd.read_sql(sql=sql,con=con)
    data['date'] = pd.to_datetime(data['date'])
    responce_df = data.loc[((data['date']>=start_date) & (data['date'] <= end_date)),:]
    responce = {}
    responce['TAVG'] = responce_df['tobs'].mean()
    responce['TMIN'] = responce_df['tobs'].min()
    responce['TMAX'] = responce_df['tobs'].max()
    return responce


        




if __name__ == '__main__':
    app.run(debug=True)

