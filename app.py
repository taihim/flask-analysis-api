from flask import Flask, send_from_directory, request
from flask_sqlalchemy import SQLAlchemy
from db_client import DBClient
from models import QueryData
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db/dataset.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/')
def home():
    return "AdjustTakehome Task"

@app.route('/api/query-str')
def get_query_str_api():
    params = dict(request.args)

    client = DBClient(db)
    client.get_results(params)

    return client.get_query_str()

@app.route('/api/filter')
def filter_api():
    params = dict(request.args)
    
    client = DBClient(db)

    return client.get_results(params)

if __name__ == "__main__":
    app.run(debug=True, port=5000, host='localhost')
