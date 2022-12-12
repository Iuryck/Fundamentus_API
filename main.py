import io
import flask
from zipfile import ZipFile
from flask import Flask, make_response
from flask_cors import CORS
from utils.stock_scraper import Fundamentus 
from flask import Response
import json

app = Flask(__name__)
CORS(app)
fundamentus = Fundamentus()

@app.route('/', methods=['GET'])
def test_api() -> dict:
    return {'status': 'API is up!'}

@app.route('/events/<ticker>', methods=['GET'])
def get_events(ticker:str) -> dict:
    return fundamentus.get_events(ticker).to_dict()

@app.route('/info/<ticker>', methods=['GET'])
def get_info(ticker:str) -> dict:
    return fundamentus.get_stock_info(ticker).to_dict()

@app.route('/dividends/<ticker>', methods=['GET'])
def get_dividends(ticker:str) -> dict:
    return fundamentus.get_dividends(ticker).to_dict()

@app.route('/fundamentals/<ticker>', methods=['GET'])
def get_fundamentals(ticker:str) -> dict:
    bytes_zip = fundamentus.get_fundamentals(ticker)
    zip_file = io.BytesIO(bytes_zip)    
    return flask.send_file(zip_file, download_name=f'{ticker}.zip')

@app.route('/tickers', methods=['GET'])
def get_tickers() -> dict:
    
    return fundamentus.get_tickers().to_dict()


if __name__ == '__main__':
    app.run(host='0.0.0.0')