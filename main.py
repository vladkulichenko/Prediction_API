import json
from datetime import datetime

import flask, csv
from pymongo import MongoClient
import pandas as pd
from flask import request, jsonify

app = flask.Flask(__name__)
app.config["DEBUG"] = True

client = MongoClient('mongodb+srv://Vladik:Zxxzzvlad12@predictionapi.rknvh.mongodb.net/test?authSource=admin&replicaSet=atlas-ka8i24-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true')

# Create some test data for our catalog in the form of a list of dictionaries.


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Dishes</h1> 
   <li>Philadelphia</li>
   <li>California</li>
   <li>Maki</li>
   <li>Sushi</li>
   <li>Noodles</li>
'''


# A route to return all of the available entries in our catalog.
@app.route(f'/api/v1', methods=['GET'])
def date_result():
    dish = request.args.get('dish')
    date = str(request.args.get('date'))
    time = str(request.args.get('hour'))
    date_and_time = date + ' ' + time + ':00:00'

    try:
        if date_and_time.split(' ')[0] > '2021-02-28':
            raise TypeError
    except TypeError:
        return jsonify('Date is not available')

    filter = {
        'ds': f'{date_and_time}'
    }
    project = {
        '_id': 0,
        f'{dish}': 1
    }

    result = client['Prediction_API']['Second_iter'].find(
        filter=filter,
        projection=project
    )

    single_res = []

    for i in result:
        for b in i:
            single_res.append(f"{{'{b}': {int(round(float(i.get(b)), 1))},"
                              f"'Date' : {date_and_time}}}")

    # single_res.append(f'Date: {date_and_time}')

    return jsonify(single_res)


@app.route(f'/api/v1/all', methods=['GET'])
def get_all():
    date = str(request.args.get('date'))
    time = str(request.args.get('hour'))
    date_and_time = date + ' ' + time + ':00:00'

    try:
        if date > '2021-02-28':
            raise TypeError
    except TypeError:
        return jsonify('Date is now available')

    filter = {
        'ds': f'{date_and_time}'
    }
    project = {
        '_id': 0,
        'California': 1,
        'Philadelphia': 1,
        'Noodles': 1,
        'Sushi': 1,
        'Maki': 1
    }

    result = client['Prediction_API']['Second_iter'].find(
        filter=filter,
        projection=project
    )

    all_res = []
    for i in result:
        for b in i:
            all_res.append(f"{b}: {int(round(float(i.get(b)),1))}")

    all_res.append(f'Date: {date_and_time}')

    return jsonify(all_res)


app.run()
