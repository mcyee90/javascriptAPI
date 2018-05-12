import pandas as pd
import numpy as np

from sqlalchemy import create_engine, inspect
engine = create_engine("sqlite:///belly_button_biodiversity.sqlite", echo = False)
inspector = inspect(engine)

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import Session
session = Session(engine)
from sqlalchemy import func

from flask import Flask, render_template, json, jsonify, redirect

app = Flask(__name__)

Base = automap_base()
Base.prepare(engine, reflect = True)
Base.classes.keys()
Samples_Metadata = Base.classes.samples_metadata
OTU = Base.classes.otu
Samples = Base.classes.samples
session = Session(engine)

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/names')
def names():
    stmt = session.query(Samples).statement
    df = pd.read_sql_query(stmt, session.bind)
    df.set_index('otu_id', inplace=True)
    bbNames = list(df.columns)
    return jsonify(bbNames)

@app.route('/otu')
def otu():
    otu_list = session.query(OTU.lowest_taxonomic_unit_found).all()   
    otu_list = list(np.ravel(otu_list))
    return jsonify(otu_list)

@app.route('/metadata/<sample>')
def sample_metadata(sample):
    results = session.query(Samples_Metadata.SAMPLEID, Samples_Metadata.ETHNICITY,
        Samples_Metadata.GENDER, Samples_Metadata.AGE,
        Samples_Metadata.LOCATION, Samples_Metadata.BBTYPE).\
        filter(Samples_Metadata.SAMPLEID == sample[3:]).all()
    for result in results:
        sample_id = result[0]
        ethnicity = result[1]
        gender = result[2]
        age = result[3]
        location = result[4]
        bb_type = result[5]
    
    sample_metadata = {'AGE': age, "BBTYPE": bb_type, "ETHNICITY": ethnicity, "GENDER": gender, "LOCATION": location, "SAMPLEID":sample_id}
    return jsonify(sample_metadata)

@app.route('/wfreq/<sample>')
def wfreq(sample):
    washingFreq = session.query(Samples_Metadata.WFREQ).filter(Samples_Metadata.SAMPLEID == sample[3:]).all()
    washingFreq = int(np.ravel(washingFreq))
    return jsonify(washingFreq)

@app.route('/samples/<sample>')
def samples(sample):
    stmt = session.query(Samples).statement
    samplesDF = pd.read_sql_query(stmt, session.bind)
    if sample not in samplesDF.columns:
        return f"Error! Sample: {sample} Not Found!"
    samplesDF = samplesDF[samplesDF[sample] > 1]
    samplesDF = samplesDF.sort_values(by=sample, ascending=0)
    data = [{
        "otu_ids": samplesDF[sample].index.values.tolist(),
        "sample_values": samplesDF[sample].values.tolist()
    }]
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)