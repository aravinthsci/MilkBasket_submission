# -*- coding: utf-8 -*-
from flask import Flask, render_template, request
from flask import jsonify
from elasticsearch import Elasticsearch
app = Flask(__name__)

es = Elasticsearch(['http://localhost:9200'])

@app.route('/')
def mainApp():
   return render_template('index.html')


@app.route('/getproduct', methods=['POST','GET'])
def index():
    if request.method == 'POST':
        result = request.form
        search_object = {'query': {'match': {'antecedent': result['Product_ID']}}}
        res = es.search(index='milkbasket_prod', body=search_object)
        return jsonify(res['hits']['hits'])

@app.route('/search',methods = ['POST', 'GET'])
def result():
   if request.method == 'POST':
      result = request.form
      return jsonify(result)

if __name__ == '__main__':
   app.run()
