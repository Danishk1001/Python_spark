import sys
from flask import Flask, jsonify
from pysparkka import query2, query3, query4, query5, query7, query8, query9, query6a, query6b, query1a, query1b, \
    query1c

app = Flask('my app')


@app.route('/api/query1a', methods=['GET'])
def api_query1a():
    data = query1a()
    return jsonify(data)

@app.route('/api/query1b', methods=['GET'])
def api_query1b():
    data = query1b()
    return jsonify(data)

@app.route('/api/query1c', methods=['GET'])
def api_query1c():
    data = query1c()
    return jsonify(data)

@app.route('/api/query2', methods=['GET'])
def api_query2():
    data = query2()
    return jsonify(data)


@app.route('/api/query3', methods=['GET'])
def api_query3():
    data = query3()
    return jsonify(data)


@app.route('/api/query4', methods=['GET'])
def api_query4():
    data = query4()
    return jsonify(data)


@app.route('/api/query5', methods=['GET'])
def api_query5():
    data = query5()
    return jsonify(data)


@app.route('/api/query6a', methods=['GET'])
def api_query6a():
    data = query6a()
    return jsonify(data)

@app.route('/api/query6b', methods=['GET'])
def api_query6b():
    data = query6b()
    return jsonify(data)

@app.route('/api/query7', methods=['GET'])
def api_query7():
    data = query7()
    return jsonify(data)


@app.route('/api/query8', methods=['GET'])
def api_query8():
    data = query8()
    return jsonify(data)


@app.route('/api/query9', methods=['GET'])
def api_query9():
    data = query9()
    return jsonify(data)


if __name__ == '__main__':
    app.run()
