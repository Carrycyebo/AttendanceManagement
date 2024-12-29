from flask import Flask, jsonify, render_template, Blueprint
# from api.db.util import fetch_attendance_data

# @app.route('/api/attendance')
# def attendance_data():
#     data = fetch_attendance_data()
#     response = [{'name': d[0], 'value': d[1]} for d in data]
#     return jsonify(response)

page_bp = Blueprint('page', __name__)


@page_bp.route('/')
def index():
    return render_template('index.html')
