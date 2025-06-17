from flask import Flask, jsonify, render_template, Blueprint
from api.service.mysql_service import get_most_absent_students, get_personal_absent_recommendations, get_student_absence_predictions, get_top_absent_per_course, get_user_recommendations

page_bp = Blueprint('page', __name__)


@page_bp.route('/')
def index():
    return render_template('index.html')

@page_bp.route('/api/most_absent_students')
def api_most_absent_students():
    data = get_most_absent_students()
    return jsonify(data)

@page_bp.route('/api/personal_absent_recommendations')
def api_personal_absent_recommendations():
    data = get_personal_absent_recommendations()
    return jsonify(data)

@page_bp.route('/api/student_absence_predictions')
def api_student_absence_predictions():
    data = get_student_absence_predictions()
    return jsonify(data)

@page_bp.route('/api/top_absent_per_course')
def api_top_absent_per_course():
    data = get_top_absent_per_course()
    return jsonify(data)

@page_bp.route('/api/user_recommendations')
def api_user_recommendations():
    data = get_user_recommendations()
    return jsonify(data)
