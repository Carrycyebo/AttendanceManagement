from flask import Flask, jsonify, render_template, Blueprint, request, redirect, url_for, session
from api.service.mysql_service import get_most_absent_students, get_personal_absent_recommendations, get_student_absence_predictions, get_top_absent_per_course, get_user_recommendations, check_user_credentials, create_user

page_bp = Blueprint('page', __name__)
page_bp.secret_key = 'your_secret_key_here'


@page_bp.route('/')
def index():
    if 'username' not in session:
        return redirect(url_for('page.login'))
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
    if 'username' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    data = get_user_recommendations()
    return jsonify(data)

@page_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if check_user_credentials(username, password):
            session['username'] = username
            return redirect(url_for('page.index'))
        return render_template('login.html', error='Invalid credentials')
    return render_template('login.html')

@page_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if create_user(username, password):
            return redirect(url_for('page.login'))
        return render_template('register.html', error='Username already exists')
    return render_template('register.html')

@page_bp.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('page.login'))
