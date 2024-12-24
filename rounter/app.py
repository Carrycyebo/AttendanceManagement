from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO
import time

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# 模拟考勤数据
attendance_records = [
    {"name": "张三", "date": "2024-12-10", "status": "打卡成功"},
    {"name": "李四", "date": "2024-12-10", "status": "打卡成功"}
]

@app.route('/')#定义根路由，用于渲染 HTML 页面，访问/，返回对应视图
def index():
    return render_template('index1.html')

@app.route('/api/attendance', methods=['GET'])#定义一个处理 GET 请求的路由，当客户端向 /api/attendance 发送 GET 请求时，这个路由会被触发并执行相关的处理逻辑。
def get_attendance():
    # 获取考勤数据并返回给前端
    return jsonify(attendance_records)#jsonify() 方法将 Python 字典转换为 JSON 格式的响应，使数据能够呈现在html页面

@app.route('/api/checkin', methods=['POST'])#用来处理 POST 请求的，当用户提交考勤打卡数据时，将会把考勤记录保存到一个列表（attendance_records）中，并返回一个包含成功信息的 JSON 响应。
def checkin():
    data = request.json#获取客户端请求（上传数据到服务器）获取客户端请求的JSON数据并将其解析成Python字典存储到data中
    name = data.get('name')#获取数据，从接收到的 JSON 数据中提取 name 字段，表示打卡人的姓名
    status = "打卡成功" #设定了一个固定的状态，表示打卡成功
    # 模拟插入考勤记录
    attendance_records.append({
        "name": name,
        "date": "2024-12-10",
        "status": status
    })
    return jsonify({"message": "打卡成功", "name": name, "status": status})

# 新增函数用于统计签到和签退人数并发送给前端
def send_attendance_data():
    while True:
        # 模拟统计签到人数（这里简单假设根据考勤记录状态统计，实际可能更复杂）
        sign_in_count = len([record for record in attendance_records if record['status'] == '打卡成功'])
        sign_out_count = 0  # 假设目前没有签退逻辑，签退人数暂设为0
        data = {
            "signInCount": sign_in_count,
            "signOutCount": sign_out_count
        }
        socketio.emit('attendance_data', data)  # 直接发送字典数据，前端可直接解析
        time.sleep(5)  # 每隔5秒发送一次数据，可根据需求调整时间间隔

@socketio.on('attendance_connect')
def handle_connect(data):
    print("客户端已连接，消息:", data)
    socketio.start_background_task(send_attendance_data)

if __name__ == '__main__':
    # socketio.run(app, debug=True)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True)  #本地测试采用