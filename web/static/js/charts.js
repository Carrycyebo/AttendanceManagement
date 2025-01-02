var socket = io.connect('http://' + document.domain + ':' + location.port);

// 数据存储
var attendanceSummaryData = { A: 0, L: 0 };
var classAttendanceData = {};
var courseCountData = {};
var courseAttendanceData = {};
var studentAttendanceData = {};

// 图表对象
var attendanceSummaryChart, classAttendanceChart, courseCountChart, courseAttendanceChart;
var studentAttendanceCharts = {};

// WebSocket 消息监听
socket.on('attendance_summary_message', function(data) {
    attendanceSummaryData.A = data.data.status === "A" ? data.data.count : attendanceSummaryData.A;
    attendanceSummaryData.L = data.data.status === "L" ? data.data.count : attendanceSummaryData.L;
    updateAttendanceSummaryChart();
});

socket.on('class_attendance_message', function(data) {
    classAttendanceData[data.data.class_id] = {
        status: data.data.status,
        count: data.data.count
    };
    updateClassAttendanceChart();
});

socket.on('course_count_message', function(data) {
    courseCountData[data.data.course] = data.data.course_count;
    updateCourseCountChart();
});

socket.on('course_attendance_data', function(data) {
    data.data.forEach(function(item) {
        courseAttendanceData[item.course] = courseAttendanceData[item.course] || {};
        courseAttendanceData[item.course][item.status] = item.count;
    });
    updateCourseAttendanceChart();
});

socket.on('student_attendance_data', function(data) {
    studentAttendanceData = {};
    data.data.forEach(function(item) {
        var classId = item.class_id;
        if (!studentAttendanceData[classId]) {
            studentAttendanceData[classId] = {};
        }
        var studentName = item.student_name + ' (' + item.student_id + ')';
        if (!studentAttendanceData[classId][studentName]) {
            studentAttendanceData[classId][studentName] = { A: 0, L: 0 };
        }
        studentAttendanceData[classId][studentName][item.status] += item.count;
    });
    updateStudentAttendanceCharts();
});

// 初始化和更新图表函数
function initializeAttendanceSummaryChart() {
    // 禁用 UTC 时间
    Highcharts.setOptions({
        global: {
            useUTC: false
        }
    });

    attendanceSummaryChart = Highcharts.chart('attendanceSummaryChart', {
        chart: {
            type: 'spline',
            animation: Highcharts.svg, // 动画效果
            marginRight: 10,
            backgroundColor: 'transparent', // 设置背景透明
            events: {
                load: function () {
                    // 定时更新数据
                    const series1 = this.series[0];
                    const series2 = this.series[1];
                    setInterval(function () {
                        const x = (new Date()).getTime(); // 当前时间
                        const attendanceCount = attendanceSummaryData.A;
                        const absenceCount = attendanceSummaryData.L;

                        // 更新系列数据
                        series1.addPoint([x, attendanceCount], true, true);
                        series2.addPoint([x, absenceCount], true, true);
                    }, 2000); // 每2秒更新一次
                }
            }
        },
        title: {
            text: '签到信息实时统计',
            style: { color: '#FFFFFF' } // 标题字体颜色为白色
        },
        xAxis: {
            type: 'datetime',
            tickPixelInterval: 50, // 动态调整刻度
            labels: {
                style: {
                    color: '#FFFFFF' // 设置x轴刻度标签颜色为白色
                }
            }
        },
        yAxis: {
            title: {
                text: 'Count',
                style: { color: '#FFFFFF' } // Y轴标题字体颜色为白色
            },
            min: 0,
            tickInterval: 1, // 设置y轴的单位长度为1
            labels: {
                style: {
                    color: '#FFFFFF' // 设置y轴刻度标签颜色为白色
                }
            },
            plotLines: [{
                value: 0,
                width: 1,
                color: '#808080'
            }]
        },
        tooltip: {
            style: { color: '#FFFFFF' }, // 提示框字体颜色为白色
            formatter: function () {
                return (
                    '<b>' + this.series.name + '</b><br/>' +
                    Highcharts.dateFormat('%Y-%m-%d %H:%M:%S', this.x) + '<br/>' +
                    Highcharts.numberFormat(this.y, 2)
                );
            }
        },
        legend: {
            enabled: true,
            itemStyle: { color: '#FFFFFF' } // 图例字体颜色为白色
        },
        exporting: {
            enabled: true
        },
        series: [{
            name: '出勤',
            data: (function () {
                const data = [];
                const time = (new Date()).getTime();
                for (let i = -19; i <= 0; i++) {
                    data.push({
                        x: time + i * 2000,
                        y: 0 // 初始数据为 0
                    });
                }
                return data;
            })(),
            color: '#1E90FF', // 设置出勤折线的颜色为蓝色
            lineWidth: 3 // 设置出勤折线的粗细
        },
        {
            name: '缺勤',
            data: (function () {
                const data = [];
                const time = (new Date()).getTime();
                for (let i = -19; i <= 0; i++) {
                    data.push({
                        x: time + i * 2000,
                        y: 0 // 初始数据为 0
                    });
                }
                return data;
            })(),
            color: '#FF6347', // 设置缺勤折线的颜色为红色
            lineWidth: 3 // 设置缺勤折线的粗细
        }]
    });
}




function updateAttendanceSummaryChart() {
if (!attendanceSummaryChart) {
initializeAttendanceSummaryChart();
}
const now = new Date().getTime();
const attendanceCount = attendanceSummaryData.A;
const absenceCount = attendanceSummaryData.L;

// 添加新数据点到图表
attendanceSummaryChart.series[0].addPoint([now, attendanceCount], true, false);
attendanceSummaryChart.series[1].addPoint([now, absenceCount], true, false);
}

function initializeClassAttendanceChart() {
    classAttendanceChart = Highcharts.chart('classAttendanceChart', {
        chart: { 
            type: 'bar',
            backgroundColor: 'transparent' // 背景透明
        },
        title: { 
            text: '各个班级的实时出勤情况',
            style: { color: '#FFFFFF' } // 标题字体颜色
        },
        xAxis: { 
            categories: [],
            labels: { style: { color: '#FFFFFF' } }, // X轴标签颜色
            title: { style: { color: '#FFFFFF' } }   // X轴标题颜色
        },
        yAxis: { 
            title: { text: 'Count', style: { color: '#FFFFFF' } }, // Y轴标题颜色
            labels: { style: { color: '#FFFFFF' } } // Y轴标签颜色
        },
        legend: { 
            itemStyle: { color: '#FFFFFF' } // 图例字体颜色
        },
        tooltip: { 
            style: { color: '#FFFFFF' } // 提示框字体颜色
        },
        series: [{
            name: '出勤',
            data: [],
            color: '#32CD32' // 自定义系列颜色
        }, {
            name: '缺勤',
            data: [],
            color: '#FF6347' // 自定义系列颜色
        }]
    });
}

function updateClassAttendanceChart() {
    var categories = Object.keys(classAttendanceData);
    var attendanceData = categories.map(classId => classAttendanceData[classId].status === 'A' ? classAttendanceData[classId].count : 0);
    var absenceData = categories.map(classId => classAttendanceData[classId].status === 'L' ? classAttendanceData[classId].count : 0);
    if (classAttendanceChart) {
        classAttendanceChart.xAxis[0].setCategories(categories);
        classAttendanceChart.series[0].setData(attendanceData);
        classAttendanceChart.series[1].setData(absenceData);
    } else {
        initializeClassAttendanceChart();
    }
}

function initializeCourseCountChart() {
    courseCountChart = Highcharts.chart('courseCountChart', {
        chart: { 
            type: 'column',
            backgroundColor: 'transparent' // 设置背景透明
        },
        title: { 
            text: '各个课程的实时出勤情况',
            style: { color: '#FFFFFF' } // 标题字体颜色为白色
        },
        xAxis: { 
            categories: [],
            labels: { style: { color: '#FFFFFF' } }, // X轴标签颜色
            title: { 
                text: '课程', // 可根据需求设置标题
                style: { color: '#FFFFFF' } // X轴标题字体颜色
            }
        },
        yAxis: { 
            title: { 
                text: '人数统计',
                style: { color: '#FFFFFF' } // Y轴标题字体颜色
            },
            labels: { style: { color: '#FFFFFF' } } // Y轴标签字体颜色
        },
        legend: { 
            itemStyle: { color: '#FFFFFF' } // 图例字体颜色
        },
        tooltip: { 
            style: { color: '#FFFFFF' } // 提示框字体颜色
        },
        series: [{
            name: 'Count',
            data: [],
            color: '#1E90FF' // 数据列颜色，设置为明亮的颜色
        }]
    });
}


function updateCourseCountChart() {
    var categories = Object.keys(courseCountData);
    var data = categories.map(course => courseCountData[course]);
    if (courseCountChart) {
        courseCountChart.xAxis[0].setCategories(categories);
        courseCountChart.series[0].setData(data);
    } else {
        initializeCourseCountChart();
    }
}

function initializeCourseAttendanceChart() {
    courseAttendanceChart = Highcharts.chart('courseAttendanceChart', {
        chart: {
            type: 'bar',
            backgroundColor: 'transparent' // 设置背景透明
        },
        title: {
            text: '各个课程的总计出勤情况',
            style: { color: '#FFFFFF' } // 标题颜色为白色
        },
        xAxis: {
            categories: [],
            labels: { style: { color: '#FFFFFF' } }, // X轴标签颜色为白色
            title: {
                text: '课程',
                style: { color: '#FFFFFF' } // X轴标题颜色为白色
            }
        },
        yAxis: {
            title: {
                text: '人数统计',
                style: { color: '#FFFFFF' } // Y轴标题颜色为白色
            },
            labels: { style: { color: '#FFFFFF' } } // Y轴标签颜色为白色
        },
        legend: {
            itemStyle: { color: '#FFFFFF' } // 图例字体颜色为白色
        },
        tooltip: {
            style: { color: '#FFFFFF' } // 提示框字体颜色为白色
        },
        series: [{
            name: '出勤',
            data: [],
            color: '#1E90FF' // 数据列颜色为亮蓝色
        }, {
            name: '缺勤',
            data: [],
            color: '#FF4500' // 数据列颜色为亮橙色
        }]
    });
}


function updateCourseAttendanceChart() {
    var categories = Object.keys(courseAttendanceData);
    var attendanceData = categories.map(course => courseAttendanceData[course].A || 0);
    var absenceData = categories.map(course => courseAttendanceData[course].L || 0);
    if (courseAttendanceChart) {
        courseAttendanceChart.xAxis[0].setCategories(categories);
        courseAttendanceChart.series[0].setData(attendanceData);
        courseAttendanceChart.series[1].setData(absenceData);
    } else {
        initializeCourseAttendanceChart();
    }
}

function initializeStudentAttendanceCharts(classId) {
    var attendanceContainer = 'studentAttendanceChart_' + classId + '_A';
    var absenceContainer = 'studentAttendanceChart_' + classId + '_L';

    // 确保容器存在
    if (!document.getElementById(attendanceContainer) || !document.getElementById(absenceContainer)) {
        console.error(`Container for class ${classId} charts not found!`);
        return;
    }

    // 初始化出勤图表
    studentAttendanceCharts[classId + '_A'] = Highcharts.chart(attendanceContainer, {
        chart: {
            type: 'pie',
            backgroundColor: 'transparent' // 设置背景透明
        },
        title: { 
            text: `Class ${classId} - 出勤统计`, 
            style: { color: '#FFFFFF' } // 标题字体颜色为白色
        },
        tooltip: {
            style: { color: '#FFFFFF' } // 提示框字体颜色为白色
        },
        plotOptions: {
            pie: {
                dataLabels: {
                    enabled: true,
                    style: {
                        color: '#FFFFFF' // 数据标签字体颜色为白色
                    }
                }
            }
        },
        legend: {
            itemStyle: { color: '#FFFFFF' } // 图例字体颜色为白色
        },
        series: [{
            name: '出勤',
            data: [],
            colors: ['#1E90FF', '#32CD32', '#FFD700', '#FF4500'] // 调整颜色为高对比度
        }]
    });

    // 初始化缺勤图表
    studentAttendanceCharts[classId + '_L'] = Highcharts.chart(absenceContainer, {
        chart: {
            type: 'pie',
            backgroundColor: 'transparent' // 设置背景透明
        },
        title: { 
            text: `Class ${classId} - 缺勤统计`, 
            style: { color: '#FFFFFF' } // 标题字体颜色为白色
        },
        tooltip: {
            style: { color: '#FFFFFF' } // 提示框字体颜色为白色
        },
        plotOptions: {
            pie: {
                dataLabels: {
                    enabled: true,
                    style: {
                        color: '#FFFFFF' // 数据标签字体颜色为白色
                    }
                }
            }
        },
        legend: {
            itemStyle: { color: '#FFFFFF' } // 图例字体颜色为白色
        },
        series: [{
            name: '缺勤',
            data: [],
            colors: ['#DC143C', '#FF6347', '#FF8C00', '#8B0000'] // 调整颜色为高对比度
        }]
    });
}


function updateStudentAttendanceCharts() {
    for (var classId in studentAttendanceData) {
        var classData = studentAttendanceData[classId];
        var attendanceData = Object.keys(classData).map(student => ({ name: student, y: classData[student].A }));
        var absenceData = Object.keys(classData).map(student => ({ name: student, y: classData[student].L }));

        // 初始化图表（如果不存在）
        if (!studentAttendanceCharts[classId + '_A'] || !studentAttendanceCharts[classId + '_L']) {
            console.log(`Initializing charts for class ${classId}`);
            initializeStudentAttendanceCharts(classId);
        }

        // 更新出勤图表数据
        if (studentAttendanceCharts[classId + '_A']) {
            studentAttendanceCharts[classId + '_A'].series[0].setData(attendanceData);
        }

        // 更新缺勤图表数据
        if (studentAttendanceCharts[classId + '_L']) {
            studentAttendanceCharts[classId + '_L'].series[0].setData(absenceData);
        }
    }
}

$(document).ready(function() {
    initializeAttendanceSummaryChart();
    initializeClassAttendanceChart();
    initializeCourseCountChart();
    initializeCourseAttendanceChart();
});