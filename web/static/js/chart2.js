// 缺勤最多的前3个学生图表
function initializeMostAbsentStudentsChart() {
    const container = document.getElementById('mostAbsentStudentsChart');
    if (!container) {
        console.error('Container mostAbsentStudentsChart not found!');
        return null;
    }
    return Highcharts.chart(container, {
        chart: { type: 'bar', backgroundColor: 'transparent' },
        title: { text: '缺勤最多的前3个学生', style: { color: '#FFFFFF' } },
        xAxis: { categories: [], labels: { style: { color: '#FFFFFF' } } },
        yAxis: { title: { text: '缺勤次数', style: { color: '#FFFFFF' } }, labels: { style: { color: '#FFFFFF' } } },
        legend: { itemStyle: { color: '#FFFFFF' } },
        tooltip: { style: { color: '#FFFFFF' } },
        series: [{ name: '缺勤次数', data: [], color: '#FF4500' }]
    });
}

// 个性化推荐的缺勤学生名单图表
function initializePersonalRecommendationsChart() {
    const container = document.getElementById('personalRecommendationsChart');
    if (!container) {
        console.error('Container personalRecommendationsChart not found!');
        return null;
    }
    return Highcharts.chart(container, {
        chart: { type: 'bar', backgroundColor: 'transparent' },
        title: { text: '个性化推荐的缺勤学生', style: { color: '#FFFFFF' } },
        xAxis: { categories: [], labels: { style: { color: '#FFFFFF' } } },
        yAxis: { title: { text: '推荐分数', style: { color: '#FFFFFF' } }, labels: { style: { color: '#FFFFFF' } } },
        legend: { itemStyle: { color: '#FFFFFF' } },
        tooltip: { style: { color: '#FFFFFF' } },
        series: [{ name: '推荐分数', data: [], color: '#1E90FF' }]
    });
}

// 初始化所有图表
let mostAbsentChart, personalRecChart;
$(document).ready(function() {
    mostAbsentChart = initializeMostAbsentStudentsChart();
    personalRecChart = initializePersonalRecommendationsChart();

    loadChartData();
});

// 加载图表数据
function loadChartData() {
    // 加载缺勤最多学生数据
    fetch('/api/most_absent_students')
        .then(response => response.json())
        .then(data => {
            console.log('Most absent students data:', data);
            const categories = data.length > 0 ? data.map(item => item.student_id) : ['暂无数据'];
            const values = data.length > 0 ? data.map(item => item.total_absences) : [0];
            mostAbsentChart.xAxis[0].setCategories(categories);
            mostAbsentChart.series[0].setData(values);
        })
        .catch(error => console.error('Error loading most absent students data:', error));

    // 加载个性化推荐数据
    fetch('/api/personal_absent_recommendations')
        .then(response => response.json())
        .then(data => {
            console.log('Personal recommendations data:', data);
            const categories = data.length > 0 ? data.map(item => `${item.student_id}-${item.course}`) : ['暂无数据'];
            const values = data.length > 0 ? data.map(item => item.recommendation_score) : [0];
            personalRecChart.xAxis[0].setCategories(categories);
            personalRecChart.series[0].setData(values);
        })
        .catch(error => console.error('Error loading personal recommendations data:', error));

    // 加载学生缺勤预测数据 → 显示为表格
    fetch('/api/student_absence_predictions')
        .then(response => response.json())
        .then(data => {
            console.log('Student absence predictions data:', data);
            updateStudentAbsencePredictionsTable(data);
        })
        .catch(error => console.error('Error loading student absence predictions data:', error));

    // 加载每门课程缺勤Top5学生数据 → 显示为表格
    fetch('/api/top_absent_per_course')
        .then(response => response.json())
        .then(data => {
            console.log('Top absent per course data:', data);
            updateTopAbsentPerCourseTable(data);
        })
        .catch(error => console.error('Error loading top absent per course data:', error));

    // 加载协同过滤推荐结果数据 → 显示为表格
    fetch('/api/user_recommendations')
        .then(response => response.json())
        .then(data => {
            console.log('User recommendations data:', data);
            updateUserRecommendationsTable(data);
        })
        .catch(error => console.error('Error loading user recommendations data:', error));
}

// 更新学生缺勤预测表格
function updateStudentAbsencePredictionsTable(data) {
    let html = '<table class="table table-dark table-striped"><thead><tr><th>学生ID</th><th>预测课程</th><th>预测分数</th></tr></thead><tbody>';
    if (data && data.length > 0) {
        data.forEach(item => {
            html += `<tr><td>${item.student_id}</td><td>${item.predicted_course}</td><td>${item.prediction_score.toFixed(2)}</td></tr>`;
        });
    } else {
        html += '<tr><td colspan="3">暂无数据</td></tr>';
    }
    html += '</tbody></table>';
    $('#studentAbsencePredictionsTable').html(html);
}

// 更新每门课程缺勤Top5表格
function updateTopAbsentPerCourseTable(data) {
    let grouped = {};
    if (data && data.length > 0) {
        data.forEach(item => {
            if (!grouped[item.course]) grouped[item.course] = [];
            grouped[item.course].push(item);
        });
    }

    let html = '';
    for (let course in grouped) {
        html += `<h5>${course} 课程缺勤 Top5</h5>`;
        html += '<table class="table table-sm table-dark table-bordered"><thead><tr><th>学生ID</th><th>缺勤次数</th></tr></thead><tbody>';
        grouped[course].slice(0, 5).forEach(item => {
            html += `<tr><td>${item.student_id}</td><td>${item.absent_count}</td></tr>`;
        });
        html += '</tbody></table>';
    }

    if (!html) {
        html = '<p>暂无数据</p>';
    }

    $('#topAbsentPerCourseTable').html(html);
}

// 更新协同过滤推荐结果表格
function updateUserRecommendationsTable(data) {
    let html = '<table class="table table-dark table-striped"><thead><tr><th>学生ID</th><th>课程</th><th>推荐评分</th></tr></thead><tbody>';
    if (data && data.length > 0) {
        data.forEach(item => {
            html += `<tr><td>${item.student_id}</td><td>${item.course}</td><td>${item.rating.toFixed(2)}</td></tr>`;
        });
    } else {
        html += '<tr><td colspan="3">暂无数据</td></tr>';
    }
    html += '</tbody></table>';
    $('#userRecommendationsTable').html(html);
}