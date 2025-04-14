# scripts/db_dashboard.py
from flask import Flask, render_template_string
import psycopg2
import os

app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Database Status</title>
    <style>
        body { font-family: Arial; margin: 20px; }
        .status { padding: 10px; margin: 5px; border-radius: 5px; }
        .ok { background-color: #d4edda; }
        .error { background-color: #f8d7da; }
        table { border-collapse: collapse; width: 100%; }
        th, td { text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <h1>Database Status Dashboard</h1>
    
    <div class="status {{ 'ok' if connected else 'error' }}">
        <h2>Connection: {{ 'Connected' if connected else 'Disconnected' }}</h2>
        {% if not connected %}
            <p>Error: {{ error }}</p>
        {% endif %}
    </div>
    
    {% if connected %}
        <h2>Tables</h2>
        <table>
            <tr>
                <th>Table Name</th>
                <th>Record Count</th>
                <th>Status</th>
            </tr>
            {% for table in tables %}
            <tr>
                <td>{{ table.name }}</td>
                <td>{{ table.count }}</td>
                <td>{{ table.status }}</td>
            </tr>
            {% endfor %}
        </table>
        
        <h2>Recent Operations</h2>
        <pre>{{ recent_logs }}</pre>
    {% endif %}
</body>
</html>
"""

def get_db_connection():
    try:
        conn = psycopg2.connect(
            os.environ.get('DATABASE_URL', 'postgresql://airflow:airflow@localhost:5432/job_collection')
        )
        return conn, None
    except Exception as e:
        return None, str(e)

@app.route('/')
def dashboard():
    conn, error = get_db_connection()
    connected = conn is not None
    tables = []
    recent_logs = "No logs available"
    
    if connected:
        cursor = conn.cursor()
        
        # Get tables
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
        table_names = [row[0] for row in cursor.fetchall()]
        
        # Get record counts
        for table_name in table_names:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            status = "OK" if count > 0 else "Empty"
            tables.append({"name": table_name, "count": count, "status": status})
        
        # Get recent logs
        try:
            with open('/opt/airflow/data/db_operations.log', 'r') as f:
                recent_logs = ''.join(f.readlines()[-20:])  # Last 20 lines
        except:
            recent_logs = "Log file not found"
        
        conn.close()
    
    return render_template_string(HTML_TEMPLATE, 
        connected=connected, 
        error=error, 
        tables=tables, 
        recent_logs=recent_logs
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)