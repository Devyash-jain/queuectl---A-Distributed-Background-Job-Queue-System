from flask import Flask, render_template_string
from .storage import Storage

HTML = """
<!DOCTYPE html>
<html>
<head>
<title>QueueCTL Dashboard</title>
<style>
body { font-family: Arial, sans-serif; margin: 40px; }
table { border-collapse: collapse; width: 100%; margin-top: 20px; }
th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
h1 { margin-bottom: 5px; }
.state-box { display: inline-block; padding: 8px 12px; margin-right: 10px; background: #eee; border-radius: 6px; }
</style>
</head>
<body>
<h1>QueueCTL Dashboard</h1>

<h2>Summary</h2>
<div>
{% for name, value in stats.states.items() %}
  <span class="state-box"><b>{{ name }}</b>: {{ value }}</span>
{% endfor %}
  <span class="state-box"><b>DLQ</b>: {{ stats.dlq }}</span>
  <span class="state-box"><b>Workers</b>: {{ stats.workers }}</span>
</div>

<h2>Recent Jobs</h2>
<table>
<tr><th>ID</th><th>Command</th><th>State</th><th>Attempts</th><th>Updated</th></tr>
{% for j in jobs %}
<tr>
<td>{{ j.id }}</td>
<td>{{ j.command }}</td>
<td>{{ j.state }}</td>
<td>{{ j.attempts }}/{{ j.max_retries }}</td>
<td>{{ j.updated_at }}</td>
</tr>
{% endfor %}
</table>

<h2>Dead Letter Queue</h2>
<table>
<tr><th>Job ID</th><th>Reason</th><th>Time</th></tr>
{% for d in dlq %}
<tr>
<td>{{ d.job_id }}</td>
<td>{{ d.last_error }}</td>
<td>{{ d.failed_at }}</td>
</tr>
{% endfor %}
</table>

</body>
</html>
"""

def start_dashboard(db_path="queuectl.db", port=8000):
    db = Storage(db_path)
    app = Flask(__name__)

    @app.route("/")
    def index():
        stats = db.stats()
        jobs = db.list_jobs(state="all", limit=50)
        dlq = db.list_dlq(limit=50)
        return render_template_string(HTML, stats=stats, jobs=jobs, dlq=dlq)

    print(f"✅ Dashboard running at http://localhost:{port}")
    app.run(port=port)
