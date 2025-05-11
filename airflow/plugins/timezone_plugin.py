from airflow.plugins_manager import AirflowPlugin
import pendulum
import os

# Set timezone untuk logs
os.environ["TZ"] = "Asia/Jakarta"

class TimezonePlugin(AirflowPlugin):
    name = "timezone_plugin"
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []