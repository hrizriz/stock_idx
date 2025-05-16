from airflow.plugins_manager import AirflowPlugin
import pendulum
import pytz
from datetime import datetime, timedelta

# Definisikan timezone kustom
JAKARTA_TIMEZONE = pendulum.timezone("Asia/Jakarta")

class TimezonePlugin(AirflowPlugin):
    name = "timezone_plugin"
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    
    @staticmethod
    def get_jakarta_now():
        """
        Mendapatkan waktu sekarang dalam timezone Jakarta
        """
        return pendulum.now().in_timezone(JAKARTA_TIMEZONE)
    
    @staticmethod
    def convert_to_jakarta(dt):
        """
        Mengkonversi datetime ke timezone Jakarta
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        return dt.astimezone(JAKARTA_TIMEZONE)
    
    @staticmethod
    def get_jakarta_date_string(dt=None, format='%Y-%m-%d'):
        """
        Mendapatkan string tanggal dalam timezone Jakarta
        """
        if dt is None:
            dt = datetime.now()
        jakarta_dt = TimezonePlugin.convert_to_jakarta(dt)
        return jakarta_dt.strftime(format)
    
    @staticmethod
    def get_next_business_day(dt=None, days=1):
        """
        Mendapatkan hari kerja berikutnya (skip akhir pekan)
        """
        if dt is None:
            dt = TimezonePlugin.get_jakarta_now()
        
        result_date = dt + timedelta(days=days)
        
        # Skip weekend
        while result_date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            result_date = result_date + timedelta(days=1)
            
        return result_date

# Ekspos fungsi-fungsi utilitas untuk digunakan langsung di DAG
def get_jakarta_now():
    return TimezonePlugin.get_jakarta_now()

def convert_to_jakarta(dt):
    return TimezonePlugin.convert_to_jakarta(dt)

def get_jakarta_date_string(dt=None, format='%Y-%m-%d'):
    return TimezonePlugin.get_jakarta_date_string(dt, format)

def get_next_business_day(dt=None, days=1):
    return TimezonePlugin.get_next_business_day(dt, days)