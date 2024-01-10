import time
import sqlite3
import pandas as pd
from rgwadmin import RGWAdmin

access_key = '****************************************'
secret_key = '****************************************'
server =     '****************************************'
log_path = '/var/log/billing.log'
db_path = '/backup/billing.db'
debug = True

timeout=600
rgw = RGWAdmin(access_key=access_key, secret_key=secret_key, server=server, timeout=timeout)

def init_db(path):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS billing (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, 
                    user_id TEXT NOT NULL, 
                    tenant_id TEXT, 
                    user_stats TEXT, 
                    user_usage TEXT, 
                    buckets TEXT, 
                    buckets_usage TEXT )''')
    conn.commit()
    conn.close()

def get_stat(path):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    all_users = rgw.get_users()
    for user in all_users:
        bucket_data = []
        user_stats = rgw.get_user(user, stats=True)
        user_stats_str = ''
        if user_stats['stats']:
            user_stats_str = json.dumps(user_stats['stats'])
        user_id = ''
        if user_stats['user_id']:
            user_id = user_stats['user_id']
        tenant_id = ''
        if user_stats['tenant']:
            tenant_id = user_stats['tenant']
        user_usage = json.dumps(rgw.get_usage(uid=user, show_entries=True, show_summary=True))
        buckets=rgw.get_bucket(uid=user)
        buckets_str = json.dumps(buckets)
        for bucket in buckets:
            bucket_info = {}
            if tenant_id:
                bucket = tenant_id + '/' + bucket
            bucket_info['bucket'] = bucket
            rgw_get_bucket = rgw.get_bucket(bucket=bucket)
            bucket_info['placement_rule'] = ''
            if rgw_get_bucket['placement_rule']:
                bucket_info['placement_rule'] = rgw_get_bucket['placement_rule']
            bucket_info['usage'] = ''
            if rgw_get_bucket['usage']:
                bucket_info['usage'] = rgw_get_bucket['usage']
            bucket_data.append(bucket_info)
        buckets_usage = json.dumps({'buckets': bucket_data})

        c.execute("INSERT INTO billing (user_id,tenant_id,user_stats,user_usage,buckets,buckets_usage) VALUES (?, ?, ?, ?, ?, ?)",
              (user_id,tenant_id,user_stats_str,user_usage,buckets_str,buckets_usage))
    conn.commit()
    conn.close()

if __name__ == '__main__':
    try:
        init_db(db_path)
        get_stat(db_path)
    except Exception as e:
        if debug:
           print(e)
        f = open(log_path, 'a')
        f.write(str(e))
        f.close()


