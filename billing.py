import time
import sqlite3
import pandas as pd
from rgwadmin import RGWAdmin

access_key = '****************************************'
secret_key = '****************************************'
server = '********************************************'
log_path = '/var/log/billing.log'
db_path = '/backup/billing.db'
debug = True

timeout=600
rgw = RGWAdmin(access_key=access_key, secret_key=secret_key, server=server, timeout=timeout)

def get_stat():
    result_DF = pd.DataFrame()
    users = rgw.get_users()
    for user in users:
        user_stats = rgw.get_user(user, stats=True)
        buckets = rgw.get_bucket(uid=user)
        buckets_usage = rgw.get_usage(uid=user, show_entries=True)
        for entry in buckets_usage['entries']:
            for bucket in entry['buckets']:
                if bucket['bucket'] in buckets:
                    df = pd.DataFrame(index=[str(time.time())])
                    df['user_id'] = str(user_stats['user_id'])
                    df['tenant_id'] = str(user_stats['tenant'])
                    df['stats_size_kb_actual'] = user_stats['stats']['size_kb_actual']
                    df["stats_num_objects"] = user_stats['stats']['num_objects']
                    if user_stats['tenant']:
                        df['bucket'] = df['tenant_id'][0] + '/' + bucket['bucket']
                    else:
                        df['bucket'] = bucket['bucket']
                    if debug:
                        print('*******************************************************************************************************************************')
                        print('user_stats: ', user_stats)
                        print('user_id: ', df['user_id'][0])
                        print('tenant_id: ', df['tenant_id'][0])
                        print('bucket: ', df['bucket'][0])
                        print('stats_size_kb_actual: ', df['stats_size_kb_actual'][0])
                        print('stats_num_objects: ', df['stats_num_objects'][0])
                    for category in bucket['categories']:
                        category_name = category['category']
                        if category_name:
                            if category['bytes_sent']:
                                df[category_name + '_bs'] = category['bytes_sent']
                                if debug:
                                    print(category_name + '_bs: ', df[category_name + '_bs'][0])
                            if category['bytes_received']:
                                df[category_name + '_br'] = category['bytes_received']
                                if debug:
                                    print(category_name + '_br: ', df[category_name + '_br'][0])
                            if category['ops']:
                                df[category_name + '_ops'] = category['ops']
                                if debug:
                                    print(category_name + '_ops: ', df[category_name + '_ops'][0])
                            if category['successful_ops']:
                                df[category_name + '_sops'] = category['successful_ops']
                                if debug:
                                    print(category_name + '_sops: ', df[category_name + '_sops'][0])
                    bucket_usage = rgw.get_bucket(bucket=df['bucket'][0])
                    if debug:
                        print('bucket_usage: ', bucket_usage['usage'])
                    if bucket_usage['placement_rule']:
                        df['placement_rule'] = bucket_usage['placement_rule']
                    if bucket_usage['usage']:
                        if bucket_usage['usage']['rgw.main']['size_kb_actual']:
                            df['bucket_size_kb_actual'] = bucket_usage['usage']['rgw.main']['size_kb_actual']
                        if bucket_usage['usage']['rgw.main']['num_objects']:
                            df['bucket_num_objects'] = bucket_usage['usage']['rgw.main']['num_objects']
                    else:
                        df['bucket_size_kb_actual'] = 0
                        df['bucket_num_objects'] = 0
                    if debug:
                        print('placement_rule: ', df['placement_rule'][0])
                        print('bucket_size_kb_actual: ', df['bucket_size_kb_actual'][0])
                        print('bucket_num_objects: ', df['bucket_num_objects'][0])
                        print('*******************************************************************************************************************************')
                    result_DF = pd.concat([result_DF, df])
    conn = sqlite3.connect(db_path)
    try:
        result_DF.to_sql(name='billing', con=conn, if_exists='append', index=True)
    except Exception as e:
        result_DF = pd.concat([result_DF, pd.read_sql('SELECT TOP 0 * FROM billing', con=conn)])
        result_DF.to_sql(name='billing', con=conn, if_exists = 'replace', index=True)
        print(e)
    conn.close()

if __name__ == '__main__':
    try:
        get_stat()
    except Exception as e:
        if debug:
           print(e)
        f = open(log_path, 'a')
        f.write(str(e))
        f.close()


