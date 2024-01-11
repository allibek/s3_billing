import json
import sqlite3

db_path = '/backup/billing.db'
user = 'bigBucketTest'
date1 = '2024-01-10 08:00:34'
date2 = '2024-01-12 08:59:34'


def analize(path, user_id, from_date, to_date):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    user_data = c.execute("SELECT id, timestamp, user_id, tenant_id, user_stats, user_usage, buckets, buckets_usage FROM billing WHERE user_id=? AND timestamp>? AND timestamp<?", (user, from_date, to_date,)).fetchall()
    conn.commit()
    conn.close()

    for data in user_data:
        record_id = data[0]
        record_time = data[1]
        user_id = data[2]
        tenant_id = data[3]
        size_kb_actual = 0
        if json.loads(data[4])['size_kb_actual']:
            size_kb_actual = int(json.loads(data[4])['size_kb_actual'])
        user_usage = ''
        if json.loads(data[5]):
            user_usage = json.loads(data[5])
        bytes_sent = 0
        bytes_received = 0
        ops = 0
        successful_ops = 0
        if user_usage['summary'][0]['total']:
            bytes_sent = user_usage['summary'][0]['total'] ['bytes_sent']
            bytes_received = user_usage['summary'][0]['total']['bytes_received']
            ops = user_usage['summary'][0]['total']['ops']
            successful_ops = user_usage['summary'][0]['total']['successful_ops']
        buckets = []
        if json.loads(data[6]):
            buckets = json.loads(data[6])
        buckets_usage = ''
        if json.loads(data[7]):
            buckets_usage = json.loads(data[7])
        print('*********************************************************************')
        print('record_id: ', record_id)
        print('record_time: ', record_time)
        print('user_id: ', user_id)
        print('tenant_id: ', tenant_id)
        print('size_kb_actual: ', size_kb_actual)
#        print('user_usage: ', user_usage['summary'][0]['total'])
        print('buckets: ', buckets)
        print('bytes_sent: ', bytes_sent)
        print('bytes_received: ', bytes_received)
        print('ops: ', ops)
        print('successful_ops: ', successful_ops)
        print('######## buckets: ##########')
        for entry in buckets_usage['buckets']:
            if entry['bucket'] in buckets:
                print(entry['bucket'], entry['placement_rule'], entry['usage']['rgw.main']['size_kb_actual'])
        print('*********************************************************************')

if __name__ == '__main__':
    try:
        analize(db_path, user, date1, date2)
    except Exception as e:
        print(e)


