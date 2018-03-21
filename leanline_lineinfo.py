import json
import sys
import requests
import psycopg2

import config

# Data config
token = 'bWljcjptaWNy'
url = 'http://192.168.128.51:8080/Thingworx/Things/Assy_Line_LineInfo_DT/Services/GetDataTableEntries'
headers = {
    'Authorization': 'Basic {0}'.format(token),
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

# Get data from Thingworx
try:
    response = requests.post(url, headers=headers, data={})
except requests.exceptions.RequestException as e:  # This is the correct syntax
    print(e)
    sys.exit(1)

data = json.loads(response.content.decode('latin1'))

print(type(data['rows'][0]['PressureFbk']))

# DB connection initialization
conn = psycopg2.connect('host={0} dbname={1} user={2}'.format(config.db_host, config.db_name, config.db_user))
cur = conn.cursor()

# str_1 = cur.mogrify("""(%(key)s, %(timestamp)s, now(), now(),
# %(CycleTime)s, %(CycleTimePallet)s, %(CycleTimeTrigger)s,
# %(EmptyWait)s, %(EmptyWaitTrigger)s,
# %(FullWait)s, %(FullWaitTrigger)s,
# %(GoodPallet)s, %(TotalPallet)s,
# %(HumidityFbk)s, %(PressureFbk)s, %(TemperatureFbk)s)
# """, data['rows'][0])
# print(type(str_1))
#
# str = ','.join(str_1)
# print(str)


# Parse data

values = [cur.mogrify("(%(key)s, %(timestamp)s, now(), now(), %(CycleTime)s, %(CycleTimePallet)s, %(CycleTimeTrigger)s, %(EmptyWait)s, %(EmptyWaitTrigger)s, %(FullWait)s, %(FullWaitTrigger)s, %(GoodPallet)s, %(TotalPallet)s, %(HumidityFbk)s, %(PressureFbk)s, %(TemperatureFbk)s)", row) for row in data['rows']]

values_str = ','.join([str(v) for v in values])
print(type(values_str))
print('INSERT INTO twx.leanline_lineinfo VALUES {0}'.format(values[0]))

# cur.execute('INSERT INTO twx.leanline_lineinfo VALUES {0}'.format(str(values[0]))

cur.execute("INSERT INTO twx.leanline_lineinfo VALUES ('1.520392211387E12', TO_TIMESTAMP(1520392211387 / 1000), now(), now(), 10284, 31, false, 1001, false, 0, false, 12, 16, 53.0, 0.990318282753516, 25.88)")
conn.commit()

# End
conn.close()
cur.close()
# + values_str +
# """
#   ON CONFLICT (twx_key) DO UPDATE
#     SET twx_timestamp   = EXCLUDED.twx_timestamp,
#         updated_at      = now(),
#         cycletime       = EXCLUDED.cycletime,
#         cycletimepallet = EXCLUDED.cycletimepallet,
#         cycletimetrigger = EXCLUDED.cycletimetrigger,
#         emptywait       = EXCLUDED.emptywaittrigger,
#         fullwait        = EXCLUDED.fullwait,
#         fullwaittrigger = EXCLUDED.fullwaittrigger,
#         goodpallet      = EXCLUDED.goodpallet,
#         totalpallet     = EXCLUDED.totalpallet,
#         humidityfbk     = EXCLUDED.humidityfbk,
#         pressurefbk     = EXCLUDED.pressurefbk,
#         temperaturefbk  = EXCLUDED.temperaturefbk
# """)


# # Insert into database
# sql = "INSERT INTO twx.leanline_lineinfo VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
# sql = """
#     INSERT INTO the_table (id, column_1, column_2)
#     VALUES (1, 'A', 'X'), (2, 'B', 'Y'), (3, 'C', 'Z')
#         ON CONFLICT (id) DO UPDATE
#     SET column_1 = excluded.column_1,
#         column_2 = excluded.column_2;
# """
# conn = psycopg2.connect('host={0} dbname={1} user={2}'.format(config.db_host, config.db_name, config.db_user))
# cur = conn.cursor()
# cur.execute('SELECT * FROM job_queue')
# all = cur.fetchall()
# print(all)


