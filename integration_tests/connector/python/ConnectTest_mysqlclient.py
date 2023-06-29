import MySQLdb
import sys
import platform

kwargs = {
  "host": sys.argv[1],
  "port": 4000,
  "user": sys.argv[2],
  "password": sys.argv[3],
  "database": "test",
  "ssl": {
    "ca": sys.argv[4]
  }
}
if platform.system() != 'Windows':
  kwargs["ssl_mode"] = "VERIFY_IDENTITY"

connection = MySQLdb.connect(**kwargs)

with connection:
  with connection.cursor() as cursor:
    cursor.execute("SELECT DATABASE();")
    m = cursor.fetchone()
    print(m[0])