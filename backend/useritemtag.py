# Stores two tables, one for album tags and one for song tags.
import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'useritemtag'

TABLES = {}
# Every time a user tags an album, the user, album, and tag are stored.
TABLES['album_tags'] = (
  "CREATE TABLE album_tags( "
  "user varchar(20) NOT NULL, "
  "item varchar(100) NOT NULL, "
  "tag varchar(20) NOT NULL ); ")

# Same as above, only with songs, not albums.
TABLES['song_tags'] = (
  "CREATE TABLE song_tags( "
  "user varchar(20) NOT NULL, "
  "item varchar(100) NOT NULL, "
  "tag varchar(20) NOT NULL ); ")

# Connects to mysql, defines a method for creating the database, and uses that method.
cnx = mysql.connector.connect(user='root', password='Reverie42')
cursor = cnx.cursor()

def create_database(cursor):
  try:
    cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
  except mysql.connector.Error as err:
     print "Failed creating database: {}".format(err)
     exit(1)

try:
    cnx.database = DB_NAME  
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        cnx.database = DB_NAME
    else:
        print err
        exit(1)

for name, ddl in TABLES.iteritems():
    try:
        print "Creating table {}: ".format(name)
        cursor.execute(ddl)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print "already exists."
        else:
            print err.msg
    else:
        print "OK"

cnx.commit()
cursor.close()
cnx.close()

# Allows users to update either tag table.
def update_tags(tablename, user, item, tag):
  cnx = mysql.connector.connect(user='root', password='Reverie42', buffered=True)
  cursor = cnx.cursor()
  cnx.database = DB_NAME
  
  check_exists = ("SELECT EXISTS(SELECT 1 FROM "
                  "%s WHERE user = %s "
                  "AND item = %s AND tag = %s); ")
  
  add_tag = ("INSERT INTO %s "
             "(user, item, tag) "
             "VALUES (%s, %s, %s); ")
  
  data = (tablename, user, item, tag)
  
  cursor.execute(check_exists, data)
  for i in cursor:
    exists = i[0]
  if exists == 0:
    cursor.execute(add_tag, data)
  
  cnx.commit()
  cursor.close()
  cnx.close()
  
# Reads either tag table into a dictionary.
def read_tags(tablename):
  cnx = mysql.connector.connect(user='root', password='Reverie42', buffered=True)
  cursor = cnx.cursor()
  cnx.database = DB_NAME
  
  tag_dict = {}
  
  cursor.execute("SELECT * FROM %s; " % (tablename))
  for i in cursor:
    if i[1] in tag_dict:
      if i[2] in tag_dict[i[1]]:
        tag_dict[i[1]][i[2]] += 1
      else:
        tag_dict[i[1]][i[2]] = 1
    else:
      tag_dict[i[1]] = {}
      tag_dict[i[1]][i[2]] = 1
  
  cnx.commit()
  cursor.close()
  cnx.close()
  
  return tag_dict
