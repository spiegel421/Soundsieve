# Stores two tables, one for album ratings and one for song ratings.
import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'useritemrating'

TABLES = {}
# Every time a user rates an album, the user, album, and rating are stored.
TABLES['album_ratings'] = (
  "CREATE TABLE album_ratings( "
  "user varchar(20) NOT NULL, "
  "item varchar(100) NOT NULL, "
  "rating int(2) NOT NULL ); ")

# Same as above, only with songs, not albums.
TABLES['song_ratings'] = (
  "CREATE TABLE song_ratings( "
  "user varchar(20) NOT NULL, "
  "item varchar(100) NOT NULL, "
  "rating int(2) NOT NULL ); ")

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
  
# Allows users to update either rating table.
def update_album_ratings(tablename, user, item, rating):
  cnx = mysql.connector.connect(user='root', password='Reverie42', buffered=True)
  cursor = cnx.cursor()
  cnx.database = DB_NAME
  
  check_rating_exists = ("SELECT EXISTS(SELECT * FROM "
                  "%s WHERE user = %s "
                  "AND item = %s); ")
  
  check_rating_same = ("SELECT EXISTS(SELECT * FROM "
                      "%s WHERE user = %s "
                      "AND item = %s AND rating = %s); ")
  
  add_rating = ("INSERT INTO %s "
             "(user, item, rating) "
             "VALUES (%s, %s, %s); ")
  
  change_rating = ("UPDATE %s "
                         "SET rating = %s "
                         "WHERE user = %s AND item = %s; ")
  
  data = (tablename, user, item, rating)
  data_short = (tablename, user, item)
  data_change = (tablename, rating, user, item)
  
  cursor.execute(check_rating_exists, data_short)
  for i in cursor:
    exists = i[0]
  if exists == 0:
    cursor.execute(add_rating, data)
  else:
    cursor.execute(check_rating_same, data)
    for i in cursor:
      same = i[0]
    if same == 0:
      cursor.execute(change_rating, data_change)
  
  cnx.commit()
  cursor.close()
  cnx.close()
  
# Reads either rating table into a dictionary.
def read_ratings(tablename):
  cnx = mysql.connector.connect(user='root', password='Reverie42', buffered=True)
  cursor = cnx.cursor()
  cnx.database = DB_NAME
  
  rating_dict = {}
  
  cursor.execute("SELECT * FROM %s; " % (tablename))
  for i in cursor:
    if i[0] not in rating_dict:
      rating_dict[i[0]] = {}
    rating_dict[i[0]][i[1]] = i[2]
    
  cnx.commit()
  cursor.close()
  cnx.close()
  
  return rating_dict
  
# Allows users to update the song ratings table.
def update_song_ratings(user, song, rating):
  cnx = mysql.connector.connect(user='root', password='Reverie42', buffered=True)
  cursor = cnx.cursor()
  cnx.database = DB_NAME
  
  check_rating_exists = ("SELECT EXISTS(SELECT * FROM "
                  "song_ratings WHERE user = %s "
                  "AND song = %s); ")
  
  check_rating_same = ("SELECT EXISTS(SELECT * FROM "
                      "song_ratings WHERE user = %s "
                      "AND song = %s AND rating = %s); ")
  
  add_song_rating = ("INSERT INTO song_ratings "
             "(user, song, rating) "
             "VALUES (%s, %s, %s); ")
  
  change_song_rating = ("UPDATE song_ratings "
                         "SET rating = %s "
                         "WHERE user = %s AND song = %s; ")
  
  data = (user, song, rating)
  data_short = (user, song)
  data_change = (rating, user, song)
  
  cursor.execute(check_rating_exists, data_short)
  for item in cursor:
    exists = item[0]
  if exists == 0:
    cursor.execute(add_song_rating, data)
  else:
    cursor.execute(check_rating_same, data)
    for item in cursor:
      same = item[0]
    if same == 0:
      cursor.execute(change_song_rating, data_change)
  
  cnx.commit()
  cursor.close()
  cnx.close()
