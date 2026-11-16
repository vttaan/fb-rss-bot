import sqlite3 as sq;

con = sq.connect("rss_bot.db")
cursor = con.cursor()

cursor.execute("SELECT content from posts")

list = cursor.fetchall()

for item in list:
    print("item: ",item,'\n')

con.close()