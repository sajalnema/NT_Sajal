# import mysql.connector as connector 

# con=connector.connect(host='localhost',port='3306',user='root',password='khushivyas2407',database='pythontest')

# print(con)



import mysql.connector as mc
con_obj = mc.connect(host="localhost",port='3306', user="root", passwd="khushivyas2407")
print(con_obj)

cur_obj = con_obj.cursor()
print(cur_obj)
try:
    # cur_obj.execute("CREATE DATABASE PYTHON_DB")
    cur_obj.execute("SHOW DATABASES")
except:
    con_obj.rollback()

for x in cur_obj:
    print(x)

con_obj.close()
    