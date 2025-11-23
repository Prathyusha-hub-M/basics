import mysql.connector
from mysql.connector import Error

#Database details DBforge
db_host = 'cctaxappeal.com'
db_port = 3306
db_username ='cctaxappeal_eesha'
db_password = 'SuccessTeam2026!'
db_name = 'cctaxappeal_ccta'

def test_mysql_connection():
    connection = None 
    try:
        connection = mysql.connector.connect(
            host=db_host,
            port=db_port,
            username=db_username,
            password=db_password,
            database = db_name,
        )

        if connection.is_connected():
            db_info = connection.get_server_info()
            print("connection successfull")
            print(f"connect to Mysql Server version : {db_info}")
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print(f"you are connected to the databse: {record[0]}")

    except Error as e:
         print(f"❌ Error connecting to MySQL database: {e}")
         print("\nPlease check the following:")
         print(f"- Host: {db_host}, Port: {db_port}")
         print("- Username/Password are correct.")
         print("- The MySQL server allows remote connections from your machine.")
         print("- Any network firewalls are not blocking port 3306.")

    finally:
        if connection is not None and connection.is_connected():
            connection.close()
            print("Connection Closed")

if __name__== "__main__":
    test_mysql_connection()