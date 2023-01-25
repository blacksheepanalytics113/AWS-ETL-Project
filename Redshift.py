#Connect to the cluster and create a Cursor
import redshift_connector
import psycopg2
print('Connecting to the AWSRedshift database...')
print('Database Connected!!')
try:
    with redshift_connector.connect( host=int(input('XXXXXXXXXXXXXXXXXXX')),
            database=int(input('XXXX')),
            port=int(input('XXXXXX')),
            user=int(input('XXXXX')),
            password=int(input('XXXXXXXXXX')))as conn:
        with conn.cursor() as cur:
            # Create a Cursor object
            cur = conn.cursor() 
            
          # Create an empty table
        cur.execute("create table category (catid int, cargroup varchar(255), catname varchar(255), catdesc varchar(11))")
        conn.commit()
            
        # close the communication with the PostgreSQL
        cur.close()
except (Exception, redshift_connector.DatabaseError) as error:
        print(error)
finally:
    if conn is not None:
        conn.close()
        print('Database connection closed.')
            

# #Use COPY to copy the contents of the S3 bucket into the empty table 
cur.execute("copy category from 's3://testing/category_csv.txt' iam_role 'arn:aws:iam::123:role/RedshiftCopyUnload' csv;")

#Retrieve the contents of the table
cur.execute("select * from category")
print(cur.fetchall())

#Use UNLOAD to copy the contents of the table into the S3 bucket
cur.execute("unload ('select * from category') to 's3://testing/unloaded_category_csv.txt'  iam_role 'arn:aws:iam::123:role/RedshiftCopyUnload' csv;")

#Retrieve the contents of the bucket
print(cur.fetchall())
