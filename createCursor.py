import psycopg2


def makeConnection():
    return psycopg2.connect(dbname='DE5_DATABASE', user='postgres',
                            password='postgres',  host="localhost", port="5433")
