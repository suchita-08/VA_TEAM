from influxdb import InfluxDBClient

def main():
    """Instantiate a connection to the InfluxDB."""
    host = 'localhost'
    port = 8086
    user = ''
    password = ''
    dbname = 'TestDB'

    query = 'SELECT * FROM "Trade";'
   
    client = InfluxDBClient(host, port, user, password, dbname)

    print("Querying data: " + query)
    result = client.query(query)

    for point in result.get_points():
        print(point)


if __name__ == '__main__':
    main()