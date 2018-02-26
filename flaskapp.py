import hashlib
import random

import itertools
from flask import Flask, request, render_template, send_from_directory, json
from werkzeug.utils import secure_filename
import mysql.connector
import csv
import os
import time
import pymemcache
from pymemcache.client.base import Client

app = Flask(__name__)
UPLOAD_PATH= "/home/ubuntu/upload"
app.config['UPLOAD_PATH'] = UPLOAD_PATH

host = 'secureassgndb.c6zq9suvhyba.us-east-2.rds.amazonaws.com'
port = 3306
dbusername ='root'
dbpassword ='welcome123'
dbname ='secureAssignDB'


@app.route("/")
def index():
    return render_template('index.html')


@app.route("/runqueries")
def runqueries():
    return render_template("runqueries.html")


@app.route("/webquery")
def webquery():
    return render_template("webquery.html")


@app.route("/upload", methods = ['GET', 'POST'])
def upload():
    if request.method == 'POST':

        try:
            mysql_conn = mysql.connector.connect(host=host, user=dbusername, password=dbpassword, database=dbname,
                                             port=port)
            cursor = mysql_conn.cursor(buffered=True)
            first = True
            starttime = int(round(time.time() * 1000))

            file = request.files['file']
            filename = secure_filename(file.filename)
            file.save(os.path.join(UPLOAD_PATH, filename))

            with open(UPLOAD_PATH + '/' + filename, 'rb') as csv_file:
                reader = csv.reader(csv_file)
                columns = next(reader)
                columns = [h.strip() for h in columns]
                if first:
                    sql = 'CREATE TABLE IF NOT EXISTS Earthquake (%s)' % ','.join(['%s text' % column for column in columns])
                    print sql;
                    cursor.execute(sql)
                    first = False

                    query ='INSERT INTO Earthquake(time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place, type, horizontalError, depthError, magError, magNst, status, locationSource, magSource)' + ' VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

                    for row in reader:
                        cursor.execute(query, row)
                    endtime = int(round(time.time() * 1000))
                    totalexectime = endtime - starttime
                    cursor.close()
                    mysql_conn.commit()
                    mysql_conn.close()
                    return 'Time taken to load table : <b>' + str(totalexectime) + '</b> msecs <br>'
        except Exception as e:
                print e
                return 'Error ' + str(e)
    else:
        return render_template('upload.html', message="Upload Again")


@app.route('/execquery', methods=['GET', 'POST'])
def execquery():
    name = request.form['fieldName']
    c1 = request.form['cclause1']
    c2 = request.form['cclause2']
    withMem = request.form['withmem']

    if withMem == 'y':
        return execqueryWithMem(name, c1, c2)

    try:
        mysql_conn = mysql.connector.connect(host=host, user=dbusername, password=dbpassword, database=dbname,
                                             port=port)
        #cursor = mysql_conn.cursor(buffered=True)

        query = 'select ' + name + ' from Earthquake where ' + c1 + ' and ' + c2

        starttime = int(round(time.time() * 1000))

        for i in range(0, 250):
            cursor = mysql_conn.cursor(buffered=True)
            cursor.execute(query)
            row = cursor.fetchall()
            count = cursor.rowcount
            cursor.close()

        endtime = int(round(time.time() * 1000))
        totalexectime = endtime - starttime

        mysql_conn.close()

        resultStr = '<div style="font-size:14px;margin-top: 30px;"><div> Last Name : Kulkarni </div><div> Last 4 digit ID : 7529 </div><div> Class Section : 4:00 PM </div></div>'
        resultStr = resultStr + '<br> Time taken : ' + str(totalexectime) + ' msecs'
        resultStr = resultStr + '<br> Rows effected : ' + str(count)
        return resultStr
    except Exception as e:
        print e
        return 'Error ' + str(e)


def execqueryWithMem(name, c1, c2):
    try:
        mysql_conn = mysql.connector.connect(host=host, user=dbusername, password=dbpassword, database=dbname,
                                             port=port)
        mc = Client(('cloudassgn.hw0lsb.0001.use2.cache.amazonaws.com', 11211))

        query = 'select ' + name + ' from Earthquake where ' + c1 + ' and ' + c2
        hashVal = hashlib.sha224(query).hexdigest()

        starttime = int(round(time.time() * 1000))

        data = mc.get(hashVal)
        count = 0

        if not data:
            for i in range(0, 250):
                cursor = mysql_conn.cursor()
                cursor.execute(query)
                row = cursor.fetchall()
                count = cursor.rowcount
                cursor.close()
            mc.set(hashVal, count)

        endtime = int(round(time.time() * 1000))
        totalexectime = endtime - starttime

        mysql_conn.close()

        resultStr = '<div style="font-size:14px;margin-top: 30px;"><div> Last Name : Manakan </div><div> Last 4 digit ID : 6131 </div><div> Class Section : 10:30 AM </div></div>'
        resultStr = resultStr + '<br> Time taken : ' + str(totalexectime) + ' msecs'
        resultStr = resultStr + '<br> Rows effected : ' + str(count)
        return resultStr
    except Exception as e:
        print e
        return 'Error ' + str(e)


@app.route("/runRandomqueriesOnLargeSample/<int:qcount>")
def runRandomqueriesOnLargeSample(qcount):
    try:
        mysql_conn = mysql.connector.connect(host=host, user=dbusername, password=dbpassword, database=dbname,
                                             port=port)

        query = 'select latitude from Earthquake where place='

        starttime = int(round(time.time() * 1000))

        for i in range(1, qcount):
            cursor = mysql_conn.cursor(buffered=True)

            place = random.randint(0, qcount)
            q = query + str(place)

            cursor.execute(q)
            row = cursor.fetchall()

            cursor.close()

        endtime = int(round(time.time() * 1000))
        totalexectime = endtime - starttime

        mysql_conn.close()

        return 'Time taken : ' + str(totalexectime) + ' msecs'

    except Exception as e:
        print e
        return 'Error ' + str(e)


@app.route("/runRandomqueriesOnLargeSampleWithMemCache/<int:qcount>")
def runRandomqueriesOnLargeSampleWithMemCache(qcount):
    try:
        mysql_conn = mysql.connector.connect(host=host, user=dbusername, password=dbpassword, database=dbname,
                                             port=port)
        mc = Client(('cloudassgn.hw0lsb.0001.use2.cache.amazonaws.com', 11211))

        query = 'select latitude from Earthquake where place='

        starttime = int(round(time.time() * 1000))

        for i in range(1, qcount):

            place = str(random.randint(0, qcount))

            obj = mc.get(place)

            if not obj:
                cursor = mysql_conn.cursor()

                q = query + place

                cursor.execute(q)
                row = cursor.fetchall()

                cursor.close()

                mc.set(place, 'data')

        endtime = int(round(time.time() * 1000))
        totalexectime = endtime - starttime

        mysql_conn.close()

        return 'Time taken : ' + str(totalexectime) + ' msecs'

    except Exception as e:
        print e
        return 'Error ' + str(e)

#Displaying results


@app.route("/jsonResult", methods=['POST', 'GET'])
def jsonResult():
    query = request.form['queryText']

    hashVal = hashlib.sha224(query).hexdigest()

    try:
        mc = Client(('cloudassgn.hw0lsb.0001.use2.cache.amazonaws.com', 11211))

        data = mc.get(hashVal)

        starttime = int(round(time.time() * 1000))
        if not data:
            mysql_conn = mysql.connector.connect(host=host, user=dbusername, password=dbpassword, database=dbname,
                                                 port=port)
            cursor = mysql_conn.cursor()

            cursor.execute(query)
            endtime = int(round(time.time() * 1000))

            desc = cursor.description
            data = [dict(itertools.izip([col[0] for col in desc], row))
                    for row in cursor.fetchall()]

            mc.set(hashVal, data)
            cursor.close()
            mysql_conn.close()
        else:
            print 'key found'
            endtime = int(round(time.time() * 1000))

        totalexectime = endtime - starttime

        results = {'status': 'true', 'data': data, 'timeInmsecs': str(totalexectime)}
    except Exception as e:
        results = {'status': 'error', 'message': str(e)}

    return json.dumps(results)


@app.route("/clearcache")
def clearcache():
    mc = Client(('cloudassgn.hw0lsb.0001.use2.cache.amazonaws.com', 11211))
    mc.flush_all()

    return 'Flushed the cache...'


@app.route("/Modifytable")
def modifytable():
    try:
        mysql_conn = mysql.connector.connect(host=host, user=dbusername, password=dbpassword, database=dbname,
                                             port=port)

        query = "update Earthquake set nst=44 where id='ak18479532'"
        starttime = int(round(time.time() * 1000))
        cursor = mysql_conn.cursor()
        cursor.execute(query)
        upCount = cursor.rowcount
        cursor.close()

        query = "DELETE FROM Earthquake WHERE place = '14km W of Mojave, CA'"

        cursor = mysql_conn.cursor()
        cursor.execute(query)
        delCount = cursor.rowcount
        endtime = int(round(time.time() * 1000))
        totalexectime = endtime - starttime

        cursor.close()

        mysql_conn.commit()

        mysql_conn.close()

        resultStr = 'Time taken to change tuples (' + str(upCount) + ') and remove tuples (' + str(
            delCount) + ') : <b>' + str(totalexectime) + '</b> msecs '
    except Exception as e:
        return str(e)

    return resultStr

if __name__ == '__main__':
  app.run()

