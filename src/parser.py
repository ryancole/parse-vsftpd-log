#!/usr/bin/python

import sys, time, datetime, logging
import psycopg2


# the database connection is constant through this script
db = None


def parse_line(line):
    
    split_on_comma = line.split(',')
    
    # extract the date information
    date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(split_on_comma[0].split('[')[0].strip())))
    
    # extract the user information
    username = split_on_comma[0].split('[')[2].split(']')[0].strip()
    
    # extract the ip address
    ip = split_on_comma[0].split('"')[1]
    
    # extract the path information
    path = split_on_comma[1].strip(' "')
    
    # extract the size of the upload
    size = int(split_on_comma[2].split()[0].strip())
    
    # extract the speed information
    speed = split_on_comma[3].strip()
    
    return { 'date': date, 'username': username, 'ip': ip, 'path': path, 'size': size, 'speed': speed }


def save_entry(details):
    
    logging.info('[%s] Writing log entry for %s, uploaded on %s' % (datetime.datetime.now(), details['path'], details['date']))
    
    # database cursor
    cursor = db.cursor()
    
    # write the data to the database
    cursor.execute('INSERT INTO vsftplogs (date, username, ip, path, size, speed) VALUES (%(date)s, %(username)s, %(ip)s, %(path)s, %(size)s, %(speed)s);', details)
    
    # commit changes
    db.commit()
    
    # close the cursor
    cursor.close()


def scan_file(path):
    
    # get the offset so we can skip all older items
    offset = get_offset()
    
    # iterate over each line in the log file
    for line in open(path, 'r'):
        
        # check to see if this line might be a file upload entry
        if "ok upload:" in line.lower():
            
            # extract details from the raw log file line
            details = parse_line(line)
            
            # go to the next log entry if this one is older than the offset
            if details['date'] <= offset[0]:
                continue
            
            # save the log entry to the database
            save_entry(details)


def get_offset():
    
    cursor = db.cursor()
    
    # get the newest log entry so that we can compare against it
    cursor.execute('SELECT date FROM vsftplogs ORDER BY id DESC LIMIT 1')
    
    # read it in from the cursor
    offset = cursor.fetchone()
    
    cursor.close()
    
    # provide the query result
    return offset


if __name__ == '__main__':

    if len(sys.argv) <> 3:
        
        sys.exit('Usage: python parser.py <connection-string> <logfile>')
    
    # initialize the logger
    logging.basicConfig(filename='/var/log/vsftpd-upload-parser.log', level=logging.DEBUG)
    
    # connect to the database
    db = psycopg2.connect(sys.argv[1])
    
    # begin the file scan
    scan_file(sys.argv[2])
    
    # close the database connection
    db.close()
