#!/usr/bin/python

import re

def get_type(type_name):
        s = type_name.lower()
        types = {
                "varchar2": "text",
                "date": "timestamp",
                "number": "int",
                "clob": "text",
                "char": "text"
        }
        return types[s]

f = open("./MASTER.sql", 'r')
data = f.read()

tablespace = re.compile("(TABLESPACE[ ]MASTER.*?;)", re.S|re.M|re.I)
text = tablespace.sub(';', data)
tables_text = text.split(';')
table_text = []
for table in tables_text:
        table_text.append(table.strip())

# TABLE Name
tablename = re.compile("CREATE[ ]TABLE[ ](.*)", re.I)
columnname = re.compile("^\s*([A-Za-z_0-9]+)\s+(VARCHAR2|DATE|NUMBER|CLOB|CHAR)", re.I|re.M|re.S)
create_script = {}
for table in table_text:
        t = tablename.match(table)
        if t:
                table_name = t.group(1)
                create_script[table_name] = {}

                columninfo = columnname.findall(table)
                column_array = []
                for c in columninfo:
                        column_name = c[0]
                        column_type = c[1]
                        column_array.append((column_name, column_type))
                        create_script[table_name] = column_array


for t in create_script:
        print "CREATE EXTERNAL TABLE %s" % t
        print "("
        i = 1
        for c in create_script[t]:
                delimiter = ","
                if i == len(create_script[t]):
                        delimiter = ""
                print "\t%s %s%s" % (c[0], get_type(c[1]).upper(), delimiter)
                i = i + 1
        print ")"
        print "USING TEXT WITH('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.GzipCodec')"
        print "LOCATION 'hdfs://name:54310/user/hadoopuser/databases/oracle/current/MASTER.%s';" % t
        print ""
        print ""
