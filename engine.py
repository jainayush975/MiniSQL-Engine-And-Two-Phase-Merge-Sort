import sys
import re
import sqlparse
from database import Database, Table

db = Database("comp","metadata.txt")

def joinTable(from_tables):
    if len(from_tables)>1:
        obj1 = db.tables[from_tables[0]]
        obj2 = db.tables[from_tables[1]]
        columns = (obj1.columns)+(obj2.columns)
        obj3 = Table("result", columns)
        data=[]
        for row1 in obj1.data:
            for row2 in obj2.data:
                data.append(row1+row2)
        obj3.update_data(data)
    elif len(from_tables) == 1:
        obj3 = db.tables[from_tables[0]]
    else:
        print "This Engine Don't support operation on more than 2 tables"
        exit()
    return obj3

def printer(to_print, data_table):
    for i in range(0, len(to_print)):
        if(to_print[i]):
            print data_table.columns[i],
    print
    for row in data_table.data:
        for i in range(0, len(to_print)):
            if(to_print[i]):
                print row[i],
        print

def projector(to_show, data_table):
    l=len(data_table.columns)
    to_print = []
    for i in range(0,l):
        to_print.append(False)

    if 'MIN' in to_show[0] or 'MAX' in to_show[0] or 'SUM' in to_show[0] or 'AVG' in to_show[0] or 'DISTINCT' in to_show[0]:
        l = re.split(r'[\ \(\)]+',to_show[0])
        ind = (data_table.columns).index(l[1])

        values = []
        for row in data_table.data:
            values.append(int(row[ind]))

        print l[1]
        if(l[0] == 'MAX'):
        	print max(values)
        elif(l[0] == 'MIN'):
        	print min(values)
        elif(l[0] == 'SUM'):
        	print reduce(lambda x,y:x+y,values)
        elif(l[0] == 'AVG'):
        	print reduce(lambda x,y:x+y,values)/float(len(values))
        elif(l[0] == 'DISTINCT'):
        	newValues = list(set(values))
        	for i in newValues:
        		print i

    else:
        if to_show[0] == '*':
            for i in range(0,l):
                to_print[i]=True
        else:
            for col in to_show:
                if col not in data_table.columns:
                    print "Given Table does not contain ", col
                    exit()
                else:
                    to_print[(data_table.columns).index(col)] = True

        printer(to_print, data_table)

def formNewData(data_table, command, data):
    colmns = re.split(r'[=><]+', command)
    colmns[1] = int(colmns[1])
    ind = (data_table.columns).index(colmns[0])
    new_data = []
    if '>=' in command:
        for row in data:
            if int(row[ind])>=colmns[1]:
                new_data.append(row)
    elif '<=' in command:
        for row in data:
            if int(row[ind])<=colmns[1]:
                new_data.append(row)
    elif '>' in command:
        for row in data:
            if int(row[ind])>colmns[1]:
                new_data.append(row)
    elif '<' in command:
        for row in data:
            if int(row[ind])<colmns[1]:
                new_data.append(row)
    elif '=' in command:
        for row in data:
            if int(row[ind])==colmns[1]:
                new_data.append(row)
    else:
        print "Invalid Command !!!"
        exit()
    return new_data

def formNewData1(data_table, command, data):
    colmns = re.split(r'[=><]+', command)
    ind1 = (data_table.columns).index(colmns[0])
    ind2 = (data_table.columns).index(colmns[1])
    new_data = []
    if '>=' in command:
        for row in data:
            if int(row[ind1])>=int(row[ind2]):
                new_data.append(row)
    elif '<=' in command:
        for row in data:
            if int(row[ind1])<=int(row[ind2]):
                new_data.append(row)
    elif '>' in command:
        for row in data:
            if int(row[ind1])>int(row[ind2]):
                new_data.append(row)
    elif '<' in command:
        for row in data:
            if int(row[ind1])<int(row[ind2]):
                new_data.append(row)
    elif '=' in command:
        for row in data:
            if int(row[ind1])==int(row[ind2]):
                new_data.append(row)
    else:
        print "Invalid Command !!!"
        exit()
    return new_data

def handelWhere(data_table, command):
    command = re.split(r'[\ ]+', command)
    if len(command)==4:
        data1 = data_table.data
        data2 = data_table.data
        data1=formNewData(data_table, command[1], data1)
        data2=formNewData(data_table, command[3], data2)

        if command[2] == 'AND' or command[2] == 'and':
            finaldata = [row for row in data1 if row in data2]
            data_table.data = finaldata
        elif command[2] == 'OR' or command[2] == 'or':
            finaldata = []
            for row in data1:
                if row not in finaldata:
                    finaldata.append(row)
            for row in data2:
                if row not in finaldata:
                    finaldata.append(row)
            data_table.data = finaldata

    elif len(command) == 2:
        tbls = re.split(r'[=.]+',command[1])
        obj=db.tables[tbls[0]]
        new_clm1 = obj.columns
        for clm in range(len(new_clm1)):
            new_clm1[clm] = tbls[0] + '.' + new_clm1[clm]
        obj=db.tables[tbls[2]]
        new_clm2 = obj.columns
        for clm in range(len(new_clm2)):
            new_clm2[clm] = tbls[2] + '.' + new_clm2[clm]
        data_table.columns = new_clm1 + new_clm2
        finaldata = data_table.data
        data_table.data = formNewData1(data_table, command[1], finaldata)
    else:
        print "INVALID SYNTAX !!!!!"
        exit()

def selectProcessor(commands):

    from_tables = commands[3]
    from_tables = re.split(r'[\ \t,]+',from_tables)
    data_table = joinTable(from_tables)

    if len(commands)>4 and ('WHERE' in commands[4] or 'where' in  commands[4]):
        handelWhere(data_table, commands[4])
    to_show = commands[1]
    to_show = re.split(r'[\ \t,]+',to_show)

    projector(to_show, data_table)



def queryProcessor(query):
    pQuery = sqlparse.parse(query)[0].tokens
    commands = []
    lst = sqlparse.sql.IdentifierList(pQuery).get_identifiers()

    for command in lst:
    	commands.append(str(command))
    if commands[0] == 'SELECT' or commands[0] == 'select':
        selectProcessor(commands)
    else:
        print "This query is not supported by this engine"


def main():
    db.readMetaData()
    db.populateDB()
    queryProcessor(sys.argv[1])

if __name__ == "__main__":
    main()
