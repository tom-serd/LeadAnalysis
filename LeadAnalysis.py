from pyspark import SparkConf, SparkContext
from datetime import date

conf = SparkConf().setMaster("local").setAppName("LeadAnalysis")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    qtype = fields[0]
    region = fields[3]
    times = fields [8]
    return (qtype,region,times)

def parseDate(line):
    fields = line.split(',')
    qtype = fields[0]
    year = fields[5]
    month = fields[6]
    day = fields [7]
    return (qtype,year,month,day)

def num(line):
    line  = list(line)
    
    for i in range(0,3):
        if line[i] < 10:
            line[i] = str("0"+str(line[i]))
        else:
            line[i] = str(line[i])
    
    return (line[0],line[1],line[2],line[3])

lines = sc.textFile("file:///SparkPrograms/taskdata.csv")
parsedLines = lines.map(parseLine)
parsedDates = lines.map(parseDate) 

ontariolp = parsedLines.filter(lambda x: "lead" in x[0])
ontariolp = ontariolp.filter(lambda x: "ON" in x[1])


datesnum = parsedDates.filter(lambda x: "lead" in x[0]).map(lambda x: date(int(x[1]),int(x[2]),int(x[3])))
totaldates = datesnum.collect()
for i in totaldates:
    print(i)

daysnum = totaldates[len(totaldates)-1] - totaldates[0]
daysnum = daysnum.days
print(daysnum)

leadtimes = ontariolp.map(lambda x: (x[2]))
leadtimes = leadtimes.map(lambda x: "".join(x)).map(lambda x: x.split(" ")).map(lambda x: (x[0].split(":"),x[1]))
leadtimes = leadtimes.map(lambda x: (int(x[0][0]),int(x[0][1]),int(x[0][2]),str(x[1])))
leadtimes = leadtimes.map(num)

leadtest = leadtimes.first()
for i in leadtest:
    print(i)
    
timesonl = []

ampm = "AM"

for h in range(7,23):
    hour = h
    if h > 12:
        hour = h-12
        ampm = "PM"
    elif h == 12:
        ampm = "PM"
    else:
        ampm = "AM"
    
    if hour < 10:
        hour = str("0"+str(hour))
    else:
        hour = str(hour)
        
    rdd = leadtimes.filter(lambda x: ampm == x[3]).filter(lambda x: hour == x[0])
    counting = rdd.collect()
    timesonl.append(len(counting))

rdd = leadtimes.filter(lambda x: "PM" == x[3]).filter(lambda x: "11" == x[0]).filter(lambda x: "00" == x[1])
timesonl[0] = timesonl[0] + len(rdd.collect())

for i in range(31,59):
    minute = str(i)
    rdd = leadtimes.filter(lambda x: "AM" == x[3]).filter(lambda x: "06" == x[0]).filter(lambda x: minute == x[1])
    timesonl[0] = timesonl[0] + len(rdd.collect())



a = 7
b = 8
daysnum = float(daysnum)
for i in timesonl:
    if b > 12:
        b = b - 12
        ampm = "PM"
    elif a > 12:
        a = a - 12
        ampm = "PM"
    elif a == 12 or b == 12:
        ampm = "PM"
    i = float(i)
    print("{} to {} {}: {:.0f} leads {:.5f} on average per day".format(a,b,ampm,i,float(i/daysnum)))
    a+=1
    b+=1