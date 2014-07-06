readFile = open("part-00000")
line = readFile.readline()

writeFile = open('result.arff', 'w')
errorFile = open('error.csv', 'w')

try:
    #writeFile.write('cport,sport,length,parameterNumber,hasAgent,visitTime,width,getRate,staticPageRate,isAttack' + '\n')
    writeFile.write('@RELATION BIG_DATA\n\n@ATTRIBUTE cport NUMERIC\n@ATTRIBUTE sport NUMERIC\n@ATTRIBUTE length NUMERIC\n@ATTRIBUTE parameterNumber NUMERIC\n@ATTRIBUTE hasAgent {0,1}\n@ATTRIBUTE visitTime NUMERIC\n@ATTRIBUTE width NUMERIC\n@ATTRIBUTE getRate NUMERIC\n@ATTRIBUTE staticPageRate NUMERIC\n@ATTRIBUTE isAttack {0,1}\n\n@DATA\n')
    while line:
        temp = line.strip('\n').split(',')
        if(len(temp) == 20):
            time = temp[0]
            gp = temp[1]
            site = temp[2]
            url = temp[3]
            Type = temp[4]
            size = temp[5]
            referer = temp[6]
            cip = temp[7]
            cport = temp[8]
            sip = temp[9]
            sport = temp[10]
            agent = temp[11]
            isAttack = temp[12]
            length = temp[13]
            parameterNumber = temp[14]
            hasAgent = temp[15]
            visitTime = temp[16]
            width = temp[17]
            getRate = temp[18]
            staticPageRate = temp[19]
            
            tempResult = cport + ',' + sport + ',' + length + ',' + parameterNumber + ',' + hasAgent + ',' + visitTime + ',' + width + ',' + getRate + ',' + staticPageRate + ',' + isAttack
            writeFile.write(tempResult + '\n')
            line = readFile.readline()
        else:
            errorFile.write(line)
            line = readFile.readline()
finally:
    writeFile.close()
    errorFile.close()
    readFile.close()
