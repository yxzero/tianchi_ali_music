'''
import datetime
artist = {}
k = 10000
fsw = open('mars_tianchi_artist_plays_predict.csv','w')
fs = open("./month/mars_tianchi_songs.csv", 'r')
for i in fs.readlines():
    line = i.strip().split(',')
    bd = datetime.datetime.strptime('20150901','%Y%m%d')
    if line[1] not in artist:
        artist[line[1]] = None
        while(bd <= datetime.datetime.strptime('20151030','%Y%m%d')):
            fsw.write(line[1]+','+str(k)+','+bd.strftime('%Y%m%d')+'\n')
            bd+=datetime.timedelta(days=1)
        k+=1
fs.close()
fsw.close()
'''
fs = open("../../libsvm-3.21/result.txt")
f = 0.0
for i in fs.readlines():
    f += float(i)
fs.close()
print f

