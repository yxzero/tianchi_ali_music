# -*- coding:utf-8 -*-
'''
Created on 2016��6��8��

@author: cs
'''
import datetime
import numpy as np
import matplotlib.pyplot as plt
import xgboost as xgb
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
song_user = {}
song_1 = {}


def RF_regreesion(ignore=120, before_day=8):
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.externals import joblib
    from sklearn.ensemble import AdaBoostRegressor
    from sklearn.tree import DecisionTreeRegressor
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn import linear_model
    song_3day = np.load("song_3day.npz")['arr_0'][()]
    song_5day = np.load("song_5day.npz")['arr_0'][()]
    song_10day = np.load("song_10day.npz")['arr_0'][()]
    song_15day = np.load("song_15day.npz")['arr_0'][()]
    song_20day = np.load("song_20day.npz")['arr_0'][()]
    song_1day = np.load("song_1.npz")['arr_0'][()]
    song_id = np.load("song_id.npz")['arr_0'][()]
    song_like = np.load("song_3like.npz")['arr_0'][()]
    song_down = np.load("song_3down.npz")['arr_0'][()]
    song2artict = np.load("song2artict.npz")['arr_0'][()]
    X = []
    Y = []
    logging.info("data prepare...")
    for song_i in song_id.keys():
        if np.mean(np.array(song_1day[song_i])) < ignore:
            continue
        song_k = song_id[song_i]
        for i in range(70, 153):
            #xtemp = [song_k]
            if i < 213-song2artict[song_i][2]:
                continue
            xtemp = song2artict[song_i][1:5]
            xtemp.append(i%7)
            for like_i in range(1,153,10):
                xtemp.append(song_like[song_i][like_i])
                xtemp.append(song_down[song_i][like_i])
            next1 = song_5day[song_i][1]
            '''
            for like_i in range(5,70,5):
                xtemp.append(song_5day[song_i][like_i] - next1)
                next1 = song_5day[song_i][like_i]
                if like_i > 5:
                    xtemp.append(song_5day[song_i][like_i]-song_5day[song_i][like_i-10])
                if song_5day[song_i][like_i] == 0:
                    song_5day[song_i][like_i] = 1
                xtemp.append(1.0*song_like[song_i][like_i]/song_5day[song_i][like_i])
            '''
            mines = 0
            for i in range(1,7):
                mines += (song_1day[song_i][i-7*i] - song_1day[song_i][i-8*i])
            xtemp.append(mines)
            xtemp.append(song_1day[song_i][i-1] + mines/6.0)
            xtemp.append(song_1day[song_i][i-1]-song_1day[song_i][i-2])
            xtemp.append(song_3day[song_i][i-1])
            xtemp.append(song_5day[song_i][i-3])
            xtemp.append(song_10day[song_i][i-5])
            #xtemp.append(song_15day[song_i][i-3])
            xtemp.append(song_20day[song_i][i-7])
            kk = 0
            xnext1 = song_1day[song_i][i-7]
            for xday in range(before_day):
                xnext = song_1day[song_i][i-7*(xday+2)]
                xtemp.append(xnext1-xnext)
                xtemp.append(xnext1)
                kk += xnext1-xnext
                xnext1 = xnext
            kk /= (1.0*before_day)
            xtemp.append(kk)
            for xday in range(before_day):
                xtemp.append(song_1day[song_i][i-7*(xday+2)]+kk*(xday+2))
            X.append(xtemp)
            Y.append(song_1day[song_i][i])
    logging.info("train model...")
    rf = RandomForestRegressor()
    rf.fit(X,Y)
    '''
    adaregr = AdaBoostRegressor(DecisionTreeRegressor(max_depth=4),
            n_estimators=300, random_state=np.random.RandomState(1))
    
    adaregr = GradientBoostingRegressor(n_estimators=300,
            learning_rate=0.1,max_depth=2, random_state=np.random.RandomState(1))
    adaregr.fit(X, Y)
    '''
    #xgmat = xgb.DMatrix(X,Y)
    #params = {"objective": "reg:linear"}
    #gbm = xgb.train(dtrain=xgmat, params=params)
    #logging.info("save model")
    joblib.dump(rf, './rf/rf.pkl')
    #joblib.dump(adaregr, './Ada/ada.pkl')
    #gbm.save_model('rbx.model')

def predict(ignore=120, days=30, before_day=8, predict_day=30):
    from sklearn.externals import joblib
    from sklearn.ensemble import RandomForestRegressor
    logging.info("load model and data")
    #rf = xgb.Booster({'nthread':4})
    #rf.load_model('rbx.model')
    rf = joblib.load('./rf/rf.pkl')
    #rf = joblib.load('./Ada/ada.pkl')
    song_1day = np.load("song_1.npz")['arr_0'][()]
    song_20day = np.load("song_20day.npz")['arr_0'][()]
    song_3day = np.load("song_3day.npz")['arr_0'][()]
    song_5day = np.load("song_5day.npz")['arr_0'][()]
    song_id = np.load("song_id.npz")['arr_0'][()]
    song_like = np.load("song_3like.npz")['arr_0'][()]
    song_down = np.load("song_3down.npz")['arr_0'][()]
    song2artict = np.load("song2artict.npz")['arr_0'][()]
    predect_song = {}
    for song_i in song_1day.keys():
        target = song_1day[song_i]
        song_1day[song_i] = song_1day[song_i][0:len(song_1day[song_i])-predict_day]
        song_3day[song_i] = song_3day[song_i][0:len(song_3day[song_i])-predict_day]
        predect_song[song_i] = []
        if np.mean(np.array(song_1day[song_i])) < ignore:
            for i in range(predict_day):
                j = len(song_1day[song_i])
                predect_y = sum(song_1day[song_i][j-20:j])/20.0
                predect_song[song_i].append(predect_y)
                song_1day[song_i].append(predect_y)
            continue
        for i in range(days):
            j = len(song_1day[song_i])
            #tempx = [song_id[song_i]]
            tempx = song2artict[song_i][1:5]
            tempx.append(len(song_1day)%7)
            for like_i in range(1,153,10):
                tempx.append(song_like[song_i][like_i])
                tempx.append(song_down[song_i][like_i])
            next1 = song_5day[song_i][1]
            '''
            for like_i in range(5,70,5):
                tempx.append(song_5day[song_i][like_i] - next1)
                next1 = song_5day[song_i][like_i]
                #tempx.append(song_5day[song_i][like_i])
                if like_i > 5:
                    tempx.append(song_5day[song_i][like_i]-song_5day[song_i][like_i-10])
                if song_5day[song_i][like_i] == 0:
                    song_5day[song_i][like_i] = 1
                tempx.append(1.0*song_like[song_i][like_i]/song_5day[song_i][like_i])
            '''
            mines = 0
            for i in range(1,7):
                mines += (song_1day[song_i][i-7*i] - song_1day[song_i][i-8*i])
            tempx.append(mines)
            tempx.append(song_1day[song_i][i-1] + mines/6.0)
            tempx.append(song_1day[song_i][-1] - song_1day[song_i][-2])
            tempx.append(sum(song_1day[song_i][j-4:j-1])/3.0)    
            tempx.append(sum(song_1day[song_i][j-8:j-3])/5.0)
            tempx.append(sum(song_1day[song_i][j-15:j-5])/10.0)
            #tempx.append(sum(song_1day[song_i][j-18:j-3])/15.0)
            tempx.append(sum(song_1day[song_i][j-27:j-7])/20.0)
            i_p = j-1
            xnext1 = song_1day[song_i][i_p-7]
            kk = 0
            for xday in range(before_day):
                xnext = song_1day[song_i][i_p-7*(xday+2)]
                tempx.append(xnext1-xnext)
                kk += xnext1-xnext
                tempx.append(xnext1)
                xnext1 = xnext
            kk /= (1.0*before_day)
            tempx.append(kk)
            for xday in range(before_day):
                tempx.append(song_1day[song_i][i_p-7*(xday+2)]+kk*(xday+2)) 
            #print tempx
            predect_y = rf.predict([tempx])
            #predect_y = rf.predict(xgb.DMatrix(np.array([tempx])))
            #predect_y += (song_1day[song_i][-1] + song_1day[song_i][-2])
            #predect_y += song_1day[song_i][-1]
            #predect_y = predect_y/2.0
            predect_song[song_i].append(predect_y[0])
            song_1day[song_i].append(predect_y[0])
            song_3day[song_i].append(sum(song_1day[song_i][j-2:j+1])/3.0)
        for i in range(predict_day-days):
            j = len(song_1day[song_i])
            predect_y = sum(song_1day[song_i][j-20:j])/20.0
            predect_song[song_i].append(predect_y)
            song_1day[song_i].append(predect_y)
        '''
        plt.plot(range(len(song_1day[song_i])), song_1day[song_i])
        plt.plot([183,183],[0,max(song_1day[song_i])])
        '''
        '''
        plt.plot(range(len(song_1day[song_i])), song_1day[song_i])
        plt.plot(range(len(song_1day[song_i])), target)
        plt.plot([153,153],[0,max(song_1day[song_i])])
        plt.show()
        '''
    logging.info("predict done")
    return predect_song, song_1day

def plot_singer(predect_song, predict_day=30):
    song2artict = np.load("song2artict.npz")['arr_0'][()]
    song_1day = np.load("song_1.npz")['arr_0'][()]
    artict_predict = {}
    artict_target = {}
    for song_i in song_1day.keys():
        if song2artict[song_i][0] not in artict_predict:
            artict_predict[song2artict[song_i][0]] = np.array(predect_song[song_i])
            artict_target[song2artict[song_i][0]] = np.array(song_1day[song_i])
        else:
            artict_predict[song2artict[song_i][0]] += np.array(predect_song[song_i])
            artict_target[song2artict[song_i][0]] += np.array(song_1day[song_i])
    f = 0.0
    for artist_i in artict_target:
        '''
        print artist_i
        plt.plot(range(len(artict_predict[artist_i])), artict_predict[artist_i])
        #plt.plot(range(183), artict_target[artist_i])
        plt.plot([153,153],[0,max(artict_predict[artist_i])])
        plt.show()
        '''
        ap = artict_predict[artist_i][(183-predict_day):183]
        at = artict_target[artist_i][(183-predict_day):183]
        for i in range(len(at)):
            if at[i] == 0:
                at[i] = 1
        ain = (1.0*(ap-at)) / at
        sigma = np.sqrt( (1.0/predict_day) * sum( ain*ain ) )
        fi = np.sqrt( sum( at ) )
        f += (1.0 - sigma) * fi
        
    print f

def save2file(predect_song, predict_day=60):
    song2artict = np.load("song2artict.npz")['arr_0'][()]
    artict_predict = {}
    for song_i in predect_song.keys():
        if song2artict[song_i] not in artict_predict:
            artict_predict[song2artict[song_i]] = np.array(predect_song[song_i])
        else:
            #print predect_song[song_i]
            artict_predict[song2artict[song_i]] += np.array(predect_song[song_i])
    with open('result.csv', 'w') as fsw:
        for artist_i in artict_predict:
            bd = datetime.datetime.strptime('20150901','%Y%m%d')
            for predict_i in artict_predict[artist_i]:
                fsw.write(artist_i+','+str(int(predict_i))+','+bd.strftime('%Y%m%d')+'\n')
                bd+=datetime.timedelta(days=1)

if __name__ == '__main__':
    RF_regreesion(ignore=50, before_day=4)
    for i in range(1,30,2):
        print i
        #RF_regreesion(ignore=50, before_day=4)
        predect_song, song_1day = predict(ignore=50, days=i, before_day=4)
    #save2file(predect_song)
        plot_singer(song_1day)
    #save_song2artict()
    #pinghua()
    #load_data()
    #fan_num('3fc33ab3c8d7875ba13d2d8e905b421f')
