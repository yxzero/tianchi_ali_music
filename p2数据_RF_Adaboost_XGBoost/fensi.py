# -*- coding:utf-8 -*-
'''
Created on 2016��6��8��

@author: cs
'''
import pandas as pd
import time

def load_data(action_type=1,song_id=''):
    data = pd.read_csv('p2_mars_tianchi_user_actions.csv', names=['user_id','song_id','gmt_create','action_type','Ds'])
    #print(data.groupby(['user_id','song_id','actiontype','Ds']).count())
    '''
    print data.where(lambda x:x.song_id=='6c13ac393fc749f0e7aca2cc58d4c55b',
                   lambda x:x.at==1).groupby(['user_id','Ds'], sort='Ds').count()
    '''
    data = data.loc[lambda x:x.action_type==1]
    del data['action_type']
    del data['Ds']
    k = 0
    for index, row in data.iterrows():
        if k%10000 == 0:
            print k 
        k += 1
        data.ix[index,'gmt_create'] = time.strftime('%Y%m%d',time.localtime(int(row['gmt_create'])))
    data.to_csv('p2_mar_action_1.csv',index=False,header=False)

if __name__ == '__main__':
    load_data()