#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pickle
import numpy as np
import pandas as pd
from libraries.Crawler import crawler


with open("./logs/model.pickle","rb") as f:
    model=pickle.load(f)
    
    
feat_cols=[
        'Temperature',
        'Humidity',
        'Mean Sea Level Pressure  [MSL]',
        'Wind Speed  [10 m above gnd]',
        'High Cloud Cover  [high cld lay]',
        'Wind Direction  [10 m above gnd]',
        'Wind Gust  [sfc]'
]

grid_dev=[(1,3),(0,0),(3,2),(2,1)]
eucledine=lambda a,b,c,d:round(((a-c)**2+(b-d)**2)**0.5,2)

def get_contribution_vec(x,y):
    try:
        device=grid_dev.index((x,y))
        #print('It is a device')
        vec=[0,0,0,0]
        vec[device]=1
    except ValueError:
        #print('Not a device')
        vec=[0,0,0,0]
        for i,(a,b) in enumerate(grid_dev):
            vec[i]=eucledine(a,b,x,y)
    return vec


grid=[]
for i in range(4):
    for j in range(4):
        grid.append(get_contribution_vec(i,j))



def Interpolate(dev,rc):

    #interpolating w.r.to distance vec for each point.

    interpol=[]
    for g in grid:
        x=1/np.array(g).reshape(-1,1)
        x[x == np.inf] = 0

        interpol.append((x*dev).sum(axis=0)/np.sum(x))

    #Interpolated features

    grid_feat=pd.DataFrame(interpol)[feat_cols]

    #RandomForest Predictions

    grid_wise_pred=rc.predict(grid_feat)
    
    return np.array(grid_wise_pred,dtype="int").reshape(4,4).tolist()


# In[2]:


from flask import Flask,jsonify
from flask_cors import cross_origin
app = Flask(__name__)

@cross_origin
@app.route('/')
def home():
    return "Home"

@cross_origin
@app.route('/<date>/<hour>')
def fun(date='2020-12-19',hour='11'):
    df=crawler(date+' '+hour)[feat_cols]
    pred=Interpolate(df,model)
    
    data=        [
          {
            "z": pred,
            "type": 'heatmap'
          }
        ]
    return jsonify(data)


# In[3]:


app.run()


# In[ ]:


#APP Done

