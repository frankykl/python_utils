# ImageUtils.
# Copyright 2016, DeepPhoton, All rights reserved.
#
import cv2
import os
import glob
import sys
import argparse
import json
from PIL import Image
import urllib
import base64
import pickle
import math
import datetime
import threading
import logging
import csv
from sklearn.decomposition import PCA
from sklearn.grid_search import GridSearchCV
from sklearn.manifold import TSNE
from sklearn.svm import SVC
import time
start = time.time()
from operator import itemgetter

def resize(frame):
    r = 640.0 / frame.shape[1]
    dim = (640, int(frame.shape[0] * r))
    # Resize frame to be processed
    frame = cv2.resize(frame, dim, interpolation = cv2.INTER_AREA)    
    return frame 

def resize_mjpeg(frame):
    r = 640.0 / frame.shape[1]
    dim = (640, 400)#int(frame.shape[0] * r))
    # perform the actual resizing of the image and show it
    frame = cv2.resize(frame, dim, interpolation = cv2.INTER_AREA)    
    return frame  

def writeToFile(filename,lineString): # Used for writing testing data to file
       f = open(filename,"a") 
       f.write(lineString + "\n")    
       f.close()


