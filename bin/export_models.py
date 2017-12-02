import unidecode
from datetime import datetime
from scipy.stats import uniform as sp_rand
import Tkinter
import csv
import datetime
import random
import math
import operator
import matplotlib.pyplot as plt
from sklearn import preprocessing
from sklearn.datasets import load_iris
import numpy as np
import pandas
import random
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neighbors import KNeighborsClassifier
from sklearn import model_selection
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import RandomizedSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from sklearn.externals import joblib
from sklearn.preprocessing import Imputer

if __name__ == "__main__":
  dataframe = pandas.read_csv("../full_data_with_fulltime_goals_and_weighted_means.csv")
  X = dataframe[["h_form05","a_form05","h_meanshotsontarget05","a_meanshotsontarget05","h_meanfulltimegoals05","a_meanfulltimegoals05"]]
  Y = dataframe['result']
  X = X.values
  Y = Y.values
  cart = DecisionTreeClassifier()
  nb = GaussianNB()
  svm = SVC(probability=True)
  cart.fit(X, Y)
  nb.fit(X, Y)
  svm.fit(X, Y)
  joblib.dump(cart, '../models/cart.pkl')
  joblib.dump(nb, '../models/nb.pkl') 
  joblib.dump(svm, '../models/svm.pkl')  

  imp = Imputer(missing_values='NaN', strategy='mean', axis=0)
  imp.fit(X)
  joblib.dump(imp, '../models/imputer.pkl')  