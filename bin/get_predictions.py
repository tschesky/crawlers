from sklearn.tree import DecisionTreeClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from sklearn.externals import joblib
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from sklearn.preprocessing import Imputer
import decimal

DB_CONNECTION = "postgresql://mlfootball:p3f6f7hs@89.69.62.221:5432/mlfootball"


def getResultAndOrderProbabilities(probabilites, labels):

  results = [0, 0, 0]

  for x in range(0,3):
    results[labels[x]] = round(probabilites[x] * 100, 2)

  verdict = results.index(max(results))

  return verdict, results

def createDataBaseConnection(parameters):
  try:
      conn = psycopg2.connect(parameters)
  except Exception, e:
      #logger.error("Could not connect to the database: " + str(e))
      print "Error: " + str(e)
      sys.exit()
  return conn

if __name__ == "__main__":
    cart = joblib.load('../models/cart.pkl') 
    nb = joblib.load('../models/nb.pkl') 
    svm = joblib.load('../models/svm.pkl') 
    engine = create_engine(DB_CONNECTION)
    imp = joblib.load('../models/imputer.pkl') 

    df = pd.read_sql_query('SELECT match_id, h_form05, a_form05, h_meanshotsontarget05,'
                                  'a_meanshotsontarget05, h_meanfulltimegoals05,'
                                  'a_meanfulltimegoals05 FROM matches', con=engine)
    
    conn = createDataBaseConnection(DB_CONNECTION)
    cursor = conn.cursor()

    matches = df.T.to_dict().values()
    for row in matches:
        id = row['match_id']
        features = imp.transform([[row["h_form05"], row["a_form05"], row["h_meanshotsontarget05"], 
                            row["a_meanshotsontarget05"], row["h_meanfulltimegoals05"], row["a_meanfulltimegoals05"]]])
        cart_tmp = cart.predict_proba(features)
        nb_tmp = nb.predict_proba(features)
        svm_tmp = svm.predict_proba(features)

        nb_verdict, nb_results = getResultAndOrderProbabilities(nb_tmp.tolist()[0], nb.classes_)
        svm_verdict, svm_results = getResultAndOrderProbabilities(svm_tmp.tolist()[0], svm.classes_)
        cart_verdict, cart_results = getResultAndOrderProbabilities(cart_tmp.tolist()[0], cart.classes_)

        cursor.execute("UPDATE matches SET cart_res=%s, nb_res=%s, svm_res=%s WHERE match_id=%s", (cart_verdict, nb_verdict, svm_verdict, id))
        cursor.execute("UPDATE matches SET cart_proba=ARRAY[%s, %s, %s] WHERE match_id=%s", (cart_results[0], cart_results[1], cart_results[2], id))
        cursor.execute("UPDATE matches SET svm_proba=ARRAY[%s, %s, %s] WHERE match_id=%s", (svm_results[0], svm_results[1], svm_results[2], id))
        cursor.execute("UPDATE matches SET nb_proba=ARRAY[%s, %s, %s] WHERE match_id=%s", (nb_results[0], nb_results[1], nb_results[2], id))

    conn.commit()
    conn.close()