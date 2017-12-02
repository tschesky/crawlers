import datetime
import pandas as pd
import sys
import psycopg2
from sqlalchemy import create_engine
import operator
import unidecode

LEAGUE_ID_H = 'league_id'
SEASON_H = 'season'
RESULT_H = 'result'
H_NAME_H = 'h_team' 
A_NAME_H = 'a_team'
MATCH_ID_H = 'match_id'
ENG_LEAGUE_ID = '1729'
SPAN_LEAGUE_ID = '21518'
RESULTS = {0 : "h_win", 1 : "draw", 2 : "a_win", '0' : "h_win", '1' : "draw", '2' : "a_win"}
DB_CONNECTION = "postgresql://mlfootball:p3f6f7hs@89.69.62.221:5432/mlfootball"
TEAMS_NAMES_MAPPING = None

def transformname(name):
  return unidecode.unidecode("".join(name.split()).lower())

def createDataBaseConnection(parameters):
  try:
      conn = psycopg2.connect(parameters)
  except Exception, e:
      #logger.error("Could not connect to the database: " + str(e))
      print "Error: " + str(e)
      sys.exit()
  return conn

# Calculate form of the team in a given set of matches
# ARGUMENTS:
# - matches     : a limited set of matches 
# - team_name   : name of the team taken into calculation
def calculateForm(matches, team_name, weights):
    if len(matches) < 2:
      return "NULL"
    form = 0
    for row in matches:
        result = RESULTS[row[RESULT_H]]
        if compareTeamNames(row[H_NAME_H],team_name):
            if result == 'h_win':
                form += 3
            elif result == 'draw':
                form += 1
            elif result == 'a_win':
                form += 0
        elif compareTeamNames(row[A_NAME_H],team_name):
            if result == 'h_win':
                form += 0
            elif result == 'draw':
                form += 1
            elif result == 'a_win':
                form += 3
    return form

# Calculate the (weighted) mean of shots
def calculateMeanOfFTG(matches, team_name, weights):
    if len(matches) < 2:
      return None
    total = 0
    x = -1
    for row in matches:
        x = x + 1
        if compareTeamNames(row[H_NAME_H],team_name):
            total += float(row['fthg']) * weights[x]
        elif compareTeamNames(row[A_NAME_H],team_name):
            total += float(row['ftag']) * weights[x]
    return total / sum(weights[0:x])          

# Calculate the (weighted) mean of shots
def calculateMeanOfSHots(matches, team_name, weights):
    if len(matches) < 2:
      return None
    total = 0
    x = -1
    for row in matches:
        x = x + 1
        if row[H_NAME_H] == team_name:
            total += float(row['ha']) * weights[x]
        elif row[A_NAME_H] == team_name:
            total += float(row['as']) * weights[x]
    return total / sum(weights[0:x])         

# Calculate the (weighted) mean of shots on target
def calculateMeanOfSHotsOnTarget(matches, team_name, weights):
    if len(matches) < 2:
      return None
    total = 0
    x = -1
    for row in matches:
        x = x + 1
        if compareTeamNames(row[H_NAME_H],team_name):
            total += float(row['hst']) * weights[x]
        elif compareTeamNames(row[A_NAME_H],team_name):
            total += float(row['ast']) * weights[x]
    return total / sum(weights[0:x])      

def createNewCumulativeAttribute(learning_vectors, header, no_of_matches, calc_fun, weights=[1,1,1,1,1,1,1,1,1,1]):
  conn = createDataBaseConnection(DB_CONNECTION)
  curs = conn.cursor()
  engine = create_engine(DB_CONNECTION)
  df = pd.read_sql_query('SELECT match_id, h_team, a_team FROM matches WHERE %s is NULL OR %s is NULL' % ("h_"+header, "a_"+header), con=engine)
  matches = df.T.to_dict().values()
  
  for row in matches:
    id = row['match_id']
    h_team_matches = getLastNMatchesOfATeam(learning_vectors, row[H_NAME_H], no_of_matches)
    a_team_matches = getLastNMatchesOfATeam(learning_vectors, row[A_NAME_H], no_of_matches)
    h_result = calc_fun(h_team_matches, row[H_NAME_H], weights)
    a_result = calc_fun(a_team_matches, row[A_NAME_H], weights)
    h_new_attribute = round(float(h_result), 2) if h_result else "NULL"
    a_new_attribute = round(float(a_result), 2) if a_result else "NULL"
    query = "UPDATE matches SET %s=%s, %s=%s WHERE match_id=%s" % ("h_"+header, h_new_attribute, "a_"+header, a_new_attribute, id)
    print query
    curs.execute(query)
    conn.commit()
  conn.close()
    
def getLastNMatchesOfATeam(data, teamName, N):
    team_matches = []
    conn = createDataBaseConnection(DB_CONNECTION)
    curs = conn.cursor()
    for row in data:
        if compareTeamNames(row[H_NAME_H], teamName) or compareTeamNames(row[A_NAME_H], teamName):
            team_matches.append(row)
    conn.close()
    return sorted(team_matches, key=operator.itemgetter(MATCH_ID_H))[:N]  

def compareTeamNames(name1, name2):
  id1 = TEAMS_NAMES_MAPPING.loc[TEAMS_NAMES_MAPPING['name'] == transformname(name1)]['team_id'].values[0]
  id2 = TEAMS_NAMES_MAPPING.loc[TEAMS_NAMES_MAPPING['name'] == transformname(name2)]['team_id'].values[0]
  if (id1 == None) or (id2 == None): 
      return False
  elif (id1 == id2): 
      return True

# Main function
if __name__ == '__main__':
  engine = create_engine(DB_CONNECTION)
  df = pd.read_sql_query('SELECT * FROM learning_vectors', con=engine)
  TEAMS_NAMES_MAPPING = pd.read_sql_query('SELECT * FROM teams_names_mapping', con=engine)
  learning_vectors = df.T.to_dict().values()
  createNewCumulativeAttribute(learning_vectors, "mean_shots05", 5, calculateMeanOfSHotsOnTarget)
  createNewCumulativeAttribute(learning_vectors, "mean_goals05", 5, calculateMeanOfFTG)