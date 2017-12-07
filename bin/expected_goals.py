from bs4 import BeautifulSoup
import requests
import datetime
import re
import sys
import psycopg2
import inspect
import unidecode
import pandas as pd

#to-do move to config file
#to-do add cacheing the names into memory
#to-do switch to server-side cursors
PREMIERE_LEAGUE_TMV = "https://www.transfermarkt.co.uk/premier-league/marktwerteverein/wettbewerb/GB1"
PREMIERE_LEAGUE_AGE = "https://www.transfermarkt.co.uk/premier-league/startseite/wettbewerb/GB1"
LALIGA_TMV = "https://www.transfermarkt.co.uk/laliga/marktwerteverein/wettbewerb/ES1"
LALIGA_AGE = "https://www.transfermarkt.co.uk/laliga/startseite/wettbewerb/ES1"

SOUP_DICT = {}

def transformname(name):
  name = name.decode("utf-8")
  return unidecode.unidecode("".join(name.split()).lower())

def compareTeamNames(name1, name2, cursor):
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
  if (transformname(name1) in transformname(name2)) or (transformname(name2) in transformname(name1)):
    return True
  cursor.execute("SELECT team_id from teams_names_mapping WHERE name = %s", (transformname(name1),))
  id1 = cursor.fetchone()
  cursor.execute("SELECT team_id from teams_names_mapping WHERE name = %s", (transformname(name2),))
  id2 = cursor.fetchone()
  if (id1 == None) or (id2 == None): 
    return False
  elif (id1 == id2): 
    return True


def createDataBaseConnection(parameters):
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
  try:
    conn = psycopg2.connect(parameters)
  except Exception, e:
    print "lol"
    #logger.error("Could not connect to the database: " + str(e))
    sys.exit()
  return conn

def getSoup(link):
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
  r = requests.get(link, 
                headers={ "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"
                })
  soup = BeautifulSoup(r.content, "lxml")
  return soup

def getLeagueSoup(league_key, link, year):
  if ( league_key ) in SOUP_DICT:
    league_soup = SOUP_DICT[league_key]
  else:  
    league_soup = getSoup(link + str(year))
    SOUP_DICT[league_key] = league_soup
  return league_soup

def getExpectedGoalsForAMatch(match_date, p_home_team, p_away_team, league_id, cursor):
  match_date = datetime.datetime.strptime(match_date, "%Y-%m-%d").date()
  if (match_date.month >= 8):
    year = match_date.year
  else:
    year = (match_date.year - 1)

  if league_id == 1729:
  # Premiere league
    league_key = str(league_id) + "_" + str(year)
    league_soup = getLeagueSoup(league_key, PREMIERE_LEAGUE_LINK, year)

  elif league_id == 21518:
  # La liga
    league_key = str(league_id) + "_" + str(year)
    league_soup = getLeagueSoup(league_key, LALIGA_LINK, year)

  else:
    print "Wrong league :("
    sys.exit()

  week = match_date.isocalendar()[1]
  #print "Week: \t%d" % (week)
  match_weeks = [x for x in league_soup.find_all("div", attrs={"data-week" : str(week)})]
  for week in match_weeks:
    matches = week.find_all("div", attrs={"class" : "calendar-game"})
    for match in matches:
      home_team = match.find("div", attrs={"class" : "team-home"}).find("a").getText()
      away_team = match.find("div", attrs={"class" : "team-away"}).find("a").getText()
      # print "\tCOMPARE HOME: %s and %s" % (p_home_team, home_team)
      # print "\tCOMPARE AWAY: %s and %s" % (p_away_team, away_team)
      if compareTeamNames(p_home_team, home_team, cursor) and compareTeamNames(p_away_team, away_team, cursor):
        x_home = match.find("div", attrs={"class" : "game-xG"}).find("div", attrs={"class" : "team-h"}).getText()
        x_away = match.find("div", attrs={"class" : "game-xG"}).find("div", attrs={"class" : "team-a"}).getText()
        return x_home, x_away

  
  # print "No result found"
  # print "---------------"
  # print "Soup h_team:\t" + home_team + "\tExpected h_team:\t" + p_home_team
  # print "Soup a_team:\t" + away_team + "\tExpected a_team:\t" + p_away_team
  return "NaN", "NaN"
  # sys.exit()

# Main function
if __name__ == '__main__':
  PREMIERE_LEAGUE_LINK = "https://understat.com/league/EPL/"
  LALIGA_LINK = "https://understat.com/league/La_liga/"

  INPUT_FILE = "v05-spanish-only.csv"
  OUTPUT_FILE = "version05_with_xgoals_la_liga.csv"

  conn = createDataBaseConnection("postgresql://mlfootball:p3f6f7hs@89.69.65.233:5432/mlfootball")
  cursor = conn.cursor()

  all_vectors = pd.read_csv(INPUT_FILE)
  all_vectors["H_xgoals"] = ""
  all_vectors["A_xgoals"] = ""

  all_vectors["H_team"] = all_vectors["H_team"].apply(lambda x: unidecode.unidecode(x.decode("utf-8")))
  all_vectors["A_team"] = all_vectors["A_team"].apply(lambda x: unidecode.unidecode(x.decode("utf-8")))

  for index, row in all_vectors.iterrows():
    # print "---- %s : %s : %s ----" % (row["Date"], row["H_team"], row["A_team"])
    x_home, x_away = getExpectedGoalsForAMatch(row["Date"], row["H_team"], row["A_team"], row["League_id"], cursor)
    all_vectors.set_value(index, "H_xgoals", x_home)
    all_vectors.set_value(index, "A_xgoals", x_away)

  all_vectors.to_csv(OUTPUT_FILE)


  


