from bs4 import BeautifulSoup
import requests
import datetime
import re
import sys
import psycopg2
import inspect
import unidecode

#to-do move to config file
#to-do add cacheing the names into memory
#to-do switch to server-side cursors
PREMIERE_LEAGUE_TMV = "https://www.transfermarkt.co.uk/premier-league/marktwerteverein/wettbewerb/GB1"
PREMIERE_LEAGUE_AGE = "https://www.transfermarkt.co.uk/premier-league/startseite/wettbewerb/GB1"
LALIGA_TMV = "https://www.transfermarkt.co.uk/laliga/marktwerteverein/wettbewerb/ES1"
LALIGA_AGE = "https://www.transfermarkt.co.uk/laliga/startseite/wettbewerb/ES1"

def transformname(name):
  return "".join(name.split()).lower()

def compareTeamNames(name1, name2, cursor):
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
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
    #logger.error("Could not connect to the database: " + str(e))
    sys.exit()
  return conn

def getCutOffDates(link):
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
  soup = getSoup(link)
  contents = [str(x['value']) for x in soup.find("div", attrs={"class" : "inline-select"}).find("select").find_all('option')]
  contents = [datetime.datetime.strptime(date, "%Y-%m-%d").date() for date in contents]
  return contents

def returnSmallestDeltaTime(cutoffDates, matchDate):
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
  bestDate = datetime.date.today()
  delta = datetime.timedelta(days=3650)
  for date in cutoffDates:
    if (date <= matchDate) and ((matchDate - date) < delta):
      delta = matchDate - date
      bestDate = date
  return bestDate

def findRowWithSpecifiedTeamName(teamName, tableRows, pattern, cursor):
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
  pattern = re.compile(pattern)
  found = False
  for row in tableRows:
    columns = row.findAll('td')
    for column in columns:
      text = column.text.strip()
      #print "Searching: %s vs %s" % (unidecode.unidecode(text), teamName)
      if compareTeamNames(unidecode.unidecode(text), teamName, cursor):
        #print "Found: %s vs %s" % (unidecode.unidecode(text), teamName)
        found = True
      if found ==True:
        match = pattern.search(text)
        if match != None:
          return match.group(1)
  if found == False:
    print "ERROR: NO RECORD FOUND FOR TEAM: %s" % teamName
    sys.exit()

def getMatchTmv(matchDate, homeTeam, awayTeam, link):
  dbConnection = createDataBaseConnection("postgresql://mlfootball:p3f6f7hs@89.69.62.221:5432/mlfootball")
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
  date = datetime.datetime.strptime(matchDate, "%Y-%m-%d").date()
  resultDate = returnSmallestDeltaTime(getCutOffDates(link), date)
  resultLink = link + "/plus/?stichtag=" + resultDate.strftime("%Y-%m-%d")
  soup = getSoup(link)
  match_table = soup.find("table", attrs={"class" : "items"}).find("tbody").findAll("tr")
  home_team_value = findRowWithSpecifiedTeamName(homeTeam, match_table, "(\d*\.\d*)", dbConnection.cursor())
  away_team_value = findRowWithSpecifiedTeamName(awayTeam, match_table, "(\d*\.\d*)", dbConnection.cursor())
  dbConnection.close()
  return home_team_value, away_team_value

def getMatchAge(matchDate, homeTeam, awayTeam, link):
  dbConnection = createDataBaseConnection("postgresql://mlfootball:p3f6f7hs@89.69.62.221:5432/mlfootball")
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
  date = datetime.datetime.strptime(matchDate, "%Y-%m-%d").date()
  if (date.month >= 8):
    year = date.year
  else:
    year = (date.year - 1)
  resultLink = link + "/plus/?saison_id=" + str(year)
  soup = getSoup(link)
  match_table = soup.find("table", attrs={"class" : "items"}).find("tbody").findAll("tr")
  home_team_value = findRowWithSpecifiedTeamName(homeTeam, match_table, "(\d*,\d*)", dbConnection.cursor())
  away_team_value = findRowWithSpecifiedTeamName(awayTeam, match_table, "(\d*,\d*)", dbConnection.cursor())
  dbConnection.close()
  return home_team_value, away_team_value

def getSoup(link):
  #print "ENTER: %40s" % inspect.currentframe().f_code.co_name
  r = requests.get(link, 
                headers={ "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"
                })
  soup = BeautifulSoup(r.content, "lxml")
  return soup

def updateRecord(id, matchDate, homeName, awayName, leagueId):
  link_tmv = ""
  link_age = ""
  if leagueId == 21518:
    link_tmv = LALIGA_TMV
    link_age = LALIGA_AGE
  elif leagueId == 1729:
    link_tmv = PREMIERE_LEAGUE_TMV
    link_age = PREMIERE_LEAGUE_AGE
  dbConnection = createDataBaseConnection("postgresql://mlfootball:p3f6f7hs@89.69.62.221:5432/mlfootball")
  #print "Id: \t%d, home: \t%s, away: \t%s" % (id, homeName, awayName)
  home_tmv, away_tmv = getMatchTmv(matchDate, homeName, awayName, link_tmv)
  home_age, away_age = getMatchAge(matchDate, homeName, awayName, link_age)
  #print "h_tmv:\t%s, a_tmv:\t%s, h_age:\t%s, a_age:\t%s\n\n" % (home_tmv, away_tmv, home_age, away_age)
  cursor = dbConnection.cursor()
  if home_age:
    home_age = home_age.replace(',', '.')
  if away_age:
    away_age = away_age.replace(',', '.')
  cursor.execute("UPDATE matches SET h_tmv=%s, a_tmv=%s, h_age=%s, a_age=%s WHERE match_id=%s", (home_tmv, away_tmv, home_age, away_age, id))
  dbConnection.commit()
  dbConnection.close()

# Main function
if __name__ == '__main__':
  conn = createDataBaseConnection("postgresql://mlfootball:p3f6f7hs@89.69.62.221:5432/mlfootball")
  cur = conn.cursor()
  cur.execute("SELECT * from matches WHERE match_id > 528")
  row = cur.fetchone()
  while row:
    updateRecord(row[0], row[4], row[5], row[6], row[1])
    row = cur.fetchone()
    print 'done'
  conn.close()