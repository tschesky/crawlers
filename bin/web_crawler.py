import ConfigParser
import json
from lxml import html
import requests
import dryscrape
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import datetime
import re
import sys
import logging
import inspect
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# WEB_CRAWLER.PY
#
# This script downloads a list of fixtures for specified league. Then it extracts only
# interesting data and returns matches from the specified time frame (default = 31 days).
# It then loads the data into specified table in postgres DataBase
# TODO: Add command line parameters

# Get current time-stamp to name the file
# Slice off the unnecessary number of microseconds
def getFileTimestamp(stamp_format='%Y%m%d%H%M%S%f'):
  t = datetime.datetime.now()
  s = t.strftime(stamp_format)
  return s[:-3]

# Download league fixtures from flashscore. Takes desired league as parameter
# todo: consider logging levels
# returned format: dataframe['date', 'round', 'home-team', 'away-team']
def downloadLeagueFixturesFromFlashscore(league='premiere', delta=datetime.timedelta(days=31)):
  logger = logging.getLogger('web_crawler')
  logger.info("ENTER: %40s" % inspect.currentframe().f_code.co_name)

  # Use a headless web browser to scrape because of JS-heavy content
  browser = webdriver.PhantomJS(service_log_path='../log/selenium/ghostdriver.log')
  
  if league == "premiere":
    league_id = 1729
    browser.get('https://www.flashscore.com/soccer/england/premier-league/fixtures/')
    logger.info("The chosen league is Premier League")
  elif league == "laliga":
    league_id = 21518
    browser.get('https://www.flashscore.com/soccer/spain/laliga/fixtures/')
    logger.info("The chosen league is La Liga")
  else:
    logger.error("An unrecognized league name has been passed to %20s" % inspect.currentframe().f_code.co_name)
    sys.exit()
  
  while (browser.find_element_by_css_selector(".link-more-games").get_attribute('style') != 'display: none;'):
    browser.execute_script("loadMoreGames();")  
    time.sleep(1)
  
  html_source = browser.page_source

  # Create session and visit page to render JS content
  soup = BeautifulSoup(html_source, "lxml")

  # Collect data
  match_table = soup.find("table", attrs={"class" : "soccer"})
  match_table_body = match_table.find('tbody')
  match_table_rows = match_table_body.find_all("tr", attrs={"class" : ["stage-scheduled", "event_round"]})
  m = re.search('(20\d\d/20\d\d)', soup.find("h2", attrs={"class" : "tournament"}).text.strip())
  season_start, season_end =  m.group(0).split('/')
  current_round = ""

  completeData = []

  x = 0

  # Iterate until we've done all the matches or until we've passed the selected time-delta
  while( x < len(match_table_rows)):
    row = match_table_rows[x]
    x = x + 1

    if row['class'][0] == "event_round":
      current_round = re.search('(\d+)', row.text.strip()).group(0)
    else:
      date = row.find("td", attrs={"class" : "time"}).text.strip()
      date_converted = datetime.datetime.strptime(date, "%d.%m. %H:%M")
      if (date_converted.month >= 8):
        year = season_start
      else:
        year = season_end
      date_converted = date_converted.replace(year = int(year))

      if date_converted > datetime.datetime.now() + delta:
        print "end"
        break

      date_converted = date_converted.strftime(year + "-%m-%d")
      h_team = row.find("td", attrs={"class" : "team-home"}).text.strip()
      a_team = row.find("td", attrs={"class" : "team-away"}).text.strip()

      # Log the data 
      ready_data_row = "{ Stage: " + current_round + \
                       "\tdate: "+  date_converted+ \
                       "\tH_team: " + h_team.ljust(20) + \
                        "\tA_team: " + a_team + "}"
      logger.debug(ready_data_row)

      # Add to result data
      completeData.append({'season': season_start + "/" + season_end, 'league_id': league_id, 'date': date_converted, 'stage': current_round, 'h_team': h_team, 'a_team': a_team})

  completeData = pd.DataFrame(completeData, columns = ('season', 'league_id', 'date', 'stage', 'h_team', 'a_team'))
  logger.info("LEAVE: %40s" % inspect.currentframe().f_code.co_name)

  return completeData

# Take a whole dataframe and insert contets into a specified table
def loadDataFrameToPostgresqlTable(df, tableName):
  df.columns = [c.lower() for c in df.columns] #postgres doesn't like capitals or spaces
  engine = create_engine('postgresql://mlfootball:p3f6f7hs@89.69.62.221:5432/mlfootball')
  df.to_sql(tableName, engine, if_exists='append', index_label='match_id')


# Parse config file for the stripts
def parseConfigFile(fileName=None):
  if fileName is None:
    fileName = "crawler.cfg"
  config = ConfigParser.RawConfigParser()
  config.read(fileName)
  englishList = json.loads(config.get("English","sites_list"))
  spanishList = json.loads(config.get("Spanish","sites_list"))
  return englishList, spanishList

def createDataBaseConnection(parameters):
  try:
    conn = psycopg2.connect(parameters)
  except Exception, e:
    logger.error("Could not connect to the database: " + str(e))
    sys.exit()
  return conn

# Load data into a table in a smart way
# Check if there is match like this and update it if not - insert
# todo: load database data from config file
def loadDataFrameToPostgresqlTableWithUpdate(df, tableName):
  logger = logging.getLogger('web_crawler')
  logger.info("ENTER: %40s" % inspect.currentframe().f_code.co_name)
  conn = createDataBaseConnection("postgresql://mlfootball:p3f6f7hs@89.69.62.221:5432/mlfootball")
  cur = conn.cursor()
  for index, row in df.iterrows():
      cur.execute("SELECT * from matches where h_team = %s AND a_team = %s AND stage = %s;", (row['h_team'], row['a_team'], row['stage']))
      result = cur.fetchone()
      if result != None:
        id = result[0]
        cur.execute("UPDATE matches SET date=%s WHERE match_id=%s", (row['date'], id))
      else:
        cur.execute("INSERT INTO matches (season, league_id, date, stage, h_team, a_team) VALUES (%s, %s, %s, %s, %s, %s)", 
                    (row['season'], row['league_id'], row['date'], row['stage'], row['h_team'], row['a_team']))
  conn.commit()
  logger.info("LEAVE: %40s" % inspect.currentframe().f_code.co_name)

# Configure logger
# to-do: add configurable logging directory from config file
# to-do: configure levels etc 
def configureLogger():
  logfileName = "../log/" + getFileTimestamp() + "_web_crawler.log"
  logger = logging.getLogger('web_crawler')
  hdlr = logging.FileHandler(logfileName)
  formatter = logging.Formatter('[%(asctime)s %(funcName)40s] %(levelname)10s %(message)s')
  hdlr.setFormatter(formatter)
  logger.addHandler(hdlr)
  logger.setLevel(logging.DEBUG)

# Main function
if __name__ == '__main__':
  # Configure the logging class
  configureLogger()

  newData = downloadLeagueFixturesFromFlashscore("premiere")
  loadDataFrameToPostgresqlTableWithUpdate(newData, "matches")
  newData = downloadLeagueFixturesFromFlashscore("laliga")
  loadDataFrameToPostgresqlTableWithUpdate(newData, "matches")

  # Load example configuration
  # newData = downloadLeagueFixturesFromFlashscore("premiere")
  # loadDataFrameToPostgresqlTable(newData, "matches")
  # newData = downloadLeagueFixturesFromFlashscore("premier")
  # loadDataFrameToPostgresqlTable(newData, "matches")