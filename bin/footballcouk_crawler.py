import datetime
import pandas
import sys
import psycopg2

def transformname(name):
    return "".join(name.split()).lower()

def createDataBaseConnection(parameters):
    try:
        conn = psycopg2.connect(parameters)
    except Exception, e:
        #logger.error("Could not connect to the database: " + str(e))
        print "Error: " + str(e)
        sys.exit()
    return conn

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

def applyToRows(name):
    return "".join(name.split()).lower()

# Download data about the specified match 
# Ecpected date format is yyyy-mm-dd
# Returs a list of values in the same order as headers [val1, val2, val3, ...]
def downloadFootballDataCoUk(date, h_team, a_team, league_id):
    link = "http://www.football-data.co.uk/mmz4281/%s/%s.csv"
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')   
    co_uk_date = datetime.datetime.strftime(dt, '%d/%m/%y')
    if (dt.month >= 8):
        season = str(int(dt.year%100)).zfill(2) + str(int((dt.year+1)%100)).zfill(2)
    elif (dt.month <= 5):
        season = str(int((dt.year-1)%100)).zfill(2) + str(int(dt.year%100)).zfill(2)
    else:
        return {'FTHG' : 'NA', 'FTAG' : 'NA', 'HS': 'NA', 'AS' : 'NA', 'HST' : 'NA', 'AST' : 'NA'}
    print link % (season, "E0")
    if league_id == '1729':
        data = pandas.read_csv(link % (season, "E0"))
    elif league_id == '21518':
        data = pandas.read_csv(season + "SP1.csv")
    match_data = data[data['Date'] == co_uk_date]

    results = {}
    for index, row in match_data.iterrows():   
        if compareTeamNames(row['HomeTeam'], h_team) and compareTeamNames(row['AwayTeam'], a_team):
            results['FTHG'] = row['FTHG'] if 'FTHG' in row else 'NA'
            results['FTAG'] = row['FTAG'] if 'FTAG' in row else 'NA'
            results['HS'] = row['HS'] if 'HS' in row else 'NA'
            results['AS'] = row['AS'] if 'AS' in row else 'NA'
            results['HST'] = row['HST'] if 'HST' in row else 'NA'
            results['AST'] = row['AST'] if 'AST' in row else 'NA'
    if not results:
        results = {'FTHG' : 'NA', 'FTAG' : 'NA', 'HS': 'NA', 'AS' : 'NA', 'HST' : 'NA', 'AST' : 'NA'}

    return results

# Main function
if __name__ == '__main__':
    result = downloadFootballDataCoUk("2017-08-11", "Arsenal", "Leicester", '1729')
    print result
    print "poop"