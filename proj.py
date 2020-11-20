import pymongo
import datetime
import pprint
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
client = MongoClient('localhost', 27017) #localhost database connection
db = client.ipl
collection=db.batsman_score

#Function to retrieve names of all palyers for a given season
def season_player():
    sesn=int(input('enter season(range 1-5) to get details of all the players who played in that season: '))
    ip=db.batsman_score.find({},{'_id':0,'season':1})
    t=set()
    for n in ip:

        t.add(n['season'])

    if sesn in t:
        cursor1=db.batsman_score.find({"season":sesn},{'_id':0,'batsman':1,'total_runs':1,'innings':1})
        for x in cursor1:
            pprint.pprint(x)

    else:
        print("enter a valid number for season")

# function to get best player of the season
def player_of_season():
    ssn=int(input('enter season to get player of the season:'))
    ip = db.batsman_score.find({}, {'_id': 0, 'season': 1})
    t = set()
    for n in ip:

        t.add(n['season'])
    if ssn in t:

        cur = db.batsman_score.aggregate([{"$match": {"season": ssn}},
                                          {"$project": {"batsman": 1, "average":
                                              {"$divide": ["$total_runs", "$innings"]}}}])
        for x in cur:
            db.player_average.insert_one(x)
        w = db.player_average.aggregate([{"$sort": {"average": -1}}, {"$limit": 1}])
        for m in w:
            print('The best player of the season',ssn,' is', m['batsman'], 'with average of', m['average'])

        db.player_average.drop()
    else:
        print("season not found, please enter valid season number(range 1-5).")

# funtion to calculate strike rate of the player
def strike_rate():
    print('V Kohli')
    print('SK Raina')
    print('RG Sharma')
    print('DA Warner')
    print('S Dhawan')
    print('CH Gayle')
    print('MS Dhoni')
    print('RV Uthappa')
    print('AB de Villiers')
    print('G Gambhir')
    s=int(input('enter the season:'))
    ip = db.batsman_score.find({}, {'_id': 0, 'season': 1})
    t = set()
    for n in ip:
        # print(n)
        t.add(n['season'])
    if s not in t:
        print('season not found, please enter valid season number(range 1-5).')
    else:
        player_name = input('enter player name :')
        e=db.batsman_score.find({"season":s},{"batsman":1})
        g=set()
        for f in e:

            g.add(f['batsman'])
        if player_name not in g:
            print('Player not found, please enter a valid name.')

        else:
            cur=db.batsman_score.aggregate([{"$match":{"season":s,"batsman":player_name}},
                                { "$project": { "batsman": 1, "strikerate":
                                    { "$divide": [ "$total_runs", "$numberofballs" ] } } }])
            for z in cur:
                print('Strike rate of a given player is : ',float(z['strikerate'])*100)

# function that joins two collections to retrieve personal information of a player
def player_details():
    print('V Kohli')
    print('SK Raina')
    print('RG Sharma')
    print('DA Warner')
    print('S Dhawan')
    print('CH Gayle')
    print('MS Dhoni')
    print('RV Uthappa')
    print('AB de Villiers')
    print('G Gambhir')
    name_player=input('enter the name:')
    e = db.batsman_score.find({}, {"batsman": 1})
    g = set()
    for f in e:
        # print(f['batsman'])
        g.add(f['batsman'])
    # print(g)
    if name_player in g:
        cur1=db.batsman_score.aggregate([{"$match":{"batsman":name_player}},{
                "$lookup":
                    {
                        "from": "details",
                        "localField": "batsman",
                        "foreignField": "name",
                        "as": "test"
                    }
            },{"$limit":1}])
        for a in cur1:
            print(a["test"])

    else:
        print('Player not found, please enter a valid name.')


#function to analyse pitvh conditions
def pitch():
    print('Rajiv Gandhi International Stadium, Uppal')
    print('Maharashtra Cricket Association Stadium')
    print('Saurashtra Cricket Association Stadium')
    print('Holkar Cricket Stadium')
    print('M Chinnaswamy Stadium')
    print('Wankhede Stadium')
    print('Eden Gardens ')
    print('Feroz Shah Kotla')
    print('Green Park')
    print('Punjab Cricket Association IS Bindra Stadium, Mohali')
    ven=input("enter the venue of the pitch:")


    ip = db.ipl_venue.find({}, {'_id': 0, 'venue': 1})
    t = set()
    for n in ip:

        t.add(n['venue'])


    if ven not in t:
        print('venue not found, please enter valid venue')

    else:
        cur2=db.ipl_venue.aggregate([{"$match":{"venue":ven}},{"$group":{"_id":{"runlead":"$win_by_runs"},"wonruns":{"$sum":1}}}])
        count=0
        count1=0
        for c in cur2:
            #pprint.pprint(c)
            if c['_id']['runlead'] !=0:
                count+=1
            else:
                count1=c['wonruns']

        print('Matches won by runs:', count)
        print('Matches won by wickets',count1)
        if count>count1:
            print("The given pitch is first batting favourable")
        elif count<count1:
            print("The given pitch is first bowling favourable")
        else:
            print("The given pitch is neutral")


# function to plot graph of runs scored by a player
def graph():
    x=db.batsman_score.aggregate([{"$group": {"_id": {"batting": "$batsman"}, "count": {"$sum": "$total_runs"}}}])
    a=[]
    b=[]
    for y in x:
        print(y)
        a.append(y['_id']['batting'])
        b.append(y['count'])
    print(a)
    print(b)
    df3=pd.DataFrame({'batsman':a,'runs_count': b})

    print(df3)
    df3.plot(kind='bar', y='runs_count', x='batsman')
    plt.xlabel('batsman')
    plt.ylabel('total no. of runs')
    plt.show()

# function to plot graph of player average for a given season
def season_avg():
    sesn = int(input('enter the season number :'))
    ip = db.batsman_score.find({}, {'_id': 0, 'season': 1})
    t = set()
    for n in ip:
        t.add(n['season'])

    if sesn in t:
        s = db.batsman_score.aggregate([{"$group": {"_id": {"batting": "$batsman"}}}])
        a = []
        c = []
        for x in s:
            a.append(x['_id']['batting'])

        print(a)
        l = len(a)

        for i in range(l):
            cur = db.batsman_score.aggregate([{"$match": {"season": sesn, "batsman": a[i]}},
                                        {"$project": {"batsman": 1, "average":
                                            {"$divide": ["$total_runs", "$innings"]}}}])
            b = []

            for z in cur:
                b.append(z['average'])

            c.append(b)
        d = []
        for i in range(len(c)):
            d.append(c[i][0])
        print(d)

        df3 = pd.DataFrame({'batsman': a, 'runs_count': d})
        print(df3)
        df3.plot(kind='bar', y='runs_count', x='batsman')
        plt.xlabel('batsman')
        plt.ylabel('avg_runs')
        plt.show()

    else:
        print('season not found, enter valid season.')

for i in range(8):
    print()
    print("***********Menu**********")
    print("1.Given season, returns all the players played in that season.")
    print("2.Calculates the strike rate of a player in a particular season.")
    print("3.Best player of the season(player with good average).")
    print("4.Joins two documents to get details of player.")
    print("5.Analysing pitch conditions.")
    print("6.Visual representation of overall runs scored by batsman. ")
    print("7.Average of a player ")
    #for i in range(8):
    inp = input('enter the query number you want: ')
    if inp == '1':
        season_player()
    elif inp == '2':
        strike_rate()
    elif inp == '3':
        player_of_season()
    elif inp == '4':
        player_details()
    elif inp == '5':
        pitch()
    elif inp == '6':
        graph()
    elif inp=='7':
        season_avg()
    elif inp == '0':
        exit()
    else:
        print('enter valid input.')
    print()