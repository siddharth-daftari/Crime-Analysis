import logging
import json
from spyne.application import Application
from spyne import rpc
from spyne.service import ServiceBase
from spyne.model.primitive import String,Unicode,Integer,Float
from spyne.protocol.json import JsonDocument
from spyne.protocol.http import HttpRpc
from spyne.server.wsgi import WsgiApplication
import urllib2
from datetime import datetime
import re

class CheckCrimeService(ServiceBase):
    @rpc(Float, Float, Float, _returns=String)
    def checkcrime(ctx,lat, lon, radius):
        crimeDataResponseVar = {}
        crimeDataResponseVar['crime_type_count'] = {}
        crimeAddressTemp = {}
        crimeDataResponseVar['event_time_count'] = {
            "12:01am-3am" : 0,
            "3:01am-6am" : 0,
            "6:01am-9am" : 0,
            "9:01am-12noon" : 0,
            "12:01pm-3pm" : 0,
            "3:01pm-6pm" : 0,
            "6:01pm-9pm" : 0,
            "9:01pm-12midnight" : 0
        }
        
        timeStamp1 = datetime.strptime('12:01 AM', '%I:%M %p')
        timeStamp2 = datetime.strptime('03:00 AM', '%I:%M %p')
        timeStamp3 = datetime.strptime('03:01 AM', '%I:%M %p')
        timeStamp4 = datetime.strptime('06:00 AM', '%I:%M %p')
        timeStamp5 = datetime.strptime('06:01 AM', '%I:%M %p')
        timeStamp6 = datetime.strptime('09:00 AM', '%I:%M %p')
        timeStamp7 = datetime.strptime('09:01 AM', '%I:%M %p')
        timeStamp8 = datetime.strptime('12:00 PM', '%I:%M %p')
        timeStamp9 = datetime.strptime('12:01 PM', '%I:%M %p')
        timeStamp10 = datetime.strptime('03:00 PM', '%I:%M %p')
        timeStamp11 = datetime.strptime('03:01 PM', '%I:%M %p')
        timeStamp12 = datetime.strptime('06:00 PM', '%I:%M %p')
        timeStamp13 = datetime.strptime('06:01 PM', '%I:%M %p')
        timeStamp14 = datetime.strptime('09:00 PM', '%I:%M %p')
        timeStamp15 = datetime.strptime('09:01 PM', '%I:%M %p')
        timeStamp16 = datetime.strptime('11:59 PM', '%I:%M %p')
        timeStamp17 = datetime.strptime('12:00 AM', '%I:%M %p')
        
        
        totalCrimeCount = 0
        addressRegEx = r"[N|E|W|S][ ].[A-Za-z 0-9]+.[ ][S][T]|[O][F].[A-Za-z 0-9]+.[ ][S][T]|[O][F].[A-Za-z 0-9]+.[ ][R][D]|[O][F].[A-Za-z 0-9]+.[ ][A][V]|[O][F].[A-Za-z 0-9]+.[ ][P][L]"
        
        print 'lat %f, lon %f, radius %f' % (lat,lon,radius)
        r = urllib2.urlopen('https://api.spotcrime.com/crimes.json?lat=37.334164&lon=-121.884301&radius=0.02&key=.')

        crimeData = json.load(r)['crimes']
        for crimeDataVar in crimeData:
            #increment counter for total crime count
            totalCrimeCount = totalCrimeCount + 1
            
            #get crime type and add count of that crime type
            crimeType = crimeDataVar['type']
            if crimeType in crimeDataResponseVar['crime_type_count']:
                crimeDataResponseVar['crime_type_count'][crimeType] = crimeDataResponseVar['crime_type_count'][crimeType] + 1
            else:
                crimeDataResponseVar['crime_type_count'][crimeType] = 1


            #get street from address; classify it and increment count correspondingly
            match = re.search(addressRegEx, crimeDataVar['address'])
            if match is not None:
                streetList = re.findall(addressRegEx, crimeDataVar['address'])

                #logic for most 3 most dangerous streets
                for streetName in streetList:
                    if "OF ".find(streetName):
                        streetName = streetName.strip("OF ")

                    if streetName in crimeAddressTemp:
                        crimeAddressTemp[streetName] += 1
                    else:
                        crimeAddressTemp[streetName] = 1


            
            #get time from date; classify it and increment count correspondingly
            crimeTime = crimeDataVar['date'][9:]
            date_object = datetime.strptime(crimeTime,'%I:%M %p')
            if timeStamp1 <= date_object <= timeStamp2:
                crimeDataResponseVar['event_time_count']['12:01am-3am'] += 1
            elif timeStamp3 <= date_object <= timeStamp4:
                crimeDataResponseVar['event_time_count']['3:01am-6am'] += 1
            elif timeStamp5 <= date_object <= timeStamp6:
                crimeDataResponseVar['event_time_count']['6:01am-9am'] += 1
            elif timeStamp7 <= date_object <= timeStamp8:
                crimeDataResponseVar['event_time_count']['9:01am-12noon'] += 1
            elif timeStamp9 <= date_object <= timeStamp10:
                crimeDataResponseVar['event_time_count']['12:01pm-3pm'] += 1
            elif timeStamp11 <= date_object <= timeStamp12:
                crimeDataResponseVar['event_time_count']['3:01pm-6pm'] += 1
            elif timeStamp13 <= date_object <= timeStamp14:
                crimeDataResponseVar['event_time_count']['6:01pm-9pm'] += 1
            elif timeStamp15 <= date_object <= timeStamp16:
                crimeDataResponseVar['event_time_count']['9:01pm-12midnight'] += 1
            elif date_object == timeStamp17:
                crimeDataResponseVar['event_time_count']['9:01pm-12midnight'] += 1
            
        #add total crime var in response
        crimeDataResponseVar['total_crime'] = totalCrimeCount
        #add 3 most dangorous streets to response
        crimeDataResponseVar['the_most_dangerous_streets'] = sorted(crimeAddressTemp, key=lambda k: (crimeAddressTemp[k], k), reverse=True)[:3]
        #crimeDataResponseVar['the_most_dangerous_streets'] = sorted(crimeAddressTemp,key=crimeAddressTemp.get,reverse=True)[:3]

        print crimeDataResponseVar
        return crimeDataResponseVar
            
application = Application([CheckCrimeService],
    tns='spyne.examples.checkCrime',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)

if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    from wsgiref.simple_server import make_server
    logging.basicConfig(level=logging.DEBUG)
    
    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    logging.info("listening to http://127.0.0.1:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")
    server.serve_forever()
