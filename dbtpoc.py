
import jsonschema
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from pprint import pprint

SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
store = file.Storage('credentials.json')
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
    creds = tools.run_flow(flow, store)
service = build('sheets', 'v4', http=creds.authorize(Http()))

SPREADSHEET_ID = '1gatrRk-pNlDwW9fUIrLi78b8sSKZmw6PtjFJwUpi9pM'
RANGE_NAME = 'Schema'
result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID,
                                             range=RANGE_NAME).execute()
values = result.get('values', [])
values.pop(0)
column_index = 0
type_index = 1
mode_index = 2
descr_index = 3

subtables = {}

for l in values:
    subtable, subcol = l[column_index].split('/')
    subtabledict = subtables.get(subtable)
    if subtabledict:
        subtabledict.update({subcol: {'type': l[type_index],
                                      'mode': l[mode_index],
                                      'desc': l[descr_index] if len(l)>3 else ''}})
    else:
        subtables.update({subtable: {subcol: {'type': l[type_index],
                                      'mode': l[mode_index],
                                      'desc': l[descr_index] if len(l)>3 else ''}}})

pprint(subtables)

for subtable in subtables:
    pprint(subtable)
    pprint(subtables[subtable])

    print("DATA : \n")

    subtableval = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID,
                                             range=subtable).execute().get('values',[])
    keys = subtableval.pop(0)
    pprint(subtableval)
    for line in subtableval:
        record = {x:y for x, y in zip(keys, line)}
        pprint(record)



type_dict = {
    'K:String'
    'FK:address/id'
    'String'
    'FK:ur/id'
    'FKR:tr/id'
    'FKR:points/id'
    'Date'
    'TZ'
    'FKR:taxes/id'
    'K:Integer'
    'DatetimeJSON'
    'FKR:last_update/id'
    'FKR:ur/id'
    'Float'
    'Array'
    'Array'

}

# import gspread
# from oauth2client.service_account import ServiceAccountCredentials
# scope = ["https://spreadsheets.google.com/feeds"]
# creds = ServiceAccountCredentials.from_json_keyfile_name('dbtpoc.json', scope)
# client = gspread.authorize(creds)
# spname = client.open("1avw3QM7NYoWAOz7BNqWMU_W6fulgeZyCglpYAg7Efy8")