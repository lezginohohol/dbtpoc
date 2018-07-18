
import jsonschema
import argparse
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
schema = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID,
                                             range=RANGE_NAME).execute().get('values', [])

# removing headers line
schema.pop(0)

column_index = 0
type_index = 1
mode_index = 2

# dict for datasheets
subtables = {}

for l in schema:
    subtable, subcol = l[column_index].split("/")
    subtabledict = subtables.get(subtable)
    if subtabledict:
        subtabledict.update({subcol: {"type": l[type_index],
                                      "mode": l[mode_index]}})
    else:
        subtables.update({subtable: {subcol: {'type': l[type_index],
                                              'mode': l[mode_index]}}})

pprint(subtables)

class FKeyValidator():
    pass

def keytype(x):
    return ("FK", x)

def keytyperesolved(x):
    return ("FKR",x)

formatted_types = {
    "K": lambda x : stdtypes[x],
    "FK": lambda x: keytype(x),
    "FKR": lambda x: keytyperesolved(x)
}

stdtypes = {
    "String": "string",
    "Date": "string",
    "TZ": "string",
    "Integer": "integer",
    "DatetimeJSON": "string",
    "Float": "number",
    "Array": "array"
}

# creating schemas for each datasheet
subschemas = {}
for table, fields in subtables.items():
    subschema = {
        "type": "object",
        "properties": {},
        "required": [],
    }
    for field, fieldinfo in fields.items():
        if ":" in fieldinfo['type']:
            pref, val = fieldinfo['type'].split(':')
            type = formatted_types[pref](val)
        else:
            type = stdtypes[fieldinfo['type']]
        subschema["properties"][field]= {"type": type}
        if fieldinfo['mode'] == "REQUIRED":
            subschema["required"].append(field)
    subschemas[table] = subschema

# setting types for foreign keys (just type of field) and for resolved foreign
# keys (inserting subschema for used datasheet)
for subschema in subschemas:
    for prop, proptype in subschemas[subschema]["properties"].items():
        if isinstance(proptype["type"], tuple):
            if proptype["type"][0] == "FK":
                schema, refprop = proptype["type"][1].split("/")
                subschemas[subschema]["properties"][prop]["type"] = subschemas[schema]["properties"][refprop]["type"]
            elif proptype["type"][0] == "FKR":
                schema, refprop = proptype["type"][1].split("/")
                subschemas[subschema]["properties"][prop] = subschemas[schema]


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
