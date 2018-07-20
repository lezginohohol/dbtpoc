
import jsonschema, json
import argparse, os, ast
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

parser = argparse.ArgumentParser()
parser.add_argument("spreadsheet_id")
parser.add_argument("schema_sheet")
args = parser.parse_args()

SPREADSHEET_ID = args.spreadsheet_id
RANGE_NAME = args.schema_sheet

SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
store = file.Storage('credentials.json')
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
    creds = tools.run_flow(flow, store)
service = build('sheets', 'v4', http=creds.authorize(Http()))
# SPREADSHEET_ID = '1kEC9nqNKq-COC3ZbCqKW4hsdf3wzLzEPgXcTXcCeOBM'
# RANGE_NAME = 'schema'
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
    "String": {"type": "string"},
    "Date": {"type": "string", "pattern": "[0-9]{4}-[0-9]{2}-[0-9]{2}"},
    "TZ": {"type": "string"},
    "Integer": {"type": "integer"},
    "DatetimeJSON": {"type": "string", "pattern": "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\+[0-9]{2}:[0-9]{2}"},
    "Float": {"type": "number"},
    "Array": {"type": "array"}
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
            typedef = formatted_types[pref](val)
        else:
            typedef = stdtypes[fieldinfo['type']]
        subschema["properties"][field]= typedef
        if fieldinfo['mode'] == "REQUIRED":
            subschema["required"].append(field)
    subschemas[table] = subschema

# setting types for foreign keys (just type of field) and for resolved foreign
# keys (inserting subschema for used datasheet)
for subschema in subschemas:
    for prop, proptype in subschemas[subschema]["properties"].items():
        if isinstance(proptype, tuple):
            if proptype[0] == "FK":
                schema, refprop = proptype[1].split("/")
                subschemas[subschema]["properties"][prop] = subschemas[schema]["properties"][refprop]
            elif proptype[0] == "FKR":
                schema, refprop = proptype[1].split("/")
                subschemas[subschema]["properties"][prop] = subschemas[schema]

# retrieving records from sheets

#
def get_records(subtable, schemasdict, spr_sh_id, sheetschema, field_name = None, record_id=None):
    records = []
    tablevalues = service.spreadsheets().values().get(spreadsheetId=spr_sh_id,
                                                      range=subtable).execute().get('values', [])
    keys = tablevalues.pop(0)
    for line in tablevalues:
        tableline = {x: y for x, y in zip(keys, line)}
        for key, value in tableline.items():
            if schemasdict[subtable]["properties"][key]["type"] == "object":
                referencetable, referencefield = sheetschema[subtable][key]["type"].split(":")[1].split('/')
                tableline[key] = get_records(referencetable, schemasdict, spr_sh_id, sheetschema, referencefield, value)
            elif schemasdict[subtable]["properties"][key]["type"] == "array":
                tableline[key] = ast.literal_eval(value)
            elif schemasdict[subtable]["properties"][key]["type"] == "integer":
                tableline[key] = int(value)
            elif schemasdict[subtable]["properties"][key]["type"] == "number":
                tableline[key] = float(value)
        records.append(tableline)

    if record_id:
        if schemasdict[subtable]["properties"][field_name]["type"] == "integer":
            record_id = int(record_id)
        return [rec for rec in records if rec[field_name] == record_id].pop()
    return records

records = {}
for subtable in subtables:
    records[subtable] = get_records(subtable, subschemas, SPREADSHEET_ID, subtables)

# create json-schemas and jsonfiles

jsonschema.validate(records["location"][0], subschemas["location"])


for schemaname, schema in subschemas.items():
    if not os.path.exists('schemas'):
        os.makedirs('schemas')
    with open('schemas/' + schemaname + '_schema.json', "w") as outfile:
        json.dump(schema, outfile)

for recordlist in records:
    if not os.path.exists('records'):
        os.makedirs('records')
    with open('records/' + recordlist + '_data.json', "w") as outfile:
        data = json.dumps(records[recordlist])
        json.dump(records[recordlist], outfile)
