
import jsonschema, json
import argparse, os, ast
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from copy import deepcopy


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

unresolvedschemas = deepcopy(subschemas)

def removeitem(dict, itemkey):
    dict["properties"].pop(itemkey)
    return dict

# setting types for foreign keys (just type of field) and for resolved foreign
# keys (inserting subschema for used datasheet)

unresolvedschemas = deepcopy(subschemas)

def resolvekeys(subschema, subschemas, unresolved=False):
    for prop, proptype in subschema["properties"].items():
        if isinstance(proptype, tuple):
            if proptype[0] == "FK":
                schema, refprop = proptype[1].split("/")
                subschema["properties"][prop] = subschemas[schema]["properties"][refprop]
            elif proptype[0] == "FKR":
                schema, refprop = proptype[1].split("/")
                if unresolved:
                    subschema["properties"][prop] = subschemas[schema]["properties"][refprop]
                else:
                    schematoinsert = subschemas[schema]
                    schematoinsert["properties"].pop(refprop)
                    schematoinsert["required"].pop(schematoinsert["required"].index(refprop))
                    subschema["properties"][prop] = resolvekeys(schematoinsert, subschemas)
    return subschema

for subschema in subschemas:
    subschemas[subschema] = resolvekeys(subschemas[subschema], subschemas)

for subschema in unresolvedschemas:
    unresolvedschemas[subschema] = resolvekeys(unresolvedschemas[subschema], unresolvedschemas, True)


# retrieving records from sheets
def get_records(subtable, schemasdict, spr_sh_id, sheetschema, resolve=True, field_name=None, record_id=None):
    records = []
    tablevalues = service.spreadsheets().values().get(spreadsheetId=spr_sh_id,
                                                      range=subtable).execute().get('values', [])
    keys = tablevalues.pop(0)
    pd = schemasdict[subtable]["properties"]
    for line in tablevalues:
        tableline = {x: y for x, y in zip(keys, line)}
        for key, value in tableline.items():
            if key in pd.keys():
                if pd[key]["type"] == "object":
                    referencetable, referencefield = sheetschema[subtable][key]["type"].split(":")[1].split('/')
                    if resolve:
                        tableline[key] = get_records(referencetable, schemasdict, spr_sh_id, sheetschema, resolve, referencefield, value)
                elif pd[key]["type"] == "array":
                    tableline[key] = ast.literal_eval(value)
                elif pd[key]["type"] == "integer":
                    tableline[key] = int(value)
                elif pd[key]["type"] == "number":
                    tableline[key] = float(value)
        records.append(tableline)
    if record_id:
        recdict = [rec for rec in records if rec[field_name] == record_id].pop()
        recdict.pop(field_name)
        return recdict
    return records

resolvedrecords = {}
for subtable in subtables:
    resolvedrecords[subtable] = get_records(subtable, subschemas, SPREADSHEET_ID, subtables)

records = {}
for subtable in subtables:
    records[subtable] = get_records(subtable, subschemas, SPREADSHEET_ID, subtables, False)

# create json-schemas and jsonfiles

#resolved schemas
for schemaname, schema in subschemas.items():
    if not os.path.exists('schemas'):
        os.makedirs('schemas')
    if not os.path.exists('schemas/resolved'):
        os.makedirs('schemas/resolved')
    with open('schemas/resolved/' + schemaname + '_schema.json', "w") as outfile:
        json.dump(schema, outfile)

#unresolved schemas
for schemaname, schema in unresolvedschemas.items():
    if not os.path.exists('schemas/unresolved'):
        os.makedirs('schemas/unresolved')
    with open('schemas/unresolved/' + schemaname + '_schema.json', "w") as outfile:
        json.dump(schema, outfile)

JSON_to_BQ = {
    "string": {"type": "STRING"},
    "number": {"type": "FLOAT"},
    "integer": {"type": "INTEGER"},
    "array": {"type": "STRING", "mode": "REPEATED"},
    "object": {"type": "RECORD"}
}


def bigqueryjson(schema):
    '''
    :param schema: json schema to be turned in bigquery fieldlist.
                   must be dict with subdict "properties" and list "required"
    :return:  list of dicts each item is field dict
    '''
    bq = []
    for property, propdict in schema["properties"].items():
        pdict = {"name": property}
        pdict.update(JSON_to_BQ[propdict["type"]])
        if propdict["type"] != "array":
            if property in schema["required"]:
                pdict["mode"] = "REQUIRED"
            else:
                pdict["mode"] = "NULLABLE"
        if propdict["type"] == "object":
            pdict["fields"] = bigqueryjson(propdict)
        bq.append(pdict)
    return bq

for schemaname, schema in unresolvedschemas.items():
    bq = bigqueryjson(schema)
    if not os.path.exists('schemas/BigQuery_flat'):
        os.makedirs('schemas/BigQuery_flat')
    with open('schemas/BigQuery_flat/' + schemaname + '_schema.json', "w") as outfile:
        json.dump(bq, outfile)

for schemaname, schema in subschemas.items():
    bq = bigqueryjson(schema)
    if not os.path.exists('schemas/BigQuery'):
        os.makedirs('schemas/BigQuery')
    with open('schemas/BigQuery/' + schemaname + '_schema.json', "w") as outfile:
        json.dump(bq, outfile)

for recordlist in records:
    if not os.path.exists('records'):
        os.makedirs('records')
    if not os.path.exists('records/subrecord_id'):
        os.makedirs('records/subrecord_id')
    with open('records/subrecord_id/' + recordlist + '_data.json', "w") as outfile:
        data = json.dumps(records[recordlist])
        json.dump(records[recordlist], outfile)

for recordlist in resolvedrecords:
    if not os.path.exists('records/resolved'):
        os.makedirs('records/resolved')
    with open('records/resolved/' + recordlist + '_data.json', "w") as outfile:
        data = json.dumps(records[recordlist])
        json.dump(records[recordlist], outfile)

for recordlist in records:
    if not os.path.exists('records/csv'):
        os.makedirs('records/csv')
    with open('records/csv/' + recordlist + '_data.csv', "w") as outfile:
        keyslist = [key for key in records[recordlist][0].keys()]
        outfile.write(",".join(keyslist)+"\n")
        for record in records[recordlist]:
            for key in keyslist:
                outfile.write(str(record[key])+",")
            outfile.write('\n')