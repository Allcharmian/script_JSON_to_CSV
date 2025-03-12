import json

import pandas as pd

import numpy as np

# Función para eliminar campos


def remove_null_fields(data):
    if isinstance(data, dict):
        return {k: remove_null_fields(v) for k, v in data.items() if v is not None}
    elif isinstance(data, list):
        return [remove_null_fields(item) for item in data if item is not None]
    else:
        return data


def remove_arrayEmpty_fields(data):
    if isinstance(data, dict):
        return {k: remove_arrayEmpty_fields(v) for k, v in data.items() if v != []}
    elif isinstance(data, list):
        return [remove_arrayEmpty_fields(item) for item in data if item != []]
    else:
        return data


def remove_digit_fields(data):
    if isinstance(data, dict):
        return {k: remove_digit_fields(v) for k, v in data.items() if type(v) != int}
    elif isinstance(data, list):
        return [remove_digit_fields(item) for item in data if type(item) != int]
    else:
        return data


def remove_float_fields(data):
    if isinstance(data, dict):
        return {k: remove_float_fields(v) for k, v in data.items() if type(v) != float}
    elif isinstance(data, list):
        return [remove_float_fields(item) for item in data if type(item) != float]
    else:
        return data


def remove_false_fields(data):
    if isinstance(data, dict):
        return {k: remove_false_fields(v) for k, v in data.items() if v is not False}
    elif isinstance(data, list):
        return [remove_false_fields(item) for item in data if item is not False]
    else:
        return data


def remove_true_fields(data):
    if isinstance(data, dict):
        return {k: remove_true_fields(v) for k, v in data.items() if v is not True}
    elif isinstance(data, list):
        return [remove_true_fields(item) for item in data if item is not True]
    else:
        return data


def remove_specific_fields(data, field_name):
    if isinstance(data, dict):
        return {k: remove_specific_fields(v, field_name) for k, v in data.items() if k != field_name}
    elif isinstance(data, list):
        return [remove_specific_fields(item, field_name) for item in data if item != field_name]
    else:
        return data


# List of field names to be removed
fields_to_remove = [
    'position', 'top', 'left', 'width', 'minWidth', 'height',
    'zIndex', 'minHeight', 'responsiveHidden', 'id', 'subOrder',
    'fieldId', 'FIELD', 'attemptsToGetContractGenerated', 'componentType', 'validatePreviousFieldsAndActions',
    'blockFields', 'blockWhileProcessing', 'blockFields', 'specificationId',
    'specification', 'businessLogicRelationship', 'componentKey', ''
    # component, componentKey, componentKeyToJoin, componentKeyGenerated, componentType
]


# Leer el archivo JSON original
with open('JSON27K.json', 'r', encoding='utf-8') as file:
    datos = json.load(file)

# uso de pandas para ver los datos


# Limpiar los datos eliminando los campos con valor null, false,

cleaned_data = remove_null_fields(datos)
cleaned_data = remove_arrayEmpty_fields(cleaned_data)
# cleaned_data = remove_digit_fields(cleaned_data)
# cleaned_data = remove_float_fields(cleaned_data)

# cleaned_data = remove_objEmpty_fields(cleaned_data)


# Eliminar campos específicos
for field in fields_to_remove:
    cleaned_data = remove_specific_fields(cleaned_data, field)


logica_herencia = cleaned_data['businessLogicRelationshipList']
logica_component_list = cleaned_data['componentList']

component_herencia = logica_component_list + logica_herencia

component_herencia.sort(key=lambda x: x['number'])


# Guardar los datos limpios en un nuevo archivo JSON
with open('JSON27K_output_cleaned.json', 'w', encoding='utf-8') as file:
    json.dump(cleaned_data, file, ensure_ascii=False, indent=4)


with open('logica_herencia.json', 'w', encoding='utf-8') as f:
    json.dump(logica_herencia, f, ensure_ascii=False, indent=4)

with open('logica_component_list.json', 'w', encoding='utf-8') as f:
    json.dump(logica_component_list, f, ensure_ascii=False, indent=4)

with open('component_herencia.pdf', 'w', encoding='utf-8') as f:
    json.dump(component_herencia, f, ensure_ascii=False, indent=4)


data_list = component_herencia  # si es una cadena JSON válida
df = pd.DataFrame(data_list)

# Mostrar el DataFrame original


df1 = pd.DataFrame(logica_herencia)

df2 = pd.DataFrame(logica_component_list)

merged_df = pd.merge(df1, df2, left_on='businessLogicChildKey',
                     right_on='componentKeyGenerated', how='outer')



merged_df.sort_values(by=['number_y'], inplace=True)


merged_df.to_csv('output.csv', index=False, sep='\t')

merged_df.to_json('output234.json', orient='records', lines=True)

print("Datos limpios guardados en 'JSON27K_output.json'.")

# funcion para hacer archivo de texto
