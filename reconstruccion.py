import json

# Supongamos que el JSON está en un archivo llamado 'flujo.json'
with open('output234.json', 'r') as file:
    lines = file.readlines()

# Parsear cada línea como un objeto JSON
nodos = [json.loads(line) for line in lines]

# Crear un diccionario para mapear cada nodo por su businessLogicChildKey
nodos_dict = {nodo['businessLogicChildKey']: nodo for nodo in nodos}

# Crear un diccionario para almacenar las relaciones padre-hijo
relaciones = {}
for nodo in nodos:
    parent_key = nodo['businessLogicParentKey']
    child_key = nodo['businessLogicChildKey']
    if parent_key not in relaciones:
        relaciones[parent_key] = []
    relaciones[parent_key].append(child_key)

# Función para reconstruir el flujo desde un nodo dado y almacenarlo en una estructura de datos

with open('output_prueba.json', 'w') as file:
    json.dump(relaciones, file, indent=4)

def reconstruir_flujo(nodo_key, nivel=0):
    nodo = nodos_dict.get(nodo_key)
    if not nodo:
        return None

    if nodo['component'] == 'CodeFactoryAction':
        nodo_info = {
            "component": nodo['component'],
            "componentKeyGenerated": nodo['componentKeyGenerated'],
            "businessLogicParentKey": nodo['businessLogicParentKey'],
            "businessLogicChildKey": nodo['businessLogicChildKey'],
            "titulo": nodo['meta']['title'],
            'complexVariables': nodo['meta']['codeFactoryActionComplexVariables'] or None,
            'simpleVariables': nodo['meta']['codeFactoryActionSimpleVariables'] or None,
            "position": (nodo['number_x'], nodo['number_y']),
            "onSuccess": nodo['onSuccess'],
            "children": []
        }
    else:
    # Crear un diccionario para el nodo actual
        nodo_info = {
            "component": nodo['component'],
            "componentKeyGenerated": nodo['componentKeyGenerated'],
            "businessLogicParentKey": nodo['businessLogicParentKey'],
            "businessLogicChildKey": nodo['businessLogicChildKey'],
            "titulo": nodo['meta']['title'] or None,
            "position": (nodo['number_x'], nodo['number_y']),
            "onSuccess": nodo['onSuccess'],
            "children": []
        }

    # Recorrer los hijos del nodo actual
    for hijo_key in relaciones.get(nodo_key, []):
        hijo_info = reconstruir_flujo(hijo_key, nivel + 1)
        if hijo_info:
            nodo_info["children"].append(hijo_info)

    return nodo_info


# Encontrar el nodo raíz (el que no tiene padre)
nodos_raiz = [
    nodo for nodo in nodos if nodo['businessLogicParentKey'] not in nodos_dict]

# Reconstruir el flujo desde cada nodo raíz y almacenarlo en una lista
flujos = []
if nodos_raiz:
    for nodo_raiz in nodos_raiz:
        flujo = reconstruir_flujo(nodo_raiz['businessLogicChildKey'])
        flujos.append(flujo)
else:
    print("No se encontraron nodos raíz.")

# Guardar la estructura de datos en un archivo JSON
with open('flujo_reconstruido.json', 'w') as file:
    json.dump(flujos, file, indent=4)

print("El flujo ha sido guardado en 'flujo_reconstruido.json'.")
