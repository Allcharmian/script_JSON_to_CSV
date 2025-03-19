"""
JSON to DataFrame Transformer

This module transforms a JSON file containing business logic and component data
into a structured pandas DataFrame. It handles the extraction and organization of
business logic relationships and component details from a specific JSON format.

The module expects a JSON file with 'businessLogicList' and 'componentList' sections
and outputs a CSV file with the transformed data.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
INPUT_FILE = 'JSON_27K.json'

OUTPUT_FILE = 'output_table.csv'
# INPUT_FILE = 'JSON14K_output_cleaned.json'
OUTPUT_FILE2 = 'output.csv'
# OUTPUT_FILE = 'output.csv'

ACTION_TERMS = {
    "OnSuccess": "OnSuccess",
    "HIDE": "HIDE",
    "BLOCK": "BLOCK",
    "UNBLOCK": "UNBLOCK",
    "SHOW": "SHOW"
}

SET_PATTERN = r'(simpleVariables|complexVariables)\.set\(\s*[\'"](.*?)[\'"]\s*,\s*(.*?)\s*\)'

MODIFY_PATTERN = r'(simpleVariables|complexVariables)\.modify\(\s*[\'"](.*?)[\'"]\s*,\s*(.*?)\s*\)'
# with the ').get(' in the parameter
GET_PATTERN = r'const\s+(\w+)\s*:\s*(\w+)\s*=\s*_json\.get\(\s*[\'"](.*?)[\'"]\s*\)(?:\.get\(\s*[\'"](.*?)[\'"]\s*\))?;'
# the down line are incorrect
#GET_PATTERN = r'const (\w+): (\w+) = _json\.get\([\'"]([^\'"]*)[\'"]\)(?:\.get\([\'"]([^\'"]*)[\'"]\))?;'

def remove_specific_fields(data, field_name):
    if isinstance(data, dict):
        return {k: remove_specific_fields(v, field_name) for k, v in data.items() if k != field_name}
    elif isinstance(data, list):
        return [remove_specific_fields(item, field_name) for item in data if item != field_name]
    else:
        return data


# List of field names to be removed
FIELDS_TO_REMOVE = [
    'position', 'top', 'left', 'width', 'minWidth', 'height',
    'zIndex', 'minHeight', 'responsiveHidden', 'subOrder',
    'fieldId', 'FIELD', 'attemptsToGetContractGenerated', 'componentType', 'validatePreviousFieldsAndActions',
    'blockFields', 'blockWhileProcessing', 'blockFields', 'specificationId',
    'specification', 'businessLogicRelationship', 'componentKey', 'stringFunctionPreview'
]


def transform_json_to_table(data: Dict[str, Any]) -> Optional[pd.DataFrame]:

    if not isinstance(data, dict):
        logger.error("Input data must be a dictionary")
        return None

    if not all(key in data for key in ['businessLogicList', 'componentList']):
        logger.error("Missing required keys in input data")
        return None

    rows: List[Dict[str, str]] = []

    try:
        # Extract businessLogicList data
        for bl in data['businessLogicList']:
            row = {
                "FLUJO": "No disponible",
                "PAGINA": "No disponible",
                "ESTATUS": "No disponible",
                "COMPONENTE": bl.get('event', "No disponible"),
                "TITULO COMPONENTE": bl.get('event', 'No disponible'),
                "ACCION": ACTION_TERMS.get(bl.get('event', ''), 'No disponible'),
                "BL": bl.get("key", "No disponible"),
                "NODO": bl.get('key', 'No disponible'),
                "VARIABLE": "No disponible",
                "PARAMETRO UNO": "No disponible",
                "PARAMETRO DOS": "No disponible",
                "SET o MODIFY": "No disponible",
                "CODIGO": bl.get('validatePreviousFieldsAndActions', 'No disponible')
            }
            rows.append(row)

        # Extract componentList data
        for comp in data['componentList']:
            tipo_var_list = []
            parametros_list = []
            parametros2_list = []
            opcion_de_list = []
            set_matches = []
            modify_matches = []
            get_matches = []

            meta = comp.get('meta', {})

            if 'meta' in comp and 'stringFunction' in comp['meta']:
                code_section = comp['meta']['stringFunction']

                set_matches.extend(re.findall(SET_PATTERN, code_section))

                modify_matches.extend(re.findall(MODIFY_PATTERN, code_section))

                get_matches.extend(re.findall(GET_PATTERN, code_section))                

            for match in get_matches:
                tipo_var_list.extend({match[0]})
                parametros_list.extend({match[1]})
                parametros2_list.extend({match[2]})
                opcion_de_list.append('get')

            for match in set_matches:
                tipo_var_list.extend({match[0]})
                parametros_list.extend({match[1]})
                parametros2_list.extend({match[2]})
                opcion_de_list.append('set')

            for match in modify_matches:
                tipo_var_list.extend({match[0]})
                parametros_list.extend({match[1]})
                parametros2_list.extend({match[2]})
                opcion_de_list.append('modify')

            # Process Action Parameters input

            # Create rows for each variable combination
            # If no variables found, create one default row
            if not tipo_var_list:
                rows.append({
                    "FLUJO": comp.get('specification', {}).get('specificationId', 'No disponible'),
                    "PAGINA": "No disponible",
                    "ESTATUS": "No disponible",
                    "COMPONENTE": comp.get('component', 'No disponible'),
                    "TITULO COMPONENTE": meta.get('title', 'No disponible'),
                    "ACCION": meta.get('title', 'No disponible'),
                    "BL": comp.get("componentKeyGenerated", "No disponible"),
                    "NODO": comp.get("componentKeyGenerated", "No disponible"),
                    "VARIABLE": "No disponible",
                    "PARAMETRO UNO": "No disponible",
                    "PARAMETRO DOS": "NO disponible",
                    "SET o MODIFY": "No disponible",
                    "CODIGO": "No disponible"
                })
            else:
                for tipo_var, tipo_dato, tipo_dato2, tipo_opcion in zip(tipo_var_list, parametros_list, parametros2_list, opcion_de_list):
                    rows.append({
                        "FLUJO": comp.get('specification', {}).get('specificationId', 'No disponible'),
                        "PAGINA": "No disponible",
                        "ESTATUS": "No disponible",
                        "COMPONENTE": comp.get('component', 'No disponible'),
                        "TITULO COMPONENTE": meta.get('title', 'No disponible'),
                        "ACCION": "No disponible",
                        "BL": comp.get("componentKeyGenerated", "No disponible"),
                        "NODO": comp.get('componentKeyGenerated', 'No disponible'),
                        "VARIABLE": tipo_var,
                        "PARAMETRO UNO": tipo_dato,
                        "PARAMETRO DOS": tipo_dato2,
                        "SET o MODIFY": tipo_opcion,
                        "CODIGO": "No disponible"
                    })

        return pd.DataFrame(rows)
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return None


def main() -> None:

    try:
        input_path = Path(INPUT_FILE)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        logger.info(f"Reading input file: {input_path}")
        with open(input_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        for field in FIELDS_TO_REMOVE:
            cleaned_data = remove_specific_fields(data, field)
            if cleaned_data is None:
                raise ValueError("Failed to transform data")

        df_table = transform_json_to_table(cleaned_data)
        if df_table is None:
            raise ValueError("Failed to transform data")

        output_path = Path(OUTPUT_FILE)
        df_table.to_csv(output_path, index=False)
        logger.info(f"Table exported to '{output_path}'")

        output_path2 = Path(OUTPUT_FILE2)
        df_table.to_csv(output_path2, mode='a', index=False)
        logger.info(f"Table exported to '{output_path2}'")

    except json.JSONDecodeError:
        logger.error("Invalid JSON format in input file")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
