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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
INPUT_FILE = 'JSON27K_output_cleaned.json'

OUTPUT_FILE = 'output_table.csv'
# INPUT_FILE = 'JSON14K.json'

# OUTPUT_FILE = 'output.csv'

ACTION_TERMS = {
    "OnSuccess": "OnSuccess",
    "HIDE": "HIDE",
    "BLOCK": "BLOCK",
    "UNBLOCK": "UNBLOCK",
    "SHOW": "SHOW"
}


def transform_json_to_table(data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Transform JSON data into a structured DataFrame.

    Args:
        data: Dictionary containing businessLogicList and componentList

    Returns:
        DataFrame containing the transformed data or None if invalid input
    """
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
                "INPUT": "No disponible",
                "OUTPUT": "No disponible",
                "TIPO VAR": "No disponible",
                "TIPO DATO": "No disponible",
                "CODIGO": bl.get('validatePreviousFieldsAndActions', 'No disponible')
            }
            rows.append(row)

        # Extract componentList data
        for comp in data['componentList']:
            tipo_var_list = []
            tipo_dato_list = []
            input_var_list = []
            input_dato_list = []
            if_var_list = []
            if_dato_list = []
            meta = comp.get('meta', {})

            # Process Simple Variables output
            for simple_var in meta.get('codeFactoryActionSimpleVariables', []):
                for var in simple_var.get('variables', []):
                    tipo_var_list.append(var.get('propertyName', 'No disponible'))
                    tipo_dato_list.append(var.get('dataType', 'No disponible'))

            # Process Complex Variables output
            for complex_var in meta.get('codeFactoryActionComplexVariables', []):
                for var in complex_var.get('variables', []):
                    tipo_var_list.append(var.get('propertyName', 'No disponible'))
                    tipo_dato_list.append(var.get('dataType', 'No disponible'))

            # Process Action Parameters input

            for Action_var in meta.get('codeFactoryActionParameters', []):
                input_var_list.append(Action_var.get('parameterAlias', 'NO disponible'))
                input_dato_list.append(Action_var.get('dataType', 'No disponible'))


            # Create rows for each variable combination
            if not (tipo_var_list or input_var_list):  # If no variables found, create one default row
                rows.append({
                    "FLUJO": comp.get('specification', {}).get('specificationId', 'No disponible'),
                    "PAGINA": "No disponible",
                    "ESTATUS": "No disponible",
                    "COMPONENTE": comp.get('component', 'No disponible'),
                    "TITULO COMPONENTE": meta.get('title', 'No disponible'),
                    "ACCION": meta.get('title', 'No disponible'),
                    "BL": comp.get("componentKeyGenerated", "No disponible"),
                    "NODO": comp.get("componentKeyGenerated", "No disponible"),
                    "INPUT": "No disponible",
                    "OUTPUT": meta.get('dataLockDependencyList',"No disponible"),
                    "TIPO VAR": "No disponible",
                    "TIPO DATO": "No disponible",
                    "CODIGO": meta.get('stringFunction', 'No disponible')
                })
            elif (input_var_list and tipo_var_list):

                for var_input, dato_input in zip(input_var_list, input_dato_list):
                    rows.append({
                        "FLUJO": comp.get('specification', {}).get('specificationId', 'No disponible'),
                        "PAGINA": "No disponible",
                        "ESTATUS": "No disponible",
                        "COMPONENTE": comp.get('component', 'No disponible'),
                        "TITULO COMPONENTE": meta.get('title', 'No disponible'),
                        "ACCION": "No disponible",
                        "BL": comp.get("componentKeyGenerated", "No disponible"),
                        "NODO": comp.get('componentKeyGenerated', 'No disponible'),
                        "INPUT": True,
                        "OUTPUT": 'Si lo arroja',
                        "TIPO VAR": var_input,
                        "TIPO DATO": dato_input,
                        "CODIGO": meta.get('stringFunction', 'No disponible')
                    })
                for tipo_var, tipo_dato in zip(tipo_var_list, tipo_dato_list):
                    rows.append({
                        "FLUJO": comp.get('specification', {}).get('specificationId', 'No disponible'),
                        "PAGINA": "No disponible",
                        "ESTATUS": "No disponible",
                        "COMPONENTE": comp.get('component', 'No disponible'),
                        "TITULO COMPONENTE": meta.get('title', 'No disponible'),
                        "ACCION": "No disponible",
                        "BL": comp.get("componentKeyGenerated", "No disponible"),
                        "NODO": comp.get('componentKeyGenerated', 'No disponible'),
                        "INPUT": 'No disponible',
                        "OUTPUT": True,
                        "TIPO VAR": tipo_var,
                        "TIPO DATO": tipo_dato,
                        "CODIGO": meta.get('stringFunction', 'No disponible')
                    })



        return pd.DataFrame(rows)

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return None


def main() -> None:
    """Main execution function."""
    try:
        input_path = Path(INPUT_FILE)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        logger.info(f"Reading input file: {input_path}")
        with open(input_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        df_table = transform_json_to_table(data)
        if df_table is None:
            raise ValueError("Failed to transform data")

        output_path = Path(OUTPUT_FILE)
        df_table.to_csv(output_path, index=False)
        logger.info(f"Table exported to '{output_path}'")

    except json.JSONDecodeError:
        logger.error("Invalid JSON format in input file")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
