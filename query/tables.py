from .colors import bcolors
from google.cloud.exceptions import NotFound

def tables(client, bigquery, customerProfileName, formatDate, customerProfileNativeID, customerProfileNetworkType):

    # Creamos el nombre de la tabla
    customName = f'{customerProfileName}-{customerProfileNetworkType}-{formatDate}-ID{customerProfileNativeID}'

    try :

        # Ejecutamos la direccion de la tabla
        table_id = f'big-query-421322.Query.{customName}'

        if not if_tbl_exists(client, table_id):

            # Creamos la nueva tabla
            schema = [
                bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
            ]

            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)

            print(bcolors.OKGREEN + f"The table '{customName}' has been created successfully." + bcolors.ENDC)


        else:
            print(bcolors.WARNING + f"Warning: The table '{customName}' already exists" + bcolors.ENDC)

        return customName

    except Exception as e:

        print(bcolors.FAIL + f'Error: {e}' + bcolors.ENDC)

# Verico que no exista otra tabla
def if_tbl_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False