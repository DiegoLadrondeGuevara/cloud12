import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):

    # URL de los últimos sismos del IGP
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    response = requests.get(url)
    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": "Error al acceder a la página del IGP"
        }

    soup = BeautifulSoup(response.content, "html.parser")

    # Buscar tabla
    table = soup.find("table")
    if not table:
        return {
            "statusCode": 404,
            "body": "No se encontró la tabla en la web del IGP"
        }

    # Leer encabezados
    headers = [th.text.strip() for th in table.find_all("th")]

    # Extraer filas (solo los primeros 10 registros)
    rows = []
    trs = table.find_all("tr")[1:11]  # los 10 primeros sismos

    for tr in trs:
        cells = tr.find_all("td")
        row = {}
        for i, cell in enumerate(cells):
            row[headers[i]] = cell.text.strip()
        
        # Agregar ID único
        row["id"] = str(uuid.uuid4())
        rows.append(row)

    # Guardar en DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table_dynamo = dynamodb.Table("TablaSismosIGP")

    # Borramos la tabla primero
    scan = table_dynamo.scan()
    with table_dynamo.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # Insertamos los nuevos
    with table_dynamo.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)

    return {
        "statusCode": 200,
        "body": rows
    }
