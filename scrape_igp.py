import requests
import boto3
import uuid
import json

def lambda_handler(event, context):

    url = "https://ultimosismo.igp.gob.pe/api/v1/sismos?limit=10"

    try:
        response = requests.get(url)
        data = response.json()
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    sismos = data.get("data", [])

    # añadir ID único
    for s in sismos:
        s["id"] = str(uuid.uuid4())

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("TablaSismosIGP")

    # limpiar tabla
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # insertar nuevos
    with table.batch_writer() as batch:
        for item in sismos:
            batch.put_item(Item=item)

    # 👇 ESTA ES LA PARTE CRÍTICA — body DEBE SER JSON STRING
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(sismos)
    }
