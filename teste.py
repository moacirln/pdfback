import requests
import base64

# URL da sua API (ajuste se precisar)
url = "https://apipdf-production-a396.up.railway.app/api/envios"

# Token JWT (exemplo)
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MSwibm9tZSI6Ik1vYWNpciIsImVtYWlsIjoibW9hY2lybG5AZ21haWwuY29tIiwiaWF0IjoxNzYxMjIyMzU1fQ.siRVNDRbXn0fAkGqRo46hlU0j-dEg9zWQ19yZYZ6PK8"

# Lê o arquivo PDF e converte para base64
with open("Exemplo.pdf", "rb") as f:
    arquivo_base64 = base64.b64encode(f.read()).decode("utf-8")

# Corpo da requisição
payload = {
    "id_model": 8,  # ou o ID do modelo que quiser usar
    "arquivo": arquivo_base64
}

# Cabeçalhos
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}"
}

# Faz o POST
response = requests.post(url, json=payload, headers=headers)

# Mostra o resultado
print("Status:", response.status_code)
try:
    print("Resposta:", response.json())
except:
    print("Resposta:", response.text)
