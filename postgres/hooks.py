from postgres.config import get_connection
import json


def fetch_marks(id_model):
    marks = []
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM marcacoes WHERE id_model = %s", (id_model,))
            resultados = cur.fetchall()
            for row in resultados:
                marks.append(row)
            return marks
    finally:
        conn.close()

def fetch_models():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM modelos WHERE ativo=true ")
            return cur.fetchall()
    finally:
        conn.close()

def fetch_envios():
    envios = []

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM envios WHERE processado = false")
            resultados = cur.fetchall()
            for row in resultados:
                envios.append(row)
            return envios
    finally:
        conn.close()


def insert_processados(id_envio, id_model, dados):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            json_str = json.dumps(dados, ensure_ascii=False)

            cur.execute(
                "INSERT INTO processados (id_envio, id_model, dados) VALUES (%s, %s, %s)",
                (id_envio, id_model, json_str)
            )
            conn.commit()
    finally:
        conn.close()

def update_envio(id):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE envios SET processado = true WHERE id = %s",
                (id,)
            )
            conn.commit()
    finally:
        conn.close()


