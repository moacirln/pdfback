import psycopg2

def get_connection():
    try:
        conn = psycopg2.connect(
            host="interchange.proxy.rlwy.net",
            port=21596,
            database="railway",
            user="postgres",
            password="deiYRuSStHtAOTBBhHLZvDVLjUAeHNwI"
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return None

