import io
import cv2
import numpy as np
import pytesseract
import pdfplumber
import time
import psycopg2
from postgres.hooks import fetch_envios, fetch_marks, insert_processados, update_envio
from postgres.config import get_connection


def extract_text_from_pdf(pdf_bytes, marks):
    """
    pdf_bytes: conteúdo do PDF em bytes (vindo do Postgres)
    marks: lista de tuplas (id, x0, top, width, height, label, ...)
    Retorna uma lista de dicionários: [{'label': ..., 'text': ..., 'method': ...}, ...]
    """

    extracted = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        first_page = pdf.pages[0]
        page_width = first_page.width
        page_height = first_page.height

        for mark in marks:
            _id = mark[0]
            x0_rel = mark[1]
            top_rel = mark[2]
            width_rel = mark[3]
            height_rel = mark[4]
            label = mark[5]

            x0 = max(0, x0_rel * page_width)
            top = max(0, top_rel * page_height)
            width = width_rel * page_width
            height = height_rel * page_height
            x1 = min(page_width, x0 + width)
            bottom = min(page_height, top + height)

            cropped = first_page.crop((x0, top, x1, bottom))

            texto_pdf = cropped.extract_text()

            if texto_pdf and texto_pdf.strip():
                texto = texto_pdf.strip()
                metodo = "PDF nativo"
            else:
                pil_image = cropped.to_image(resolution=300).original
                img_cv = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
                gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
                _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                texto = pytesseract.image_to_string(thresh, lang="por")
                metodo = "OCR"

            extracted.append({label: texto})

    return extracted


def processar():
    envios = fetch_envios()
    print(envios)

    for envio in envios:
        id_envio = envio[0]
        id_model = envio[1]
        arquivo_bytes = envio[3]

        marks = fetch_marks(id_model)
        result = extract_text_from_pdf(arquivo_bytes, marks)

        print(marks)
        print(result)

        try:
            insert_processados(id_envio, id_model, result)
            update_envio(id_envio)
        except Exception as e:
            print(f"Erro ao processar envio {id_envio}: {e}")


if __name__ == "__main__":
    conn = get_connection()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("LISTEN nova_linha;")
    print("Aguardando notificações...")

    while True:
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            print(f"Nova linha inserida com ID: {notify.payload}")
            processar()
        time.sleep(2)
