import io
import cv2
import numpy as np
import pytesseract
import pdfplumber
import time
import psycopg2
from postgres.hooks import fetch_envios, fetch_models, fetch_marks, insert_processados, update_envio
from postgres.config import get_connection
from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


def extract_text_from_pdf(pdf_bytes, marks):
    """
    pdf_bytes: conteúdo do PDF em bytes (vindo do Postgres)
    marks: lista de tuplas (id, x0, top, width, height, label, ...)
    Retorna uma lista de dicionários: [{'label': ..., 'text': ..., 'method': ...}, ...]
    """

    """
    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe
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
    models = fetch_models()

    for envio in envios:
        print("Iterando sobre envios pendentes")
        id_envio = envio[0]
        arquivo_bytes = envio[3]

        if envio[1] != 21:
            print(f"Id do modelo presente: {envio[1]}")
            id_model = envio[1]
            marks = fetch_marks(id_model)
            print(f"Marcações adquiridas: {marks}")
            result = extract_text_from_pdf(arquivo_bytes, marks)
            print(f"Extração feita: {result}")

            try:
                insert_processados(id_envio, id_model, result)
                update_envio(id_envio)
                print(f"Sucesso na atualização do banco de dados")
            except Exception as e:
                update_envio(id_envio)
                insert_processados(id_envio, id_model, {"resultado": "Falha, motivo desconhecido"})
                print(f"Erro ao processar envio {id_envio}: {e}")

        else:
            print(f"Sem id_model")
            referencia = extract_text_from_pdf(arquivo_bytes, [[1, 0.0541130, 0.048417, 0.89075, 0.0861173, "referencia"]])
            print(f"Referência adquirida: {referencia}")


            modelos_filtrados = sorted(
                [(m, similar(str(m[5]).strip(), str(referencia).strip())) for m in models],
                key=lambda x: x[1],
                reverse=True
            )
            print(f"Modelos filtrados: {modelos_filtrados}")

            if not modelos_filtrados or modelos_filtrados[0][1] < 0.8:
                print(f"Nenhum modelo satisfatório para {referencia}")
                update_envio(id_envio)
                insert_processados(id_envio, 21, {"resultado":"Falha, sem modelos compatíveis"})
                continue

            id_model = modelos_filtrados[0][0][0]
            print(f"Melhor modelo: {modelos_filtrados[0][0][5]} (similaridade {modelos_filtrados[0][1]:.2f})")

            marks = fetch_marks(id_model)
            result = extract_text_from_pdf(arquivo_bytes, marks)

            print(marks)
            print(result)

            try:
                insert_processados(id_envio, id_model, result)
                update_envio(id_envio)
                print(f"Atualização do Banco de dados com sucesso")
            except Exception as e:
                update_envio(id_envio)
                insert_processados(id_envio, 21, {"resultado": "Falha, motivo desconhecido"})
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
