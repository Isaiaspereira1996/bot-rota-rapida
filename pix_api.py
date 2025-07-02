# pix_api.py
from flask import Flask, request, jsonify, send_file
from py_pix import Pix
from io import BytesIO
import qrcode

app = Flask(__name__)

CHAVE_PIX = '46221483824'
NOME_RECEBEDOR = 'ROTA RAPIDA'
CIDADE_RECEBEDOR = 'SAO PAULO'

@app.route('/pix/copiacola')
def gerar_copia_cola():
    valor = request.args.get('valor', type=float)
    pix = Pix(
        key=CHAVE_PIX,
        name=NOME_RECEBEDOR,
        city=CIDADE_RECEBEDOR,
        value=valor
    )
    payload = pix.payload()
    return jsonify({"copia_e_cola": payload})

@app.route('/pix/qrcode')
def gerar_qrcode():
    valor = request.args.get('valor', type=float)
    pix = Pix(
        key=CHAVE_PIX,
        name=NOME_RECEBEDOR,
        city=CIDADE_RECEBEDOR,
        value=valor
    )
    payload = pix.payload()
    img = qrcode.make(payload)
    buf = BytesIO()
    img.save(buf)
    buf.seek(0)
    return send_file(buf, mimetype='image/png')

if __name__ == '__main__':
    app.run(port=5000)
