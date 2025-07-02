import os
import logging
import pandas as pd
import re
import tempfile
import requests
import json
import base64
from unidecode import unidecode
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler,
    CallbackQueryHandler
)

# --- Configurações Iniciais ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Constantes de arquivos e estados
USUARIOS_JSON = "usuarios.json"
PAGAMENTOS_JSON = "pagamentos_pendentes.json"
CADASTRO, MENU_CREDITOS = range(2)

# Token do Mercado Pago (substitua se necessário)
MERCADO_PAGO_TOKEN = "APP_USR-7932872917685276-070207-bd97e26d1c2b9e1a5d12b8e4a45d73d7-1029050718"

# Dicionário de abreviações
abreviacoes_nomes = {
    'joao s': 'joao simoes', 'dr': 'doutor', 'cel': 'coronel', 'alm': 'almirante',
    'prof': 'professor', 'gen': 'general', 'cap': 'capitao', 'dep': 'deputado',
    'sen': 'senador', 'gov': 'governador', 'pres': 'presidente', 'min': 'ministro',
    'pad': 'padre', 'pe': 'padre', 'mad': 'madre'
}


# --- Funções de Manipulação de Dados (JSON) ---
def carregar_dados_json(filepath):
    """Carrega dados de um arquivo JSON de forma segura."""
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}

def salvar_dados_json(filepath, data):
    """Salva dados em um arquivo JSON."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

# Carrega os dados na inicialização
usuarios = carregar_dados_json(USUARIOS_JSON)
pagamentos_pendentes = carregar_dados_json(PAGAMENTOS_JSON)


# --- Funções de Processamento de Planilha (sem alterações) ---
def expandir_abreviacoes(texto):
    texto = texto.lower()
    for abrev, completo in abreviacoes_nomes.items():
        texto = re.sub(r'\b' + re.escape(abrev) + r'\b', completo, texto)
    return texto

def normalize(texto):
    texto = str(texto).lower()
    texto = unidecode(texto)
    texto = expandir_abreviacoes(texto)
    texto = re.sub(r'[^a-z0-9 ]', '', texto)
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip()

def format_postal_code(pc_str):
    pc_str = str(pc_str).strip().replace('-', '')
    if len(pc_str) == 8 and pc_str.isdigit():
        return f"{pc_str[:5]}-{pc_str[5:]}"
    return pc_str

def dividir_endereco_completo(destination_raw):
    raw_dest = str(destination_raw).strip()
    cleaned_dest = re.sub(r'(?:,?\s*-\s*)?(?:São\s*Paulo|SP)?(?:,\s*\d{5}-?\d{3})?\s*$', '', raw_dest, flags=re.IGNORECASE).strip()
    cleaned_dest = re.sub(r'(?i)\b(r|rua|av|avenida|ua)\.?\s*', '', cleaned_dest).strip()
    
    match = re.match(r'^(.*?)[,\s]+(nº|numero|num|n°)?\s*(\d+[a-zA-Z]?)\s*(.*)$', cleaned_dest, re.IGNORECASE)
    
    if match:
        street_name = ' '.join(word.capitalize() for word in match.group(1).strip().rstrip(',').split())
        number = match.group(3).strip()
        complement = match.group(4).strip().lstrip(',-').strip()
        address_line_1 = f"{street_name}, {number}"
        address_line_2 = complement
        normalized_key = normalize(f"{street_name} {number}")
    else:
        address_line_1 = ' '.join(word.capitalize() for word in cleaned_dest.split())
        address_line_2 = ""
        normalized_key = normalize(cleaned_dest)
        
    return pd.Series([address_line_1, address_line_2, normalized_key])

def corrigir_planilha_completo(df):
    df = df.rename(columns={
        'Sequence': 'Pacotes Na Parada',
        'Destination Address': 'Destination',
        'Zipcode/Postal code': 'Postal Code'
    })
    required_cols = ['Destination', 'Bairro', 'City', 'Postal Code', 'Pacotes Na Parada']
    for col in required_cols:
        if col not in df.columns:
            df[col] = ''
    
    df['Pacotes Na Parada'] = df['Pacotes Na Parada'].apply(lambda x: '0' if pd.isna(x) or str(x).strip() in ('', 'nan', '--') else str(x).strip())
    df[['Address Line 1', 'Address Line 2', 'Normalized_Street_Num_For_Group']] = df['Destination'].apply(dividir_endereco_completo)
    df['Postal Code Clean'] = df['Postal Code'].astype(str).str.replace('-', '').str.strip()
    df['Address Group'] = df['Normalized_Street_Num_For_Group'] + "_" + df['Postal Code Clean'] + "_" + df['Bairro'].apply(normalize) + "_" + df['City'].apply(normalize)

    def agrupar_pacotes(series):
        all_pacotes = []
        for p_str in series:
            cleaned_p_str = re.sub(r';\s*Total:\s*\d+\s*pacotes\.', '', str(p_str)).strip()
            all_pacotes.extend([item.strip() for item in cleaned_p_str.split(',') if item.strip()])
        unique_pacotes = sorted(list(set(all_pacotes)), key=lambda item: (int(item) if item.isdigit() else float('inf'), item))
        return ', '.join(unique_pacotes)

    df = df.sort_values(['Address Group', 'Address Line 1', 'Address Line 2'])
    agrupado = df.groupby('Address Group', sort=False).agg(
        Address_Line_1=('Address Line 1', 'first'), Address_Line_2=('Address Line 2', 'first'),
        Bairro=('Bairro', 'first'), City=('City', 'first'), Postal_Code_Clean=('Postal Code Clean', 'first'),
        Pacotes_Na_Parada=('Pacotes Na Parada', agrupar_pacotes)
    ).reset_index()
    agrupado['Postal Code'] = agrupado['Postal_Code_Clean'].apply(format_postal_code)
    agrupado = agrupado.rename(columns={'Address_Line_1': 'Address Line 1', 'Address_Line_2': 'Address Line 2', 'Pacotes_Na_Parada': 'Pacotes Na Parada'})
    return agrupado[['Address Line 1', 'Address Line 2', 'Bairro', 'City', 'Postal Code', 'Pacotes Na Parada']]


# --- Funções do Bot Telegram ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id in usuarios:
        creditos = usuarios.get(user_id, 0)
        await update.message.reply_text(f"""👋 Olá novamente! Você já está cadastrado.

💳 *Créditos disponíveis:* {creditos}.

Use /comprar para adquirir mais créditos ou envie sua planilha.""", parse_mode="Markdown")
        return ConversationHandler.END

    await update.message.reply_text("""👋 Seja bem-vindo ao RotaRápida!

Para começar, por favor, digite seu *primeiro nome* para se cadastrar e receber *1 crédito grátis*.""", parse_mode="Markdown")
    return CADASTRO

async def receber_nome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    nome = update.message.text.strip()
    if user_id in usuarios:
        await update.message.reply_text("⚠️ Você já está cadastrado.")
    else:
        usuarios[user_id] = 1
        salvar_dados_json(USUARIOS_JSON, usuarios)
        await update.message.reply_text(f"✅ {nome}, seu cadastro foi concluído!\n\nVocê ganhou *1 crédito grátis* para testar. Envie sua planilha ou use /comprar para adquirir mais.", parse_mode="Markdown")
    return ConversationHandler.END

async def comprar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("R$ 1.00 - 1 crédito", callback_data="comprar_1")],
        [InlineKeyboardButton("R$ 5.00 - 7 créditos (+40%)", callback_data="comprar_5")],
        [InlineKeyboardButton("R$ 10.00 - 15 créditos (+50%)", callback_data="comprar_10")],
        [InlineKeyboardButton("R$ 18.00 - 30 créditos (+66%)", callback_data="comprar_18")],
        [InlineKeyboardButton("🔙 Voltar", callback_data="voltar")]
    ]
    await update.message.reply_text("Escolha um pacote de créditos:", reply_markup=InlineKeyboardMarkup(keyboard))
    return MENU_CREDITOS

async def selecionar_credito(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "voltar":
        await query.edit_message_text("Operação cancelada.")
        return ConversationHandler.END

    valores = {"comprar_1": (1.00, 1), "comprar_5": (5.00, 7), "comprar_10": (10.00, 15), "comprar_18": (18.00, 30)}
    valor_reais, creditos = valores[query.data]
    user_id = str(query.from_user.id)

    await query.edit_message_text("⏳ Gerando sua cobrança PIX, por favor aguarde...")

    payload = {
        "transaction_amount": valor_reais,
        "description": f"Compra de {creditos} créditos para o bot RotaRápida",
        "payment_method_id": "pix",
        "payer": {"email": f"{user_id}@telegram.user"} # Email fictício, mas necessário
    }
    headers = {"Authorization": f"Bearer {MERCADO_PAGO_TOKEN}", "Content-Type": "application/json"}

    try:
        response = requests.post("https://api.mercadopago.com/v1/payments", headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()
        
        payment_id = result.get('id')
        qr_code_b64 = result['point_of_interaction']['transaction_data']['qr_code_base64']
        pix_copia_cola = result['point_of_interaction']['transaction_data']['qr_code']

        # Salva o pagamento como pendente para verificação futura
        pagamentos_pendentes[str(payment_id)] = {"user_id": user_id, "creditos": creditos}
        salvar_dados_json(PAGAMENTOS_JSON, pagamentos_pendentes)

        qr_image = base64.b64decode(qr_code_b64)
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=qr_image,
            caption=f"✅ Pagamento de R$ {valor_reais:.2f} gerado!\n\nEscaneie o QR Code acima ou use o 'Copia e Cola' abaixo."
        )
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"👇 *PIX Copia e Cola* 👇\n\n`{pix_copia_cola}`\n\nAssim que o pagamento for confirmado, você será notificado e seus créditos serão adicionados automaticamente.",
            parse_mode="Markdown"
        )
        await query.message.delete() # Remove a mensagem "Gerando sua cobrança..."

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na API MercadoPago: {e} - {e.response.text if e.response else 'No response'}")
        await query.edit_message_text("❌ *Erro ao gerar cobrança.*\n\nO Mercado Pago retornou um erro. Verifique se o token de acesso é válido e tente novamente mais tarde.", parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Erro inesperado ao gerar PIX: {e}")
        await query.edit_message_text("❌ Ocorreu um erro inesperado. Tente novamente.")
            
    return ConversationHandler.END

async def check_pending_payments(context: ContextTypes.DEFAULT_TYPE):
    """Tarefa que roda em segundo plano para verificar pagamentos pendentes."""
    if not pagamentos_pendentes:
        return

    headers = {"Authorization": f"Bearer {MERCADO_PAGO_TOKEN}"}
    # Itera sobre uma cópia, pois o dicionário pode ser modificado
    for payment_id in list(pagamentos_pendentes.keys()):
        try:
            response = requests.get(f"https://api.mercadopago.com/v1/payments/{payment_id}", headers=headers)
            if response.status_code == 200:
                payment_data = response.json()
                if payment_data.get('status') == 'approved':
                    info = pagamentos_pendentes[payment_id]
                    user_id = info['user_id']
                    creditos_a_adicionar = info['creditos']
                    
                    # Adiciona créditos ao usuário
                    usuarios[user_id] = usuarios.get(user_id, 0) + creditos_a_adicionar
                    salvar_dados_json(USUARIOS_JSON, usuarios)
                    
                    # Notifica o usuário
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"✅ Pagamento confirmado! *{creditos_a_adicionar} créditos* foram adicionados à sua conta.\n\nSeu novo saldo é: *{usuarios[user_id]}*.",
                        parse_mode="Markdown"
                    )
                    
                    # Remove da lista de pendentes
                    del pagamentos_pendentes[payment_id]
                    salvar_dados_json(PAGAMENTOS_JSON, pagamentos_pendentes)
                    logging.info(f"Pagamento {payment_id} para user {user_id} aprovado. Créditos adicionados.")

            elif response.status_code == 404: # Pagamento não encontrado ou expirado
                logging.warning(f"Pagamento {payment_id} não encontrado na API. Removendo da lista de pendentes.")
                del pagamentos_pendentes[payment_id]
                salvar_dados_json(PAGAMENTOS_JSON, pagamentos_pendentes)

        except Exception as e:
            logging.error(f"Erro ao verificar pagamento {payment_id}: {e}")


async def saldo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    creditos = usuarios.get(user_id, 0)
    await update.message.reply_text(f"💰 Seu saldo atual é de *{creditos}* crédito(s).", parse_mode="Markdown")

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in usuarios:
        await update.message.reply_text("⚠️ Você não está cadastrado. Use /start para começar.")
        return

    creditos = usuarios.get(user_id, 0)
    if creditos <= 0:
        await update.message.reply_text("❌ Você não tem créditos. Use /comprar para adquirir mais.")
        return

    file = update.message.document
    if not file.file_name.lower().endswith(('.xlsx', '.xls')):
        await update.message.reply_text("❌ Formato de arquivo inválido. Por favor, envie uma planilha `.xlsx` ou `.xls`.")
        return

    usuarios[user_id] -= 1
    salvar_dados_json(USUARIOS_JSON, usuarios)
    await update.message.reply_text(f"✅ Um crédito foi utilizado. Processando sua planilha... Saldo restante: {usuarios[user_id]}.")

    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, "input_" + file.file_id + ".xlsx")
    output_path = os.path.join(temp_dir, "corrigida_" + file.file_id + ".xlsx")
    
    try:
        new_file = await file.get_file()
        await new_file.download_to_drive(file_path)
        
        df = pd.read_excel(file_path)
        df_corrigido = corrigir_planilha_completo(df)
        df_corrigido.to_excel(output_path, index=False)

        num_paradas = len(df_corrigido)
        bairros_unicos = df_corrigido['Bairro'].dropna().unique().tolist()
        bairros_str = ", ".join(sorted(bairros_unicos))

        success_message = (f"✅ Planilha corrigida!\n\n"
                           f"📍 *Total de Paradas:* {num_paradas}\n"
                           f"🏙️ *Bairros na Rota:* {bairros_str}")
        await update.message.reply_text(success_message, parse_mode="Markdown")
        await update.message.reply_document(document=open(output_path, 'rb'))

    except Exception as e:
        logging.error("Erro ao processar planilha:", exc_info=True)
        await update.message.reply_text(f"❌ Erro ao processar o arquivo: {e}\n\nSeu crédito foi devolvido.")
        usuarios[user_id] += 1
        salvar_dados_json(USUARIOS_JSON, usuarios)
    finally:
        if os.path.exists(file_path): os.remove(file_path)
        if os.path.exists(output_path): os.remove(output_path)


def main():
    BOT_TOKEN = "7569642602:AAGwxJqH5FSLYZmSN_MaJTtspsjbqZMkCYI" # SEU TOKEN DO BOT
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Adiciona a tarefa de verificação de pagamentos
    job_queue = application.job_queue
    job_queue.run_repeating(check_pending_payments, interval=60, first=10) # Roda a cada 60s, começando 10s após o bot iniciar

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CommandHandler("comprar", comprar)],
        states={
            CADASTRO: [MessageHandler(filters.TEXT & ~filters.COMMAND, receber_nome)],
            MENU_CREDITOS: [CallbackQueryHandler(selecionar_credito)],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("comprar", comprar)],
    )

    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("saldo", saldo))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    print("✅ Bot RotaRápida iniciado com verificação de pagamentos.")
    application.run_polling()

if __name__ == '__main__':
    main()