import os
import logging
import pandas as pd
import re
import tempfile
import requests
from unidecode import unidecode
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters, ConversationHandler, CallbackQueryHandler
import json
import uuid # Importado para gerar chaves de idempotência (X-Idempotency-Key)
import base64 # Importado para decodificar a imagem do QR Code
from datetime import datetime # Para armazenar timestamp das transações

# Configuração de Log
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__) # Usar logger para loggings específicos

# Abreviações para normalização de nomes de rua
abreviacoes_nomes = {
    'joao s': 'joao simoes',
    'dr': 'doutor',
    'cel': 'coronel',
    'alm': 'almirante',
    'prof': 'professor',
    'gen': 'general',
    'cap': 'capitao',
    'dep': 'deputado',
    'sen': 'senador',
    'gov': 'governador',
    'pres': 'presidente',
    'min': 'ministro',
    'pad': 'padre',
    'pe': 'padre',
    'mad': 'madre'
}

def expandir_abreviacoes(texto):
    """Expande abreviações comuns para normalização de texto."""
    texto = texto.lower()
    for abrev, completo in abreviacoes_nomes.items():
        texto = re.sub(r'\b' + re.escape(abrev) + r'\b', completo, texto)
    return texto

def normalize(texto):
    """Normaliza texto para criação de chaves de agrupamento (sem acentos, minúsculas, sem caracteres especiais)."""
    texto = str(texto).lower()
    texto = unidecode(texto) # Remove accents
    texto = expandir_abreviacoes(texto)
    texto = re.sub(r'[^a-z0-9 ]', '', texto) # Keep only alphanumeric and spaces
    texto = re.sub(r'\s+', ' ', texto) # Reduce multiple spaces to one
    return texto.strip()

def format_postal_code(pc_str):
    """Formata o CEP para o padrão XXXXX-XXX."""
    pc_str = str(pc_str).strip().replace('-', '') # Remove existing hyphens
    if len(pc_str) == 8:
        return f"{pc_str[:5]}-{pc_str[5:]}"
    return pc_str # Return as is if not a standard 8-digit code

def dividir_endereco_completo(destination_raw):
    """
    Divide a string de endereço bruta em seus componentes para exibição e para a chave de agrupamento.
    Retorna: (Address Line 1 para exibição, Address Line 2 para exibição, String normalizada para chave de agrupamento rua+numero)
    """
    raw_dest = str(destination_raw).strip()
    
    cleaned_dest_for_parsing = re.sub(r'(?:,?\s*-\s*)?(?:São\s*Paulo|SP)?(?:,\s*\d-?\d)?\s*$', '', raw_dest, flags=re.IGNORECASE).strip()
    cleaned_dest_for_parsing = re.sub(r'(?i)\bua\s*\b', '', cleaned_dest_for_parsing).strip()
    cleaned_dest_for_parsing = re.sub(r'\s+', ' ', cleaned_dest_for_parsing).strip()

    display_prefix = ""
    core_address_part = cleaned_dest_for_parsing
        
    if re.match(r'^(r[.]?\s+)', cleaned_dest_for_parsing, re.IGNORECASE):
        display_prefix = "Rua "
        core_address_part = re.sub(r'^(r[.]?\s+)', '', cleaned_dest_for_parsing, flags=re.IGNORECASE).strip()
    elif re.match(r'^(av[.]?\s+)', cleaned_dest_for_parsing, re.IGNORECASE):
        display_prefix = "Avenida "
        core_address_part = re.sub(r'^(av[.]?\s+)', '', cleaned_dest_for_parsing, flags=re.IGNORECASE).strip()
    
    core_address_part_for_street_name = re.sub(r'(?i)\s+(?:no|nº|numero|apto|ap|apartamento|bl|bloco)\s*', ' ', core_address_part).strip()
    core_address_part_for_street_name = re.sub(r'\s+', ' ', core_address_part_for_street_name).strip()

    street_name_extracted = ""
    number_extracted = ""
    complement_extracted = ""

    match = re.match(r"^(.*?)\s*(\d+[a-zA-Z]?)(?:[,\s]*(.*))?$", core_address_part_for_street_name, re.IGNORECASE)

    if match:
        street_name_extracted = match.group(1).strip().rstrip(",").strip()
        number_extracted = match.group(2).strip()
        complement_extracted = match.group(3).strip().lstrip(',').strip() if match.group(3) else ""

        formatted_street_name_for_display = ' '.join(word.capitalize() for word in street_name_extracted.split())
        
        address_line_1_display = f"{display_prefix}{formatted_street_name_for_display}, {number_extracted}"
        address_line_2_display = complement_extracted
        
        normalized_address_key = normalize(f"{street_name_extracted} {number_extracted}")

    else:
        formatted_entire_address_for_display = ' '.join(word.capitalize() for word in core_address_part_for_street_name.split())
        address_line_1_display = f"{display_prefix}{formatted_entire_address_for_display}"
        address_line_2_display = ""
        normalized_address_key = normalize(core_address_part_for_street_name)

    return address_line_1_display, address_line_2_display, normalized_address_key


def corrigir_planilha_completo(df):
    """
    Corrige e agrupa dados da planilha para otimização de rotas.
    """
    df = df.rename(columns={
        'Sequence': 'Pacotes Na Parada',
        'Destination Address': 'Destination',
        'Zipcode/Postal code': 'Postal Code'
    })

    required_cols = ['Destination', 'Bairro', 'City', 'Postal Code']
    for col in required_cols:
        if col not in df.columns:
            logger.warning(f"Coluna '{col}' não encontrada. Criando coluna vazia.")
            df[col] = ''
        else:
            df[col] = df[col].astype(str)
            
    df['Pacotes Na Parada'] = df['Pacotes Na Parada'].apply(
        lambda x: '0' if pd.isna(x) or str(x).strip() in ('', 'nan', '--') else str(x).strip()
    )

    df[['Address Line 1', 'Address Line 2', 'Normalized_Street_Num_For_Group']] = df['Destination'].apply(
        lambda dest: pd.Series(dividir_endereco_completo(str(dest)))
    )

    df['Postal Code Clean'] = df['Postal Code'].astype(str).str.replace('-', '').str.strip()

    df['Address Group'] = df['Normalized_Street_Num_For_Group'] + "_" + \
                          df['Postal Code Clean'] + "_" + \
                          df['Bairro'].apply(normalize) + "_" + \
                          df['City'].apply(normalize)


    def agrupar_pacotes(series):
        """Função auxiliar para agrupar e formatar pacotes."""
        all_pacotes = []
        for p_str in series:
            cleaned_p_str = re.sub(r';\s*Total:\s*\d+\s*pacotes\.', '', p_str).strip()
            current_pacotes = [item.strip() for item in cleaned_p_str.split(',') if item.strip()]
            all_pacotes.extend(current_pacotes)
            
        def sort_key_for_packets(item):
            try:
                return (0, int(item))
            except ValueError:
                return (1, item)
            
        unique_pacotes = sorted(list(set(all_pacotes)), key=sort_key_for_packets)
        
        return ', '.join(unique_pacotes)


    df = df.sort_values(['Address Group', 'Address Line 1', 'Address Line 2', 'Pacotes Na Parada'])

    agrupado = df.groupby(['Address Group'], sort=False).agg(
        Address_Line_1=('Address Line 1', lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]),
        Address_Line_2=('Address Line 2', lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]),
        Bairro=('Bairro', lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]),
        City=('City', lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]),
        Postal_Code_Clean=('Postal Code Clean', lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]),
        Pacotes_Na_Parada=('Pacotes Na Parada', agrupar_pacotes)
    ).reset_index()

    agrupado['Postal Code'] = agrupado['Postal_Code_Clean'].apply(format_postal_code)

    agrupado = agrupado.rename(columns={
        'Address_Line_1': 'Address Line 1',
        'Address_Line_2': 'Address Line 2',
        'Pacotes_Na_Parada': 'Pacotes Na Parada',
    })

    return agrupado[['Address Line 1', 'Address Line 2', 'Bairro', 'City', 'Postal Code', 'Pacotes Na Parada']]


# Armazenamento temporário de usuários/créditos
USUARIOS_JSON = "usuarios.json"

# Inicializa a estrutura de usuários: {user_id: {"credits": N, "pending_payments": [...]}}
def carregar_usuarios():
    """Carrega os dados dos usuários do arquivo JSON."""
    if os.path.exists(USUARIOS_JSON):
        with open(USUARIOS_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Garante a estrutura para usuários existentes sem pending_payments
            for user_id, user_data in data.items():
                # O código original que você forneceu não tem essas chaves,
                # então ele será atualizado para garantir consistência.
                if isinstance(user_data, int): # Se for o formato antigo (apenas créditos)
                    data[user_id] = {"credits": user_data, "pending_payments": []}
                if "pending_payments" not in data[user_id]:
                    data[user_id]["pending_payments"] = []
                if "processed_payments" not in data[user_id]:
                    data[user_id]["processed_payments"] = []
                if "failed_payments" not in data[user_id]:
                    data[user_id]["failed_payments"] = []
            return data
    return {}

def salvar_usuarios(data):
    """Salva os dados dos usuários no arquivo JSON."""
    with open(USUARIOS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

usuarios = carregar_usuarios()

# Estados para o ConversationHandler
CADASTRO, MENU_CREDITOS = range(2)

# --- Funções do Bot Telegram ---

async def receber_nome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recebe o nome do usuário para cadastro."""
    nome = update.message.text.strip()
    user_id = str(update.effective_user.id)

    if user_id in usuarios:
        await update.message.reply_text("⚠️ Você já está cadastrado.")
        return ConversationHandler.END

    # Inicializa o usuário com 1 crédito e lista vazia de pagamentos pendentes
    # Incluindo a inicialização para os novos campos
    usuarios[user_id] = {"credits": 1, "pending_payments": [], "processed_payments": [], "failed_payments": []}
    salvar_usuarios(usuarios)

    await update.message.reply_text(
        f"✅ {nome}, seu cadastro foi concluído com sucesso!\n\nVocê ganhou *1 crédito grátis* para testar. Envie sua planilha quando quiser.\n\nUse /comprar para adquirir mais créditos.",
        parse_mode="Markdown"
    )
    return ConversationHandler.END

async def comprar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra as opções de compra de créditos."""
    keyboard = [
        [InlineKeyboardButton("R$ 1.00 - 1 crédito (0%)", callback_data="comprar_1")],
        [InlineKeyboardButton("R$ 5.00 - 7 créditos (+40%)", callback_data="comprar_5")],
        [InlineKeyboardButton("R$ 10.00 - 15 créditos (+50%)", callback_data="comprar_10")],
        [InlineKeyboardButton("R$ 18.00 - 30 créditos (+66.7%)", callback_data="comprar_18")],
        [InlineKeyboardButton("🔙 Voltar ao Menu Principal", callback_data="voltar_compras")]
    ]
    
    # Se a chamada é uma CallbackQuery (ex: do botão "Voltar"), edita a mensagem existente.
    if update.callback_query:
         await update.callback_query.edit_message_text(
             "Escolha a quantidade de créditos que deseja comprar:",
             reply_markup=InlineKeyboardMarkup(keyboard)
         )
    # Senão (ex: comando /comprar), envia uma nova mensagem.
    else:
        await update.message.reply_text(
            "Escolha a quantidade de créditos que deseja comprar:", 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    return MENU_CREDITOS

async def selecionar_credito(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processa a seleção de créditos e gera o PIX."""
    query = update.callback_query
    await query.answer() # Importante para parar o ícone de carregamento no botão
    
    logger.info(f"Callback data recebido: {query.data}")

    valores = {
        "comprar_1": (1.00, 1),
        "comprar_5": (5.00, 7),
        "comprar_10": (10.00, 15),
        "comprar_18": (18.00, 30)
    }

    if query.data.startswith("comprar_"):
        valor_reais, creditos_a_receber = valores[query.data]
        user_id = str(query.from_user.id) # Obtenha o ID do usuário para o e-mail

        # --- VERIFICAÇÃO DE USUÁRIO - MUITO IMPORTANTE ---
        if user_id not in usuarios:
            await query.edit_message_text("Por favor, use /start primeiro para se cadastrar.")
            return ConversationHandler.END
        # --- FIM DA VERIFICAÇÃO ---

        # Token do Mercado Pago - substitua pelo seu token real ou token de teste apropriado (APP_USR-...)
        # Este token é de exemplo, certifique-se de que ele tem as permissões corretas para criar pagamentos.
        access_token_mp = "APP_USR-7932872917685276-070207-bd97e26d1c2b9e1a5d12b8e4a45d73d7-1029050718" 
        
        # --- ALTERAÇÃO SOLICITADA: Incluindo o e-mail no formato f"{user_id}@example.com" ---
        # ATENÇÃO: É provável que este formato de e-mail cause o erro "Payer email forbidden"
        # em ambientes de teste (Sandbox) do Mercado Pago.
        # A solução robusta é usar um e-mail de "usuário de teste" criado no painel Sandbox do Mercado Pago.
        payer_email_to_use = f"{user_id}@example.com" 

        payload = {
            "transaction_amount": float(f"{valor_reais:.2f}"), # Garante que seja float com 2 casas decimais
            "description": f"Compra de {creditos_a_receber} créditos RotaRápida",
            "payment_method_id": "pix",
            "payer": {
                "email": payer_email_to_use, # <-- Usando o formato f"{user_id}@example.com"
                "first_name": query.from_user.first_name if query.from_user.first_name else "Guest",
                "last_name": query.from_user.last_name if query.from_user.last_name else "User",
            }
        }

        # Geração de uma chave de idempotência única para esta transação.
        # Isso previne que a mesma transação seja processada múltiplas vezes por erro ou retentativa.
        idempotency_key = str(uuid.uuid4())
        logger.info(f"Gerando X-Idempotency-Key: {idempotency_key} para user {user_id}")

        headers = {
            "Authorization": f"Bearer {access_token_mp}",
            "Content-Type": "application/json",
            "X-Idempotency-Key": idempotency_key # Adiciona a chave de idempotência no cabeçalho
        }

        logger.info(f"Enviando requisição MP para o usuário {user_id}:")
        logger.info(f"Payload: {payload}")
        logger.info(f"Headers: {headers}")

        try:
            response = requests.post("https://api.mercadopago.com/v1/payments", headers=headers, data=json.dumps(payload))
            result = response.json()
            
            logger.info(f"Resposta MP Status: {response.status_code}")
            logger.info(f"Resposta MP Body: {json.dumps(result, indent=2)}")

            if response.status_code == 201: # 201 Created indica sucesso
                payment_id = result['id'] # <-- ID DO PAGAMENTO DO MERCADO PAGO
                qr_code_base64 = result['point_of_interaction']['transaction_data']['qr_code_base64']
                pix_link = result['point_of_interaction']['transaction_data']['ticket_url']
                
                # Armazena a transação como pendente (incluindo o user_id do Telegram para referência)
                usuarios[user_id]["pending_payments"].append({
                    "payment_id": payment_id,
                    "credits_to_add": creditos_a_receber,
                    "amount": valor_reais,
                    "generated_at": datetime.now().isoformat(), # Armazena a data/hora para referência
                    "status": "pending", # Status inicial
                    "user_telegram_id": user_id # Adiciona o user_id_telegram
                })
                salvar_usuarios(usuarios) # Salva a informação da transação pendente

                await query.edit_message_text(
                    f"💳 Você escolheu comprar <b>{creditos_a_receber} créditos</b> por R$ {valor_reais:.2f}.\n\n"
                    f"Seu pagamento (ID: <code>{payment_id}</code>) está aguardando. Use o QR Code Pix abaixo para pagamento.\n\n"
                    f"Assim que efetuar o pagamento, envie o comando /paguei para que eu possa verificar a confirmação.\n\n"
                    f"<a href='{pix_link}'>Clique aqui para copiar o código Pix (cola e paga)</a>",
                    parse_mode="HTML",
                    disable_web_page_preview=True 
                )
                
                # Decodifica a string base64 para bytes para enviar como foto
                photo_data = base64.b64decode(qr_code_base64)
                await context.bot.send_photo(chat_id=query.message.chat_id, photo=photo_data)
            else:
                error_message = result.get('message', 'Erro inesperado do Mercado Pago.')
                # Tenta extrair detalhes do erro se disponível
                if 'cause' in result and result['cause']:
                    # Filtra causas que podem vir vazias, para não exibir string vazia
                    error_details = ", ".join([c.get('description', '') for c in result['cause'] if c.get('description')])
                    if error_details:
                        error_message += " Detalhes: " + error_details
                
                await query.edit_message_text(f"❌ Erro ao gerar a cobrança: {error_message}\n\nPor favor, tente novamente mais tarde.")

        except requests.exceptions.RequestException as req_e:
            logger.error(f"Erro de requisição ao Mercado Pago: {req_e}", exc_info=True)
            await query.edit_message_text("❌ Erro de conexão ao Mercado Pago. Verifique sua internet e tente novamente.")
        except Exception as e:
            logger.error(f"Exceção inesperada ao processar compra de crédito: {e}", exc_info=True)
            await query.edit_message_text("❌ Erro inesperado ao gerar a cobrança. Por favor, tente novamente mais tarde.")

    elif query.data == "voltar_compras":
        # Se o usuário clicar em "Voltar", chama a função 'comprar' novamente.
        # A função 'comprar' já lida com a edição da mensagem se for uma callback.
        return await comprar(update, context) # Isso faz com que ele volte para o menu de compra

    return ConversationHandler.END


async def check_payment_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /paguei: Verifica o status de pagamentos pendentes no Mercado Pago."""
    user_id = str(update.effective_user.id)

    if user_id not in usuarios or not usuarios[user_id].get("pending_payments"):
        await update.message.reply_text("Você não tem pagamentos de crédito pendentes para verificar. Use /comprar para adquirir créditos.")
        return

    await update.message.reply_text("Verificando seus pagamentos pendentes, aguarde um momento...")

    pending_payments_to_check = list(usuarios[user_id]["pending_payments"]) # Cria uma cópia para iterar
    updated_pending_payments = [] # Para armazenar pagamentos que ainda estão pendentes
    credits_added_count = 0
    
    # Obtenha o token de acesso (mesmo token usado para criar o pagamento)
    # Em um cenário real, você teria um serviço de gerenciamento de tokens seguro.
    access_token_mp = "APP_USR-7932872917685276-070207-bd97e26d1c2b9e1a5d12b8e4a45d73d7-1029050718" 

    for payment_info in pending_payments_to_check:
        payment_id = payment_info["payment_id"]
        credits_to_add = payment_info["credits_to_add"]
        
        headers = {
            "Authorization": f"Bearer {access_token_mp}"
        }

        try:
            response = requests.get(f"https://api.mercadopago.com/v1/payments/{payment_id}", headers=headers, timeout=10)
            payment_status_data = response.json()
            
            logger.info(f"Status do pagamento {payment_id} para user {user_id}: {payment_status_data.get('status')}")

            if response.status_code == 200:
                mp_status = payment_status_data.get('status')

                # Verifica se o pagamento já foi aprovado/falho antes (medida de segurança)
                is_already_processed = any(str(p.get("payment_id")) == str(payment_id) for p in usuarios[user_id].get("processed_payments", []))
                is_already_failed = any(str(p.get("payment_id")) == str(payment_id) for p in usuarios[user_id].get("failed_payments", []))


                if mp_status == 'approved':
                    if not is_already_processed:
                        # Garante que a chave 'credits' exista na estrutura do usuário
                        if "credits" not in usuarios[user_id]: 
                            usuarios[user_id]["credits"] = 0
                        usuarios[user_id]["credits"] += credits_to_add
                        credits_added_count += credits_to_add
                        
                        # Move para uma lista de pagamentos processados (para histórico)
                        # O dict payment_info_in_pending foi removido daqui e do escopo do webhook
                        # para simplificar a passagem de dados. Usaremos payment_status_data.
                        payment_status_data["processed_at"] = datetime.now().isoformat()
                        payment_status_data["status_in_bot"] = "approved"
                        usuarios[user_id]["processed_payments"].append(payment_status_data)
                        
                        await update.message.reply_text(
                            f"✅ Pagamento de R$ {payment_info.get('amount', 0):.2f} (ID: <code>{payment_id}</code>) aprovado! "
                            f"Adicionado {credits_to_add} créditos à sua conta.",
                            parse_mode="HTML"
                        )
                    else:
                        logger.warning(f"Pagamento {payment_id} já foi processado anteriormente para user {user_id}.")
                        await update.message.reply_text(
                            f"ℹ️ O pagamento com ID <code>{payment_id}</code> já foi confirmado e seus créditos já foram adicionados.",
                            parse_mode="HTML"
                        )
                    
                elif mp_status in ['pending', 'in_process']:
                    payment_info["status"] = mp_status # Atualiza status no registro local
                    updated_pending_payments.append(payment_info) # Permanece na lista
                    await update.message.reply_text(
                        f"⏳ O pagamento com ID <code>{payment_id}</code> ainda está pendente. Por favor, aguarde a confirmação.",
                        parse_mode="HTML"
                    )
                elif mp_status in ['rejected', 'cancelled', 'refunded', 'charged_back']:
                    if not is_already_failed: # Se ainda não o marcamos como falho
                        payment_status_data["processed_at"] = datetime.now().isoformat()
                        payment_status_data["status_in_bot"] = "failed"
                        usuarios[user_id]["failed_payments"].append(payment_status_data)
                        await update.message.reply_text(
                            f"❌ O pagamento com ID <code>{payment_id}</code> foi {mp_status}. Se houve um erro, tente novamente ou entre em contato.",
                            parse_mode="HTML"
                        )
                    else:
                        logger.info(f"Pagamento {payment_id} já marcado como falho para user {user_id}. Ignorando.")

                else: # Outros status inesperados
                    payment_info["status"] = mp_status
                    updated_pending_payments.append(payment_info)
                    await update.message.reply_text(
                        f"❓ O status do pagamento <code>{payment_id}</code> é '{mp_status}'. Por favor, aguarde ou entre em contato se demorar.",
                        parse_mode="HTML"
                    )
            else:
                logger.error(f"Erro ao consultar MP para payment_id {payment_id}: {payment_status_data}")
                payment_info["status"] = "error_query" # Sinaliza erro na query
                updated_pending_payments.append(payment_info)
                await update.message.reply_text(f"⚠️ Erro ao consultar o status do pagamento <code>{payment_id}</code>. Tente novamente mais tarde.", parse_mode="HTML")

        except requests.exceptions.Timeout:
            logger.error(f"Timeout ao consultar MP para payment_id {payment_id}", exc_info=True)
            payment_info["status"] = "timeout"
            updated_pending_payments.append(payment_info)
            await update.message.reply_text(f"⚠️ A consulta do status para <code>{payment_id}</code> excedeu o tempo limite. Tente novamente.", parse_mode="HTML")
        except requests.exceptions.RequestException as req_e:
            logger.error(f"Erro de requisição ao consultar MP para payment_id {payment_id}: {req_e}", exc_info=True)
            payment_info["status"] = "request_error"
            updated_pending_payments.append(payment_info)
            await update.message.reply_text(f"⚠️ Erro de conexão ao consultar o status do pagamento <code>{payment_id}</code>. Tente novamente.", parse_mode="HTML")
        except Exception as e:
            logger.error(f"Exceção inesperada ao verificar pagamento {payment_id}: {e}", exc_info=True)
            payment_info["status"] = "exception"
            updated_pending_payments.append(payment_info)
            await update.message.reply_text(f"⚠️ Erro inesperado ao verificar o status para <code>{payment_id}</code>. Contate o suporte.", parse_mode="HTML")

    usuarios[user_id]["pending_payments"] = [
        p for p in updated_pending_payments if p['status'] in ['pending', 'in_process', 'timeout', 'error_query', 'request_error', 'exception']
    ]

    salvar_usuarios(usuarios) # Salva o estado atualizado após as verificações

    if credits_added_count > 0:
        await update.message.reply_text(f"🎉 Seu saldo atualizado é de *{usuarios[user_id]['credits']}* créditos!", parse_mode="Markdown")
    
    if not usuarios[user_id]["pending_payments"]:
        await update.message.reply_text("✅ Todas as verificações concluídas. Você não tem mais pagamentos pendentes.")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /start: Inicia o bot e verifica o cadastro do usuário."""
    user_id = str(update.effective_user.id) # Chave para o dict de usuários
    
    if user_id in usuarios:
        # Garante que a estrutura do usuário está completa para usuários antigos
        if isinstance(usuarios[user_id], int): # Formato antigo (apenas int para créditos)
            usuarios[user_id] = {"credits": usuarios[user_id], "pending_payments": [], "processed_payments": [], "failed_payments": []}
        else: # Já é um dicionário, apenas adiciona os campos se não existirem
            if "pending_payments" not in usuarios[user_id]: usuarios[user_id]["pending_payments"] = []
            if "processed_payments" not in usuarios[user_id]: usuarios[user_id]["processed_payments"] = []
            if "failed_payments" not in usuarios[user_id]: usuarios[user_id]["failed_payments"] = []
        salvar_usuarios(usuarios) # Salva qualquer atualização de estrutura

        creditos = usuarios[user_id].get("credits", 0) # Acessa créditos da nova estrutura

        await update.message.reply_text(
            f"""👋 Olá novamente!
Você já está cadastrado. Créditos disponíveis: *{creditos}*.
Use /comprar para adquirir mais créditos ou envie sua planilha.""",
            parse_mode="Markdown"
        )
        return ConversationHandler.END # Encerra o conversation handler se já cadastrado

    await update.message.reply_text(
        """👋 Seja bem-vindo ao RotaRápida!

Para começar a usar, preciso do seu *primeiro nome* para te cadastrar. Você receberá 1 crédito grátis para testar.
""",
        parse_mode="Markdown"
    )
    return CADASTRO # Entra no estado de CADASTRO

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Gerencia o recebimento e processamento de arquivos XLSX."""
    user_id = str(update.effective_user.id) # Chave para o dict de usuários

    if user_id not in usuarios: # Verifica se o usuário existe na nova estrutura
        await update.message.reply_text("❌ Você precisa se cadastrar primeiro! Use /start para iniciar.")
        return
        
    creditos = usuarios[user_id].get("credits", 0) # Acessa créditos da nova estrutura
    if creditos <= 0:
        await update.message.reply_text(
            "❌ Você não possui créditos suficientes para processar a planilha. Use /comprar para adquirir mais."
        )
        return

    # Se tem créditos, desconta 1
    usuarios[user_id]["credits"] -= 1 # Desconta da nova estrutura
    salvar_usuarios(usuarios) # Salva o estado atualizado dos créditos

    file = update.message.document
    if not file.file_name.endswith('.xlsx'):
        await update.message.reply_text("❌ Envie apenas arquivos no formato `.xlsx` (Excel).")
        return

    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, file.file_name)
    output_path = os.path.join(temp_dir, f"planilha_corrigida_{user_id}.xlsx") # Nome único para evitar conflito

    await update.message.reply_text("Processando sua planilha, aguarde um momento...")

    try:
        new_file = await file.get_file()
        await new_file.download_to_drive(file_path)

        df = pd.read_excel(file_path)
        df_corrigido = corrigir_planilha_completo(df)
        df_corrigido.to_excel(output_path, index=False)

        num_paradas = len(df_corrigido)
        bairros_unicos = df_corrigido['Bairro'].unique().tolist()
        bairros_formatados = sorted([' '.join(word.capitalize() for word in b.split()) for b in bairros_unicos if b]) # Adicionado 'if b' para evitar vazios/NaN
        bairros_str = ", ".join(bairros_formatados)

        success_message = ( # Corrigido f-string
            f"✅ Planilha corrigida com sucesso! Aqui está o arquivo corrigido:\n\n"
            f"📍 Quantidade de Paradas: *{num_paradas}*\n"
            f"🏙️ Bairros na Rota: *{bairros_str}*\n\n"
            f"Seus créditos restantes: *{usuarios[user_id]['credits']}*." # Acessa da nova estrutura
        )

        await update.message.reply_text(success_message, parse_mode="Markdown")
        await update.message.reply_document(document=open(output_path, 'rb'))

        os.remove(output_path)
        os.remove(file_path)

    except Exception as e:
        logger.error(f"Erro no handler de arquivo para user {user_id}: {e}", exc_info=True) # Melhorar log
        await update.message.reply_text(f"❌ Erro ao processar a planilha: {str(e)}\n\nPor favor, verifique se o arquivo está no formato correto e sem erros.")
    
async def saldo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /saldo: Exibe os créditos disponíveis do usuário."""
    user_id = str(update.effective_user.id)
    if user_id in usuarios:
        creditos = usuarios[user_id].get("credits", 0) # Acessa créditos da nova estrutura
        await update.message.reply_text(f"💰 Créditos disponíveis: *{creditos}*", parse_mode="Markdown")
    else:
        await update.message.reply_text("Você ainda não está cadastrado. Use /start para iniciar e ganhar 1 crédito grátis!")

def main():
    """Função principal para iniciar o bot."""
    BOT_TOKEN = "7569642602:AAGwxJqH5FSLYZmSN_MaJTtspsjbqZMkCYI" 
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Conversation handler para cadastro e compra de créditos
    cadastro_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CommandHandler("comprar", comprar)], # /comprar também inicia o conversation
        states={
            CADASTRO: [MessageHandler(filters.TEXT & ~filters.COMMAND, receber_nome)],
            MENU_CREDITOS: [CallbackQueryHandler(selecionar_credito)],
        },
        fallbacks=[
            CommandHandler("cancelar", lambda update, context: ConversationHandler.END) # Adiciona um fallback para cancelar
        ],
    )

    app.add_handler(cadastro_handler)
    app.add_handler(CommandHandler("saldo", saldo))
    app.add_handler(CommandHandler("paguei", check_payment_status)) # O comando /paguei foi mantido
    app.add_handler(MessageHandler(filters.Document.ALL & filters.Document.FileExtension("xlsx"), handle_file))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, lambda update, context: update.message.reply_text("Não entendi sua mensagem. Por favor, envie uma planilha .xlsx ou use um dos comandos: /start, /saldo, /comprar, /paguei.")))


    logger.info("✅ Bot RotaRápida iniciado. Pressione Ctrl+C para parar.")
    app.run_polling(allowed_updates=Update.ALL_TYPES) # Captura todos os tipos de updates


if __name__ == '__main__':
    main()