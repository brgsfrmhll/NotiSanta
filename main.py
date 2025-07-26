# --- git pull https://github.com/brgsfrmhll/NotiSanta
# --- sudo systemctl daemon-reload
# --- sudo systemctl restart streamlit-app2.service
# --- source /home/ubuntu/NotiSanta/venv/bin/activate

import streamlit as st
import json
import hashlib
import os
from datetime import datetime, date as dt_date_class, time as dt_time_class, timedelta
from typing import Dict, List, Optional, Any
import uuid
import pandas as pd
import time as time_module
import psycopg2
from psycopg2 import sql  # Importa sql para usar na construÃ§Ã£o de queries dinÃ¢micas
from dotenv import load_dotenv
from streamlit import fragment as st_fragment  # Mantido para compatibilidade com o cÃ³digo completo

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

def get_db_connection():
    """
    Estabelece e retorna uma conexÃ£o com o banco de dados PostgreSQL.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        raise  # Levanta a exceÃ§Ã£o para que o chamador possa lidar com ela


# --- ConfiguraÃ§Ã£o do Streamlit e CSS Customizado ---
# CORREÃ‡ÃƒO DO ERRO 1.1: Removida a linha duplicada de st.set_page_config
st.set_page_config(
    page_title="NotificaSanta",
    page_icon="favicon/logo.png",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CORREÃ‡ÃƒO DO ERRO 1.2: Sintaxe corrigida no bloco CSS dentro de st.markdown
st.markdown(r"""
<style>
    /* Esconde botÃµes e decoraÃ§Ãµes padrÃ£o do Streamlit */
    button[data-testid="stDeployButton"],
    .stDeployButton,
    footer,
    #stDecoration,
    .stAppDeployButton {
        display: none !important;
    }

    /* Ajuste de margem superior para o container principal do Streamlit */
    .reportview-container {
        margin-top: -2em;
    }

    /* Permite que a sidebar seja aberta (linha originalmente comentada, mantida para contexto) */
    /* .sidebar-hint {
        /* display: none; */
    /* } */

    /* Garante que a Sidebar fique ACIMA de outros elementos fixos, se houver */
    div[data-testid="stSidebar"] {
        z-index: 9999 !important; /* Prioridade de empilhamento muito alta */
    }
    /* Linhas duplicadas e chaves } extras/malposicionadas removidas daqui e de blocos similares */

    /* Adjust Streamlit's default margins for sidebar content */
    /* This targets the internal container of the sidebar */
    [data-testid="stSidebarContent"] {
        padding-top: 10px; /* Reduced from default to move content higher */
    }

    /* Logo - Reduced size and moved up */
    div[data-testid="stSidebar"] img {
        transform: scale(0.6); /* Reduce size by 20% */
        transform-origin: top center; /* Scale from the top center */
        margin-top: -80px; /* Pull the image up */
        margin-bottom: -20px; /* Reduce space below image */
    }

    /* Estilo do cabeÃ§alho principal da aplicaÃ§Ã£o */
    .main-header {
        text-align: center;
        color: #2E86AB;
        margin-bottom: 30px;
    }

    /* Novo Estilo para o TÃ­tulo Principal da Sidebar */
    /* Usamos [data-testid="stSidebarContent"] para aumentar a especificidade e garantir a aplicaÃ§Ã£o */
    [data-testid="stSidebarContent"] .sidebar-main-title {
        text-align: center !important; /* Centraliza o texto */
        color: #00008B !important; /* Cor azul escuro para o tÃ­tulo principal */
        font-size: 1.76em !important; /* 2.2em * 0.8 = 1.76em */
        font-weight: 700 !important; /* Negrito forte para o tÃ­tulo */
        text-transform: uppercase !important; /* Transforma todo o texto em maiÃºsculas */
        letter-spacing: 2px !important; /* Aumenta o espaÃ§amento entre as letras para um visual "minimalista" e "estiloso" */
        text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.2) !important; /* Sombra mais suave para profundidade */
        margin-top: -30px !important; /* Move tÃ­tulo principal para cima */
    }

    /* Novo Estilo para o SubtÃ­tulo da Sidebar */
    /* Usamos [data-testid="stSidebarContent"] para aumentar a especificidade e garantir a aplicaÃ§Ã£o */
    [data-testid="stSidebarContent"] .sidebar-subtitle {
        text-align: center !important; /* Centraliza o texto */
        color: #333 !important; /* Cor mais suave para o subtÃ­tulo */
        font-size: 0.72em !important; /* 0.9em * 0.8 = 0.72em */
        font-weight: 400 !important; /* Peso de fonte mÃ©dio */
        text-transform: uppercase !important; /* Transforma todo o texto em maiÃºsculas, mantendo a consistÃªncia */
        letter-spacing: 1.5px !important; /* EspaÃ§amento entre letras para alinhamento visual */
        margin-top: -30px !important; /* Pull closer to main title */
        margin-bottom: 5px !important; /* Reduce margin below subtitle */
    }

    /* Estilo geral para cartÃµes de notificaÃ§Ã£o */
    .notification-card {
        border: 1px solid #ddd;
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
        background-color: #f9f9f9;
        color: #2E86AB; /* Cor do texto padrÃ£o para o cartÃ£o */
    }

    /* Cores e destaque para diferentes status de notificaÃ§Ã£o */
    .status-pendente_classificacao { color: #ff9800; font-weight: bold; } /* Laranja */
    .status-classificada { color: #2196f3; font-weight: bold; } /* Azul */
    .status-em_execucao { color: #9c27b0; font-weight: bold; } /* Roxo */
    .status-aguardando_classificador { color: #ff5722; font-weight: bold; } /* Laranja avermelhado (Usado para RevisÃ£o Rejeitada) */
    .status-revisao_classificador_execucao { color: #8BC34A; font-weight: bold; } /* Verde Lima - Novo Status */
    .status-aguardando_aprovacao { color: #ffc107; font-weight: bold; } /* Amarelo */
    .status-aprovada { color: #4caf50; font-weight: bold; } /* Verde */
    .status-concluida { color: #4caf50; font-weight: bold; } /* Verde (mesmo que aprovada para simplificar) */
    .status-rejeitada { color: #f44336; font-weight: bold; } /* Vermelho (Usado para RejeiÃ§Ã£o Inicial) */
    .status-reprovada { color: #f44336; font-weight: bold; } /* Vermelho (Usado para RejeiÃ§Ã£o de AprovaÃ§Ã£o)*/
    /* Estilo para o conteÃºdo da barra lateral */
    .sidebar .sidebar-content {
        background-color: #f0f2f6; /* Cinza claro */
    }

    /* Estilo para a caixa de informaÃ§Ãµes do usuÃ¡rio na sidebar */
    .user-info {
        background-color: #e8f4fd; /* Azul claro */
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 20px;
    }

    /* Estilo para seÃ§Ãµes de formulÃ¡rio */
    .form-section {
        background-color: #f8f9fa; /* Cinza bem claro */
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        border-left: 4px solid #2E86AB; /* Barra lateral azul */
    }

    /* Estilo para campos condicionais em formulÃ¡rios (ex: detalhes de aÃ§Ã£o imediata) */
    .conditional-field {
        background-color: #fff3cd; /* Amarelo claro */
        padding: 10px;
        border-radius: 5px;
        border-left: 3px solid #ffc107; /* Barra lateral amarela */
        margin: 10px 0;
    }

    /* Estilo para campos obrigatÃ³rios */
    .required-field {
        color: #dc3545; /* Vermelho */
        font-weight: bold;
    }

    /* Cores especÃ­ficas para botÃµes "Sim" e "NÃ£o" selecionados */
    div.stButton > button[data-testid='stButton'][data-key*='_sim_step'][data-selected='true'] {
        border-color: #4caf50; /* Verde */
        color: #4caf50;
    }
    div.stButton > button[data-testid='stButton'][data-key*='_nao_step'][data-selected='true'] {
        border-color: #f44336; /* Vermelho */
        color: #f44336;
    }

    /* Negrito geral para labels dentro de blocos horizontais do Streamlit */
    div[data-testid="stHorizontalBlock"] div[data-testid^="st"] label p {
        font-weight: bold;
    }

    /* Estilo para cartÃµes de mÃ©tricas no dashboard */
    .metric-card {
        background-color: #ffffff;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }

    .metric-card h4 {
        margin-top: 0;
        color: #333;
    }

    .metric-card p {
        font-size: 1.8em;
        font-weight: bold;
        color: #2E86AB;
        margin-bottom: 0;
    }

    /* Estilo para o rodapÃ© da sidebar */
    .sidebar-footer {
        text-align: center;
        margin-top: 20px; /* Adiciona um espaÃ§o acima do rodapÃ© */
        padding: 10px;
        color: #888;
        font-size: 0.75em;
        border-top: 1px solid #eee; /* Linha divisÃ³ria sutil */
    }

    /* Remove padding do container principal, pois o rodapÃ© fixo foi removido */
    div[data-testid="stAppViewContainer"] {
        padding-bottom: 0px; /* NÃ£o Ã© mais necessÃ¡rio padding na parte inferior */
    }

    /* Estilos para o fundo do cartÃ£o de notificaÃ§Ã£o com base no status do prazo */
    .notification-card.card-prazo-dentro {
        background-color: #e6ffe6; /* Verde claro para "No Prazo" e "Prazo PrÃ³ximo" */
        border: 1px solid #4CAF50; /* Borda verde */
    }

    .notification-card.card-prazo-fora {
        background-color: #ffe6e6; /* Vermelho claro para "Atrasada" */
        border: 1px solid #F44336; /* Borda vermelha */
    }

    /* Estilos para status de prazo */
    .deadline-ontrack { color: #4CAF50; font-weight: bold; } /* Verde */
    .deadline-duesoon { color: #FFC107; font-weight: bold; } /* Amarelo */

    /* Estilo para entrada de aÃ§Ã£o individual */
    .action-entry-card {
        border: 1px solid #cceeff; /* Azul claro */
        border-left: 5px solid #2E86AB; /* Azul mais escuro para destaque */
        border-radius: 8px;
        padding: 12px;
        margin-top: 10px;
        margin-bottom: 10px;
        background-color: #f0f8ff; /* Fundo azul muito claro */
        box-shadow: 0 2px 5px rgba(0,0,0,0.05); /* Sombra suave */
    }

    .action-entry-card strong {
        color: #2E86AB;
    }

    .action-entry-card em {
        color: #555;
    }

    /* Estilo para "minhas" aÃ§Ãµes na execuÃ§Ã£o */
    .my-action-entry-card {
        border: 1px solid #d4edda; /* Verde claro */
        border-left: 5px solid #28a745; /* Verde para destaque */
        border-radius: 8px;
        padding: 12px;
        margin-top: 10px;
        margin-bottom: 10px;
        background-color: #eaf7ed; /* Fundo verde muito claro */
        box-shadow: 0 2px 5px rgba(0,0,0,0.05); /* Sombra suave */
    }

    .my-action-entry-card strong {
        color: #28a745;
    }

    /* Estilo para a seÃ§Ã£o de evidÃªncias dentro de uma aÃ§Ã£o */
    .evidence-section {
        background-color: #ffffff; /* Fundo branco */
        border-top: 1px dashed #cccccc; /* Linha tracejada superior */
        margin-top: 10px;
        padding-top: 10px;
    }

    .evidence-section h6 { /* Adicionado para subtÃ­tulos dentro das evidÃªncias */
        color: #666;
        margin-bottom: 5px;
    }

    options = {
        'show_menu': False }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}    

</style>
""", unsafe_allow_html=True)

# Mapeamento de prazos para conclusÃ£o da notificaÃ§Ã£o
DEADLINE_DAYS_MAPPING = {
    "NÃ£o conformidade": 30,
    "CircunstÃ¢ncia de Risco": 30,
    "Near Miss": 30,
    "Evento sem dano": 10,
    "Evento com dano": {
        "Dano leve": 7,
        "Dano moderado": 5,
        "Dano grave": 3,
        "Ã“bito": 3
    }
}


# --- Classes de Dados Globais ---

class UI_TEXTS:
    selectbox_default_event_shift = "Selecionar Turno"
    selectbox_default_immediate_actions_taken = "Selecione"
    selectbox_default_patient_involved = "Selecione"
    selectbox_default_patient_outcome_obito = "Selecione"
    selectbox_default_initial_event_type = "Selecione"
    selectbox_default_initial_severity = "Selecione"
    selectbox_default_notification_select = "Selecione uma notificaÃ§Ã£o..."
    text_na = "N/A"
    selectbox_default_procede_classification = "Selecione"
    selectbox_default_classificacao_nnc = "Selecione"
    selectbox_default_nivel_dano = "Selecione"
    selectbox_default_prioridade_resolucao = "Selecione"
    selectbox_default_never_event = "Selecione"
    selectbox_default_evento_sentinela = "Selecione"
    selectbox_default_tipo_principal = "Selecione"
    multiselect_instruction_placeholder = "Selecione uma ou mais opÃ§Ãµes..."
    multiselect_event_spec_label_prefix = "EspecificaÃ§Ã£o do Evento "
    multiselect_event_spec_label_suffix = ":"
    multiselect_classification_oms_label = "ClassificaÃ§Ã£o OMS:* (selecionar ao menos um)"
    selectbox_default_requires_approval = "Selecione"
    selectbox_default_approver = "Selecione"
    selectbox_default_decisao_revisao = "Selecione"
    selectbox_default_acao_realizar = "Selecione"
    multiselect_assign_executors_label = "Atribuir Executores ResponsÃ¡veis:*"
    selectbox_default_decisao_aprovacao = "Selecione"
    multiselect_all_option = "Todos"
    selectbox_sort_by_placeholder = "Ordenar por..."
    selectbox_sort_by_label = "Ordenar por:"
    selectbox_items_per_page_placeholder = "Itens por pÃ¡gina..."
    selectbox_items_per_page_label = "Itens por pÃ¡gina:"
    selectbox_default_admin_debug_notif = "Selecione uma notificaÃ§Ã£o..."
    selectbox_never_event_na_text = "NÃ£o AplicÃ¡vel (N/A)"
    multiselect_user_roles_label = "FunÃ§Ãµes do UsuÃ¡rio:*"

    # Novos textos para status de prazo
    deadline_status_ontrack = "No Prazo"
    deadline_status_duesoon = "Prazo PrÃ³ximo"
    deadline_status_overdue = "Atrasada"
    deadline_days_nan = "Nenhum prazo definido"
    selectbox_default_department_select = "Selecione o Setor..."  # <-- ADICIONADO

    # Constantes para filtros do dashboard
    multiselect_filter_status_label = "Filtrar por Status:"
    multiselect_filter_nnc_label = "Filtrar por ClassificaÃ§Ã£o NNC:"
    multiselect_filter_priority_label = "Filtrar por Prioridade:"


class FORM_DATA:
    turnos = ["Diurno", "Noturno", "NÃ£o sei informar"]
    classificacao_nnc = ["NÃ£o conformidade", "CircunstÃ¢ncia de Risco", "Near Miss", "Evento sem dano",
                         "Evento com dano"]
    niveis_dano = ["Dano leve", "Dano moderado", "Dano grave", "Ã“bito"]
    prioridades = ["Baixa", "MÃ©dia", "Alta", "CrÃ­tica"]

    SETORES = [
        "SuperintendÃªncia", "AgÃªncia Transfusional (AGT)", "Ala A", "Ala B",
        "Ala C", "Ala E", "Almoxarifado", "AssistÃªncia Social",
        "AmbulatÃ³rio BariÃ¡trica/Reparadora", "CCIH", "CDI", "Centro CirÃºrgico",
        "Centro ObstÃ©trico", "CME", "Comercial/Tesouraria", "Compras",
        "ComunicaÃ§Ã£o", "Contabilidade", "CPD (TI)", "DPI",
        "Diretoria Assistencial", "Diretoria ClÃ­nica", "Diretoria Financeira",
        "Diretoria TÃ©cnica", "Departamento Pessoal (RH)", "AmbulatÃ³rio Egresso (Especialidades)",
        "EMTN", "FarmÃ¡cia ClÃ­nica", "FarmÃ¡cia Central", "FarmÃ¡cia OncolÃ³gica (ManipulaÃ§Ã£o Quimioterapia)",
        "FarmÃ¡cia UNACON", "FarmÃ¡cia SatÃ©lite UTI",
        "Faturamento", "Fisioterapia", "Fonoaudiologia", "GestÃ£o de Leitos",
        "HemodiÃ¡lise", "HigienizaÃ§Ã£o", "InternaÃ§Ã£o/AutorizaÃ§Ã£o (ConvÃªnio)", "Iodoterapia",
        "LaboratÃ³rio de AnÃ¡lises ClÃ­nicas", "Lavanderia", "ManutenÃ§Ã£o Equipamentos", "ManutenÃ§Ã£o Predial",
        "Maternidade", "Medicina do Trabalho", "NHE", "Odontologia", "Ouvidoria", "Pediatria",
        "Portaria/GestÃ£o de Acessos", "Psicologia", "Qualidade", "Quimioterapia (SalÃ£o de Quimio)",
        "RecepÃ§Ã£o", "Recrutamento e SeleÃ§Ã£o", "RegulaÃ§Ã£o", "SAME", "SESMT",
        "ServiÃ§o de NutriÃ§Ã£o e DietÃ©tica", "SSB", "UrgÃªncia e EmergÃªncia/Pronto Socorro",
        "UNACON", "UTI Adulto", "UTI Neo e PediÃ¡trica"
    ]
    never_events = [
        "Cirurgia no local errado do corpo, no paciente errado ou o procedimento errado",
        "RetenÃ§Ã£o de corpo estranho em paciente apÃ³s a cirurgia",
        "Morte de paciente ou lesÃ£o grave associada ao uso de dispositivo mÃ©dico",
        "Morte de paciente ou lesÃ£o grave associada Ã  incompatibilidade de tipo sanguÃ­neo",
        "Morte de paciente ou lesÃ£o grave associada a erro de medicaÃ§Ã£o",
        "Morte de paciente ou lesÃ£o grave associada Ã  trombose venosa profunda (TVP) ou embolia pulmonar (EP) apÃ³s artroplastia total de quadril ou joelho",
        "Morte de paciente ou lesÃ£o grave associada a hipoglicemia",
        "Morte de paciente ou lesÃ£o grave associada Ã  infecÃ§Ã£o hospitalar",
        "Morte de paciente ou lesÃ£o grave associada a Ãºlcera por pressÃ£o (escaras) adquirida no hospital",
        "Morte de paciente ou lesÃ£o grave associada Ã  contenÃ§Ã£o inadequada",
        "Morte ou lesÃ£o grave associada Ã  falha ou uso incorreto de equipamentos de proteÃ§Ã£o individual (EPIs)",
        "Morte de paciente ou lesÃ£o grave associada Ã  queda do paciente",
        "Morte de paciente ou lesÃ£o grave associada Ã  violÃªncia fÃ­sica ou sexual no ambiente hospitalar",
        "Morte de paciente ou lesÃ£o grave associada ao desaparecimento de paciente"
    ]
    tipos_evento_principal = {
        "ClÃ­nico": [
            "InfecÃ§Ã£o Relacionada Ã  AssistÃªncia Ã  SaÃºde (IRAS)",
            "AdministraÃ§Ã£o de AntineoplÃ¡sicos",
            "META 1 - IdentificaÃ§Ã£o Incorreta do Paciente",
            "META 2 - Falha na ComunicaÃ§Ã£o entre Profissionais",
            "META 3 - Problema com Medicamento (SeguranÃ§a Medicamentosa)",
            "META 4 - Procedimento Incorreto (Cirurgia/Parto)",
            "META 5 - Higiene das MÃ£os Inadequada",
            "META 6 - Queda de Paciente e LesÃ£o por PressÃ£o",
            "TransfusÃ£o Inadequada de Sangue ou Derivados",
            "Problema com Dispositivo/Equipamento MÃ©dico",
            "Evento CrÃ­tico ou IntercorrÃªncia Grave em Processo Seguro",
            "Problema Nutricional Relacionado Ã  AssistÃªncia",
            "NÃ£o Conformidade com Protocolos Gerenciados",
            "Quebra de SLA (Atraso ou Falha na AssistÃªncia)",
            "Evento Relacionado ao Parto e Nascimento",
            "Crise Convulsiva em Ambiente Assistencial",
            "[HemodiÃ¡lise] CoagulaÃ§Ã£o do Sistema ExtracorpÃ³reo",
            "[HemodiÃ¡lise] DesconexÃ£o Acidental da Agulha de PunÃ§Ã£o da FÃ­stula Arteriovenosa",
            "[HemodiÃ¡lise] DesconexÃ£o Acidental do Cateter Ã s Linhas de HemodiÃ¡lise",
            "[HemodiÃ¡lise] Embolia Pulmonar Relacionada Ã  HemodiÃ¡lise",
            "[HemodiÃ¡lise] ExteriorizaÃ§Ã£o Acidental da Agulha de PunÃ§Ã£o da FÃ­stula Arteriovenosa",
            "[HemodiÃ¡lise] ExteriorizaÃ§Ã£o Acidental do Cateter de HemodiÃ¡lise",
            "[HemodiÃ¡lise] Falha na IdentificaÃ§Ã£o do Dialisador ou das Linhas de HemodiÃ¡lise",
            "[HemodiÃ¡lise] Falha no Fluxo SanguÃ­neo do Cateter de HemodiÃ¡lise",
            "[HemodiÃ¡lise] Falha no Fluxo SanguÃ­neo da FÃ­stula Arteriovenosa",
            "[HemodiÃ¡lise] Hematoma Durante a Passagem do Cateter de HemodiÃ¡lise",
            "[HemodiÃ¡lise] HemÃ³lise Relacionada Ã  HemodiÃ¡lise",
            "[HemodiÃ¡lise] InfiltraÃ§Ã£o, Edema ou Hematoma na FÃ­stula Arteriovenosa",
            "[HemodiÃ¡lise] PneumotÃ³rax Durante a Passagem do Cateter de HemodiÃ¡lise",
            "[HemodiÃ¡lise] Pseudoaneurisma na FÃ­stula Arteriovenosa",
            "[HemodiÃ¡lise] PunÃ§Ã£o Arterial Acidental Durante InserÃ§Ã£o do Cateter de HemodiÃ¡lise",
            "[HemodiÃ¡lise] Rotura da FÃ­stula Arteriovenosa",
            "[HemodiÃ¡lise] Sangramento pelo Ã“stio do Cateter de HemodiÃ¡lise",
            "[HemodiÃ¡lise] Outras Falhas Relacionadas Ã  HemodiÃ¡lise"
        ],
        "NÃ£o-clÃ­nico": [
            "Incidente de SeguranÃ§a Patrimonial",
            "Problema Estrutural/InstalaÃ§Ãµes",
            "Problema de Abastecimento/LogÃ­stica",
            "Incidente de TI/Dados",
            "Erro Administrativo",
            "Outros Eventos NÃ£o-clÃ­nicos"
        ],
        "Ocupacional": [
            "Acidente com Material BiolÃ³gico",
            "Acidente de Trabalho (geral)",
            "DoenÃ§a Ocupacional",
            "ExposiÃ§Ã£o a Agentes de Risco",
            "Outros Eventos Ocupacionais"
        ],
        "Queixa tÃ©cnica": [],
        "Outros": []
    }
    classificacao_oms = [
        "Quedas", "InfecÃ§Ãµes", "MedicaÃ§Ã£o", "Cirurgia", "IdentificaÃ§Ã£o do Paciente",
        "Procedimentos", "Dispositivos MÃ©dicos", "UrgÃªncia/EmergÃªncia",
        "SeguranÃ§a do Ambiente", "ComunicaÃ§Ã£o", "Recursos Humanos", "Outros"
    ]


# --- DiretÃ³rios de Dados e Arquivos (para anexos) ---
DATA_DIR = "data"
ATTACHMENTS_DIR = os.path.join(DATA_DIR, "attachments")


# --- FunÃ§Ãµes de PersistÃªncia e Banco de Dados ---

def init_database():
    """Garante que os diretÃ³rios de dados e arquivos iniciais existam e cria tabelas no DB."""
    # Garante que os diretÃ³rios existam
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(ATTACHMENTS_DIR):
        os.makedirs(ATTACHMENTS_DIR)

    conn = None # Inicializa a variÃ¡vel de conexÃ£o para garantir que seja None em caso de erro na conexÃ£o
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Criar tabelas
        # Os comandos SQL foram limpos de duplicaÃ§Ãµes e erros de sintaxe
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                roles TEXT[] NOT NULL DEFAULT '{}', -- Array de strings para as funÃ§Ãµes (e.g., {'admin', 'classificador'})
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_users_username ON users (username);

            CREATE TABLE IF NOT EXISTS notifications (
                id SERIAL PRIMARY KEY,
                title VARCHAR(500) NOT NULL,
                description TEXT NOT NULL,
                location VARCHAR(255),
                occurrence_date DATE,
                occurrence_time TIME,
                reporting_department VARCHAR(255),
                reporting_department_complement VARCHAR(255),
                notified_department VARCHAR(255),
                notified_department_complement VARCHAR(255),
                event_shift VARCHAR(50),
                immediate_actions_taken BOOLEAN,
                immediate_action_description TEXT,
                patient_involved BOOLEAN,
                patient_id VARCHAR(255),
                patient_outcome_obito BOOLEAN,
                additional_notes TEXT,
                status VARCHAR(50) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

                -- Campos complexos armazenados como JSONB
                classification JSONB,
                rejection_classification JSONB,
                review_execution JSONB,
                approval JSONB,
                rejection_approval JSONB,
                rejection_execution_review JSONB,
                conclusion JSONB,

                -- ReferÃªncias a usuÃ¡rios (IDs de usuÃ¡rios)
                executors INTEGER[] DEFAULT '{}', -- IDs dos usuÃ¡rios executores (pode ser um array de IDs)
                approver INTEGER REFERENCES users(id), -- ID do usuÃ¡rio aprovador

                -- Colunas para otimizaÃ§Ã£o de busca (Full-text search)
                search_vector TSVECTOR
            );
            CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications (status);
            CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications (created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_notifications_approver ON notifications (approver);
            CREATE INDEX IF NOT EXISTS idx_notifications_classification_gin ON notifications USING GIN (classification);
            CREATE INDEX IF NOT EXISTS idx_notifications_executors_gin ON notifications USING GIN (executors);
            CREATE INDEX IF NOT EXISTS idx_notifications_search_vector ON notifications USING GIN (search_vector);

            -- Trigger para atualizar search_vector automaticamente
            -- Usamos $BODY$ como delimitador, que Ã© uma prÃ¡tica comum para funÃ§Ãµes PL/pgSQL
            CREATE OR REPLACE FUNCTION update_notification_search_vector() RETURNS TRIGGER AS $BODY$
            BEGIN
                NEW.search_vector := to_tsvector('portuguese',
                    COALESCE(NEW.title, '') || ' ' ||
                    COALESCE(NEW.description, '') || ' ' ||
                    COALESCE(NEW.location, '') || ' ' ||
                    COALESCE(NEW.reporting_department, '') || ' ' ||
                    COALESCE(NEW.patient_id, '')
                );
                RETURN NEW;
            END;
            $BODY$ LANGUAGE plpgsql;

            -- Remover o trigger antigo se existir para evitar duplicaÃ§Ã£o ou erros
            DROP TRIGGER IF EXISTS trg_notifications_search_vector ON notifications;
            CREATE TRIGGER trg_notifications_search_vector
            BEFORE INSERT OR UPDATE ON notifications
            FOR EACH ROW EXECUTE FUNCTION update_notification_search_vector();

            CREATE TABLE IF NOT EXISTS notification_attachments (
                id SERIAL PRIMARY KEY,
                notification_id INTEGER NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
                unique_name VARCHAR(255) NOT NULL, -- Nome Ãºnico do arquivo no disco
                original_name VARCHAR(255) NOT NULL, -- Nome original do arquivo
                uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_attachments_notification_id ON notification_attachments (notification_id);

            CREATE TABLE IF NOT EXISTS notification_history (
                id SERIAL PRIMARY KEY,
                notification_id INTEGER NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
                action_type VARCHAR(255) NOT NULL, -- e.g., 'NotificaÃ§Ã£o criada', 'Classificada', 'ExecuÃ§Ã£o concluÃ­da'
                performed_by VARCHAR(255), -- Nome de usuÃ¡rio ou 'Sistema'
                action_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                details TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_history_notification_id ON notification_history (notification_id);
            CREATE INDEX IF NOT EXISTS idx_history_timestamp ON notification_history (action_timestamp);

            CREATE TABLE IF NOT EXISTS notification_actions (
                id SERIAL PRIMARY KEY,
                notification_id INTEGER NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
                executor_id INTEGER REFERENCES users(id),
                executor_name VARCHAR(255), -- Para facilitar a exibiÃ§Ã£o, embora executor_id seja a FK
                description TEXT NOT NULL,
                action_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                final_action_by_executor BOOLEAN NOT NULL DEFAULT FALSE,
                evidence_description TEXT,
                evidence_attachments JSONB -- Lista de {unique_name, original_name} para evidÃªncias (pode ser uma FK para notification_attachments ou JSONB para pequenos dados)
            );
            CREATE INDEX IF NOT EXISTS idx_actions_notification_id ON notification_actions (notification_id);
            CREATE INDEX IF NOT EXISTS idx_actions_executor_id ON notification_actions (executor_id);
            CREATE INDEX IF NOT EXISTS idx_actions_timestamp ON notification_actions (action_timestamp);
        """)

        # Adiciona usuÃ¡rio admin padrÃ£o se nÃ£o existir
        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0:
            admin_password_hash = hash_password("6105/*") # Hash da senha padrÃ£o
            cur.execute("""
                INSERT INTO users (username, password_hash, name, email, roles, active)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, ('admin', admin_password_hash, 'Administrador', 'admin@hospital.com',
                  ['admin', 'classificador', 'executor', 'aprovador'], True))
            conn.commit() # Confirma a inserÃ§Ã£o do usuÃ¡rio admin
            st.toast("UsuÃ¡rio administrador padrÃ£o criado no banco de dados!")         


        conn.commit() # Confirma todas as operaÃ§Ãµes de criaÃ§Ã£o de tabelas e Ã­ndices
        cur.close() # Fecha o cursor apÃ³s o uso

    except psycopg2.Error as e:
        # Em caso de qualquer erro de banco de dados, exibe uma mensagem e faz rollback
        st.error(f"Erro ao inicializar o banco de dados: {e}")
        if conn:
            conn.rollback() # Reverte quaisquer alteraÃ§Ãµes incompletas

    finally:
        # Garante que a conexÃ£o seja fechada, mesmo que ocorra um erro
        if conn:
            conn.close()

def load_users() -> List[Dict]:
    """Carrega dados de usuÃ¡rio do banco de dados."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, username, password_hash, name, email, roles, active, created_at FROM users ORDER BY name")
        users_raw = cur.fetchall()
        cur.close()
        return [
            {
                "id": u[0],
                "username": u[1],
                "password": u[2],  # Armazenando o hash, nÃ£o a senha em texto claro
                "name": u[3],
                "email": u[4],
                "roles": u[5],  # Lista de strings
                "active": u[6],
                "created_at": u[7].isoformat() if u[7] else None
            }
            for u in users_raw
        ]
    except psycopg2.Error as e:
        st.error(f"Erro ao carregar usuÃ¡rios: {e}")
        return []
    finally:
        if conn:
            conn.close()


def create_user(data: Dict) -> Optional[Dict]:
    """Cria um novo registro de usuÃ¡rio no banco de dados."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Verifica se o username jÃ¡ existe
        cur.execute("SELECT id FROM users WHERE username = %s", (data.get('username', '').lower(),))
        if cur.fetchone():
            return None  # Username jÃ¡ existe

        user_password_hash = hash_password(data.get('password', '').strip())
        cur.execute("""
            INSERT INTO users (username, password_hash, name, email, roles, active, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, username, password_hash, name, email, roles, active, created_at
        """, (
            data.get('username', '').strip(),
            user_password_hash,
            data.get('name', '').strip(),
            data.get('email', '').strip(),
            data.get('roles', []),  # psycopg2 lida bem com listas Python para TEXT[]
            True,
            datetime.now().isoformat()
        ))
        new_user_raw = cur.fetchone()
        conn.commit()
        cur.close()

        if new_user_raw:
            return {
                "id": new_user_raw[0],
                "username": new_user_raw[1],
                "password": new_user_raw[2],  # Retorna o hash para consistÃªncia com load_users
                "name": new_user_raw[3],
                "email": new_user_raw[4],
                "roles": new_user_raw[5],
                "active": new_user_raw[6],
                "created_at": new_user_raw[7].isoformat()
            }
        return None
    except psycopg2.Error as e:
        st.error(f"Erro ao criar usuÃ¡rio: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def update_user(user_id: int, updates: Dict) -> Optional[Dict]:
    """Atualiza um registro de usuÃ¡rio existente no banco de dados."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        set_clauses = []
        values = []
        for key, value in updates.items():
            if key == 'password' and value:  # Se for para atualizar a senha
                set_clauses.append(sql.Identifier('password_hash') + sql.SQL(' = %s'))
                values.append(hash_password(value))
            elif key == 'roles':  # Se for para atualizar roles (array de texto)
                set_clauses.append(sql.Identifier(key) + sql.SQL(' = %s'))
                values.append(list(value))  # Converte para lista explicitamente, se for um set
            elif key not in ['id', 'username', 'created_at']:  # Campos que nÃ£o devem ser atualizados diretamente
                set_clauses.append(sql.Identifier(key) + sql.SQL(' = %s'))
                values.append(value)

        if not set_clauses:
            return None  # Nenhuma atualizaÃ§Ã£o para aplicar

        query = sql.SQL(
            "UPDATE users SET {} WHERE id = %s RETURNING id, username, password_hash, name, email, roles, active, created_at").format(
            sql.SQL(', ').join(set_clauses)
        )
        values.append(user_id)

        cur.execute(query, values)
        updated_user_raw = cur.fetchone()
        conn.commit()
        cur.close()

        if updated_user_raw:
            return {
                "id": updated_user_raw[0],
                "username": updated_user_raw[1],
                "password": updated_user_raw[2],  # hash
                "name": updated_user_raw[3],
                "email": updated_user_raw[4],
                "roles": updated_user_raw[5],
                "active": updated_user_raw[6],
                "created_at": updated_user_raw[7].isoformat()
            }
        return None
    except psycopg2.Error as e:
        st.error(f"Erro ao atualizar usuÃ¡rio: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def load_notifications() -> List[Dict]:
    """Carrega dados de notificaÃ§Ã£o do banco de dados, incluindo dados relacionados."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                id, title, description, location, occurrence_date, occurrence_time,
                reporting_department, reporting_department_complement, notified_department,
                notified_department_complement, event_shift, immediate_actions_taken,
                immediate_action_description, patient_involved, patient_id, patient_outcome_obito,
                additional_notes, status, created_at,
                classification, rejection_classification, review_execution, approval,
                rejection_approval, rejection_execution_review, conclusion,
                executors, approver
            FROM notifications ORDER BY created_at DESC
        """)
        notifications_raw = cur.fetchall()

        # Mapeamento dos nomes das colunas para facilitar a construÃ§Ã£o do dicionÃ¡rio
        column_names = [desc[0] for desc in cur.description]
        notifications = []
        for row in notifications_raw:
            notification = dict(zip(column_names, row))
            # Ajustar tipos de dados que nÃ£o sÃ£o serializÃ¡veis para JSON (datetime, date, time)
            if notification.get('occurrence_date'):
                notification['occurrence_date'] = notification['occurrence_date'].isoformat()
            if notification.get('occurrence_time'):
                notification['occurrence_time'] = notification['occurrence_time'].isoformat()
            if notification.get('created_at'):
                notification['created_at'] = notification['created_at'].isoformat()

            # Buscar dados de outras tabelas para compor a notificaÃ§Ã£o completa
            notification['attachments'] = get_notification_attachments(notification['id'], conn, cur)
            notification['history'] = get_notification_history(notification['id'], conn, cur)
            notification['actions'] = get_notification_actions(notification['id'], conn, cur)

            notifications.append(notification)
        cur.close()
        return notifications
    except psycopg2.Error as e:
        st.error(f"Erro ao carregar notificaÃ§Ãµes: {e}")
        return []
    finally:
        if conn:
            conn.close()


def create_notification(data: Dict, uploaded_files: Optional[List[Any]] = None) -> Dict:
    """
    Cria um novo registro de notificaÃ§Ã£o no banco de dados e seus anexos iniciais.
    Retorna o objeto de notificaÃ§Ã£o completo (como se fosse lido do DB).
    """
    conn = None
    notification_id = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Prepare main notification data
        occurrence_date_iso = data.get('occurrence_date').isoformat() if isinstance(data.get('occurrence_date'),
                                                                                    dt_date_class) else None
        occurrence_time_str = data.get('occurrence_time').isoformat() if isinstance(data.get('occurrence_time'),
                                                                                    dt_time_class) else None

        cur.execute("""
            INSERT INTO notifications (
                title, description, location, occurrence_date, occurrence_time,
                reporting_department, reporting_department_complement, notified_department,
                notified_department_complement, event_shift, immediate_actions_taken,
                immediate_action_description, patient_involved, patient_id, patient_outcome_obito,
                additional_notes, status, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get('title', '').strip(),
            data.get('description', '').strip(),
            data.get('location', '').strip(),
            occurrence_date_iso,
            occurrence_time_str,
            data.get('reporting_department', '').strip(),
            data.get('reporting_department_complement', '').strip(),
            data.get('notified_department', '').strip(),
            data.get('notified_department_complement', '').strip(),
            data.get('event_shift', UI_TEXTS.selectbox_default_event_shift),
            data.get('immediate_actions_taken') == "Sim",  # Converte para booleano
            data.get('immediate_action_description', '').strip() if data.get(
                'immediate_actions_taken') == "Sim" else None,
            data.get('patient_involved') == "Sim",  # Converte para booleano
            data.get('patient_id', '').strip() if data.get('patient_involved') == "Sim" else None,
            (True if data.get('patient_outcome_obito') == "Sim" else False if data.get(
                'patient_outcome_obito') == "NÃ£o" else None) if data.get('patient_involved') == "Sim" else None,
            data.get('additional_notes', '').strip(),
            "pendente_classificacao",
            datetime.now().isoformat()
        ))
        notification_id = cur.fetchone()[0]

        # Save initial attachments
        if uploaded_files:
            for file in uploaded_files:
                # save_uploaded_file_to_disk salva o arquivo no sistema de arquivos
                saved_file_info = save_uploaded_file_to_disk(file, notification_id)
                if saved_file_info:
                    # E adiciona o registro na tabela notification_attachments
                    cur.execute("""
                        INSERT INTO notification_attachments (notification_id, unique_name, original_name)
                        VALUES (%s, %s, %s)
                    """, (notification_id, saved_file_info['unique_name'], saved_file_info['original_name']))

        # Add initial history entry
        add_history_entry(
            notification_id,
            "NotificaÃ§Ã£o criada",
            "Sistema (FormulÃ¡rio PÃºblico)",
            f"NotificaÃ§Ã£o enviada para classificaÃ§Ã£o. TÃ­tulo: {data.get('title', 'Sem tÃ­tulo')[:100]}..." if len(
                data.get('title',
                         '')) > 100 else f"NotificaÃ§Ã£o enviada para classificaÃ§Ã£o. TÃ­tulo: {data.get('title', 'Sem tÃ­tulo')}",
            conn=conn,  # Passa a conexÃ£o para a funÃ§Ã£o de histÃ³rico usar a mesma transaÃ§Ã£o
            cursor=cur  # Passa o cursor
        )

        conn.commit()
        cur.close()

        # Recarregar a notificaÃ§Ã£o completa para retornar
        # Isso Ã© importante porque a notificaÃ§Ã£o pode ter valores padrÃ£o ou triggers que a modificam
        # (Chama load_notifications e filtra pelo ID)
        all_notifications = load_notifications()  # Recarrega tudo para garantir consistÃªncia
        created_notification = next((n for n in all_notifications if n['id'] == notification_id), None)
        return created_notification

    except psycopg2.Error as e:
        st.error(f"Erro ao criar notificaÃ§Ã£o: {e}")
        if conn:
            conn.rollback()  # Rollback em caso de erro
        return {}  # Retorna um dicionÃ¡rio vazio em caso de falha grave
    finally:
        if conn:
            conn.close()


def update_notification(notification_id: int, updates: Dict):
    """
    Atualiza um registro de notificaÃ§Ã£o com novos dados no banco de dados.
    Esta funÃ§Ã£o Ã© inteligente para lidar com campos JSONB e arrays,
    alÃ©m de campos simples.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        set_clauses = []
        values = []

        # Mapeamento para garantir que booleanos e datas/tempos sejam formatados corretamente para o DB
        # E que dicionÃ¡rios sejam serializados para JSONB
        column_mapping = {
            'immediate_actions_taken': lambda x: True if x == "Sim" else False if x == "NÃ£o" else None,
            'patient_involved': lambda x: True if x == "Sim" else False if x == "NÃ£o" else None,
            'patient_outcome_obito': lambda x: (True if x == "Sim" else False if x == "NÃ£o" else None),
            'occurrence_date': lambda x: x.isoformat() if isinstance(x, dt_date_class) else x,
            'occurrence_time': lambda x: x.isoformat() if isinstance(x, dt_time_class) else x,
            'classification': lambda x: json.dumps(x) if x is not None else None,
            'rejection_classification': lambda x: json.dumps(x) if x is not None else None,
            'review_execution': lambda x: json.dumps(x) if x is not None else None,
            'approval': lambda x: json.dumps(x) if x is not None else None,
            'rejection_approval': lambda x: json.dumps(x) if x is not None else None,
            'rejection_execution_review': lambda x: json.dumps(x) if x is not None else None,
            'conclusion': lambda x: json.dumps(x) if x is not None else None,
            'executors': lambda x: x  # psycopg2 lida bem com arrays Python para INTEGER[]
        }

        for key, value in updates.items():
            if key not in ['id', 'created_at', 'attachments', 'actions',
                           'history']:  # NÃ£o atualiza IDs ou listas complexas aqui
                if key in column_mapping:
                    set_clauses.append(sql.Identifier(key) + sql.SQL(' = %s'))
                    values.append(column_mapping[key](value))
                else:
                    set_clauses.append(sql.Identifier(key) + sql.SQL(' = %s'))
                    values.append(value)

        if not set_clauses:
            return None  # Nenhuma atualizaÃ§Ã£o para aplicar

        query = sql.SQL("UPDATE notifications SET {} WHERE id = %s").format(
            sql.SQL(', ').join(set_clauses)
        )
        values.append(notification_id)

        cur.execute(query, values)
        conn.commit()
        cur.close()

        # Recarregar a notificaÃ§Ã£o atualizada para retornar (chama load_notifications e filtra)
        all_notifications = load_notifications()
        updated_notification = next((n for n in all_notifications if n['id'] == notification_id), None)
        return updated_notification

    except psycopg2.Error as e:
        st.error(f"Erro ao atualizar notificaÃ§Ã£o: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


# FunÃ§Ãµes auxiliares para buscar dados relacionados (usadas por load_notifications)
def get_notification_attachments(notification_id: int, conn=None, cur=None) -> List[Dict]:
    """Busca anexos para uma notificaÃ§Ã£o especÃ­fica. Pode usar conexÃ£o e cursor existentes."""
    local_conn = conn
    local_cur = cur
    try:
        if not (local_conn and local_cur):
            local_conn = get_db_connection()
            local_cur = local_conn.cursor()

        local_cur.execute("SELECT unique_name, original_name FROM notification_attachments WHERE notification_id = %s",
                          (notification_id,))
        attachments_raw = local_cur.fetchall()
        return [{"unique_name": att[0], "original_name": att[1]} for att in attachments_raw]
    except psycopg2.Error as e:
        st.error(f"Erro ao carregar anexos da notificaÃ§Ã£o {notification_id}: {e}")
        return []
    finally:
        if not (conn and cur) and local_cur: local_cur.close()
        if not (conn and cur) and local_conn: local_conn.close()


def get_notification_history(notification_id: int, conn=None, cur=None) -> List[Dict]:
    """Busca entradas de histÃ³rico para uma notificaÃ§Ã£o. Pode usar conexÃ£o e cursor existentes."""
    local_conn = conn
    local_cur = cur
    try:
        if not (local_conn and local_cur):
            local_conn = get_db_connection()
            local_cur = local_conn.cursor()

        local_cur.execute(
            "SELECT action_type, performed_by, action_timestamp, details FROM notification_history WHERE notification_id = %s ORDER BY action_timestamp",
            (notification_id,))
        history_raw = local_cur.fetchall()
        return [
            {
                "action": h[0],
                "user": h[1],
                "timestamp": h[2].isoformat() if h[2] else None,
                "details": h[3]
            }
            for h in history_raw
        ]
    except psycopg2.Error as e:
        st.error(f"Erro ao carregar histÃ³rico da notificaÃ§Ã£o {notification_id}: {e}")
        return []
    finally:
        if not (conn and cur) and local_cur: local_cur.close()
        if not (conn and cur) and local_conn: local_conn.close()


def get_notification_actions(notification_id: int, conn=None, cur=None) -> List[Dict]:
    """Busca aÃ§Ãµes de executores para uma notificaÃ§Ã£o. Pode usar conexÃ£o e cursor existentes."""
    local_conn = conn
    local_cur = cur
    try:
        if not (local_conn and local_cur):
            local_conn = get_db_connection()
            local_cur = local_conn.cursor()

        local_cur.execute(
            "SELECT executor_id, executor_name, description, action_timestamp, final_action_by_executor, evidence_description, evidence_attachments FROM notification_actions WHERE notification_id = %s ORDER BY action_timestamp",
            (notification_id,))
        actions_raw = local_cur.fetchall()
        return [
            {
                "executor_id": a[0],
                "executor_name": a[1],
                "description": a[2],
                "timestamp": a[3].isoformat() if a[3] else None,
                "final_action_by_executor": a[4],
                "evidence_description": a[5],
                "evidence_attachments": a[6]  # JÃ¡ Ã© JSONB, entÃ£o vem como objeto Python (list/dict)
            }
            for a in actions_raw
        ]
    except psycopg2.Error as e:
        st.error(f"Erro ao carregar aÃ§Ãµes da notificaÃ§Ã£o {notification_id}: {e}")
        return []
    finally:
        if not (conn and cur) and local_cur: local_cur.close()
        if not (conn and cur) and local_conn: local_conn.close()


def add_history_entry(notification_id: int, action: str, user: str, details: str = "", conn=None, cursor=None):
    """
    Adiciona uma entrada ao histÃ³rico de uma notificaÃ§Ã£o.
    Pode usar uma conexÃ£o e cursor existentes para transaÃ§Ãµes, ou criar novas.
    """
    local_conn = conn
    local_cur = cursor
    try:
        if not (local_conn and local_cur):
            local_conn = get_db_connection()
            local_cur = local_conn.cursor()

        local_cur.execute("""
            INSERT INTO notification_history (notification_id, action_type, performed_by, action_timestamp, details)
            VALUES (%s, %s, %s, %s, %s)
        """, (notification_id, action, user, datetime.now().isoformat(), details))

        if not (conn and cursor):  # Se for uma transaÃ§Ã£o separada, faÃ§a commit aqui
            local_conn.commit()
        return True
    except psycopg2.Error as e:
        st.error(f"Erro ao adicionar entrada de histÃ³rico para notificaÃ§Ã£o {notification_id}: {e}")
        if local_conn and not (conn and cursor):
            local_conn.rollback()
        return False
    finally:
        if local_cur and not (conn and cursor):
            local_cur.close()
        if local_conn and not (conn and cursor):
            local_conn.close()


def add_notification_action(notification_id: int, action_data: Dict, conn=None, cur=None):
    """
    Adiciona uma aÃ§Ã£o de executor a uma notificaÃ§Ã£o.
    Pode usar uma conexÃ£o e cursor existentes para transaÃ§Ãµes, ou criar novas.
    """
    local_conn = conn
    local_cur = cur
    try:
        if not (local_conn and local_cur):
            local_conn = get_db_connection()
            local_cur = local_conn.cursor()

        local_cur.execute("""
            INSERT INTO notification_actions (
                notification_id, executor_id, executor_name, description, action_timestamp,
                final_action_by_executor, evidence_description, evidence_attachments
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            notification_id,
            action_data.get('executor_id'),
            action_data.get('executor_name'),
            action_data.get('description'),
            action_data.get('timestamp'),
            action_data.get('final_action_by_executor'),
            action_data.get('evidence_description'),
            json.dumps(action_data.get('evidence_attachments')) if action_data.get('evidence_attachments') else None
        ))
        if not (conn and cur):
            local_conn.commit()
        return True
    except psycopg2.Error as e:
        st.error(f"Erro ao adicionar aÃ§Ã£o para notificaÃ§Ã£o {notification_id}: {e}")
        if local_conn and not (conn and cur):
            local_conn.rollback()
        return False
    finally:
        if local_cur and not (conn and cur): local_cur.close()
        if local_conn and not (conn and cur): local_conn.close()


def save_uploaded_file_to_disk(uploaded_file: Any, notification_id: int) -> Optional[Dict]:
    """Salva um arquivo enviado para o diretÃ³rio de anexos no disco e retorna suas informaÃ§Ãµes."""
    if uploaded_file is None:
        return None
    original_name = uploaded_file.name
    safe_original_name = "".join(c for c in original_name if c.isalnum() or c in ('.', '_', '-')).rstrip('.')
    unique_filename = f"{notification_id}_{uuid.uuid4().hex}_{safe_original_name}"
    file_path = os.path.join(ATTACHMENTS_DIR, unique_filename)
    try:
        os.makedirs(ATTACHMENTS_DIR, exist_ok=True)  # Garante que o diretÃ³rio exista
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        return {"unique_name": unique_filename, "original_name": original_name}
    except Exception as e:
        st.error(f"Erro ao salvar o anexo {original_name} no disco: {e}")
        return None


def get_attachment_data(unique_filename: str) -> Optional[bytes]:
    """LÃª o conteÃºdo de um arquivo de anexo do disco."""
    file_path = os.path.join(ATTACHMENTS_DIR, unique_filename)
    try:
        with open(file_path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        st.warning(f"Anexo nÃ£o encontrado no caminho: {unique_filename}")
        return None
    except Exception as e:
        st.error(f"Erro ao ler o anexo {unique_filename}: {e}")
        return None


# --- FunÃ§Ãµes de AutenticaÃ§Ã£o e AutorizaÃ§Ã£o ---

def hash_password(password: str) -> str:
    """Faz o hash de uma senha usando SHA-256."""
    return hashlib.sha256(password.encode()).hexdigest()


def authenticate_user(username: str, password: str) -> Optional[Dict]:
    """Autentica um usuÃ¡rio com base no nome de usuÃ¡rio e senha."""
    users = load_users()  # Carrega usuÃ¡rios do DB
    hashed_password = hash_password(password)
    for user in users:
        # A senha do user no dicionÃ¡rio retornado por load_users Ã© o hash
        if (user.get('username', '').lower() == username.lower() and
                user.get('password') == hashed_password and  # Compare com o hash jÃ¡ armazenado
                user.get('active', True)):
            return user
    return None


def logout_user():
    """Desloga o usuÃ¡rio atual."""
    st.session_state.authenticated = False
    st.session_state.user = None
    st.session_state.page = 'create_notification'
    _reset_form_state()
    if 'initial_classification_state' in st.session_state: st.session_state.pop('initial_classification_state')
    if 'review_classification_state' in st.session_state: st.session_state.pop('review_classification_state')
    if 'current_initial_classification_id' in st.session_state: st.session_state.pop(
        'current_initial_classification_id')
    if 'current_review_classification_id' in st.session_state: st.session_state.pop('current_review_classification_id')
    # Limpa o estado do formulÃ¡rio de aprovaÃ§Ã£o tambÃ©m
    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')


def check_permission(required_role: str) -> bool:
    """Verifica se o usuÃ¡rio logado possui a funÃ§Ã£o necessÃ¡ria ou Ã© um admin."""
    if not st.session_state.authenticated or st.session_state.user is None:
        return False
    user_roles = st.session_state.user.get('roles', [])
    return required_role in user_roles or 'admin' in user_roles


def get_users_by_role(role: str) -> List[Dict]:
    """Retorna usuÃ¡rios ativos com uma funÃ§Ã£o especÃ­fica."""
    users = load_users()  # Carrega usuÃ¡rios do DB
    return [user for user in users if role in user.get('roles', []) and user.get('active', True)]


# --- FunÃ§Ãµes Auxiliares/UtilitÃ¡rias ---

def get_deadline_status(deadline_date_str: Optional[str], completion_timestamp_str: Optional[str] = None) -> Dict:
    """
    Calcula o status do prazo com base no prazo final e, caso aplicÃ¡vel, tambÃ©m se a notificaÃ§Ã£o foi concluÃ­da a tempo.
    Retorna um dicionÃ¡rio com 'text' (status) e 'class' (classe CSS para estilo).
    """
    if not deadline_date_str:
        return {"text": UI_TEXTS.deadline_days_nan, "class": ""}

    try:
        # CORREÃ‡ÃƒO AQUI: Usando dt_date_class.fromisoformat
        deadline_date = dt_date_class.fromisoformat(deadline_date_str)

        if completion_timestamp_str:
            # A notificaÃ§Ã£o foi concluÃ­da, compare a data de conclusÃ£o com o prazo limite
            completion_date = datetime.fromisoformat(completion_timestamp_str).date()
            if completion_date <= deadline_date:
                return {"text": UI_TEXTS.deadline_status_ontrack, "class": "deadline-ontrack"}
            else:
                return {"text": UI_TEXTS.deadline_status_overdue, "class": "deadline-overdue"}
        else:
            # Caso nÃ£o tenha sido concluÃ­da ainda: verificar relaÃ§Ã£o com a data de hoje
            # CORREÃ‡ÃƒO AQUI: Usando dt_date_class.today()
            today = dt_date_class.today()
            days_diff = (deadline_date - today).days

            if days_diff < 0:
                return {"text": UI_TEXTS.deadline_status_overdue, "class": "deadline-overdue"}  # Prazo vencido
            elif days_diff <= 7:
                return {"text": UI_TEXTS.deadline_status_duesoon, "class": "deadline-duesoon"}  # Prazo prÃ³ximo
            else:
                return {"text": UI_TEXTS.deadline_status_ontrack, "class": "deadline-ontrack"}  # Dentro do prazo
    except ValueError:
        return {"text": UI_TEXTS.text_na, "class": ""}  # Formato invÃ¡lido de data


def format_date_time_summary(date_val: Any, time_val: Any) -> str:
    """Formata data e hora opcional para exibiÃ§Ã£o."""
    date_part_formatted = UI_TEXTS.text_na
    time_part_formatted = ''

    if isinstance(date_val, dt_date_class):  # Corrigido: usa dt_date_class
        date_part_formatted = date_val.strftime('%d/%m/%Y')
    elif isinstance(date_val, str) and date_val:
        try:
            date_part_formatted = datetime.fromisoformat(date_val).date().strftime('%d/%m/%Y')
        except ValueError:
            date_part_formatted = 'Data invÃ¡lida'
    elif date_val is None:
        date_part_formatted = 'NÃ£o informada'

    if isinstance(time_val, dt_time_class):  # Corrigido: usa dt_time_class
        time_part_formatted = f" Ã s {time_val.strftime('%H:%M')}"
    elif isinstance(time_val, str) and time_val and time_val.lower() != 'none':
        try:
            time_str_part = time_val.split('.')[0]
            try:
                time_obj = datetime.strptime(time_str_part, '%H:%M:%S').time()
                # CORREÃ‡ÃƒO: Tratar o caso de "00:00:00" para evitar que seja exibido para campos de hora vazios
                if time_obj == datetime.strptime("00:00:00", '%H:%M:%S').time():
                    time_part_formatted = ''
                else:
                    time_part_formatted = f" Ã s {time_obj.strftime('%H:%M')}"
            except ValueError:
                try:
                    time_obj = datetime.strptime("00:00", '%H:%M').time()
                    if time_obj == datetime.strptime("00:00", '%H:%M').time():
                        time_part_formatted = ''
                    else:
                        time_part_formatted = f" Ã s {time_obj.strftime('%H:%M')}"
                except ValueError:
                    time_part_formatted = f" Ã s {time_val}"
                    time_obj = None

        except ValueError:
            time_part_formatted = f" Ã s {time_val}"
    elif time_val is None:
        time_part_formatted = ''

    return f"{date_part_formatted}{time_part_formatted}"


def _clear_execution_form_state(notification_id: int):
    """Limpa as chaves do session_state para o formulÃ¡rio de execuÃ§Ã£o apÃ³s o envio."""
    key_desc = f"exec_action_desc_{notification_id}_refactored"
    key_choice = f"exec_action_choice_{notification_id}_refactored"
    key_evidence_desc = f"exec_evidence_desc_{notification_id}_refactored"
    key_evidence_attachments = f"exec_evidence_attachments_{notification_id}_refactored"

    if key_desc in st.session_state:
        del st.session_state[key_desc]
    if key_choice in st.session_state:
        del st.session_state[key_choice]
    if key_evidence_desc in st.session_state:
        del st.session_state[key_evidence_desc]
    if key_evidence_attachments in st.session_state:
        del st.session_state[key_evidence_attachments]


def _clear_approval_form_state(notification_id: int):
    """Limpa as chaves do session_state para o formulÃ¡rio de aprovaÃ§Ã£o."""
    key_notes = f"approval_notes_{notification_id}_refactored"
    key_decision = f"approval_decision_{notification_id}_refactored"

    if key_notes in st.session_state:
        del st.session_state[key_notes]
    if key_decision in st.session_state:
        del st.session_state[key_decision]

    # TambÃ©m remove o estado do formulÃ¡rio de aprovaÃ§Ã£o da notificaÃ§Ã£o especÃ­fica
    if 'approval_form_state' in st.session_state and notification_id in st.session_state.approval_form_state:
        del st.session_state.approval_form_state[notification_id]


def _reset_form_state():
    """Reinicia as variÃ¡veis de estado para o formulÃ¡rio de criaÃ§Ã£o de notificaÃ§Ã£o e outros estados especÃ­ficos da pÃ¡gina."""
    keys_to_clear = [
        'form_step', 'create_form_data',
        'create_title_state_refactored', 'create_location_state_refactored',
        'create_occurrence_date_state_refactored', 'create_event_time_state_refactored',
        'create_reporting_dept_state_refactored', 'create_reporting_dept_comp_state_refactored',
        'create_event_shift_state_refactored', 'create_description_state_refactored',
        'immediate_actions_taken_state_refactored', 'create_immediate_action_desc_state_refactored',
        'patient_involved_state_refactored', 'create_patient_id_state_refactored',
        'create_patient_outcome_obito_state_refactored', 'create_notified_dept_state_refactored',
        'create_notified_dept_comp_state_refactored', 'additional_notes_state_refactored',
        'create_attachments_state_refactored',
        # Dashboard states
        'dashboard_filter_status', 'dashboard_filter_nnc', 'dashboard_filter_priority',
        'dashboard_filter_date_start', 'dashboard_filter_date_end', 'dashboard_search_query',
        'dashboard_sort_column', 'dashboard_sort_ascending', 'dashboard_current_page', 'dashboard_items_per_page'
    ]
    current_keys = set(st.session_state.keys())
    for key in current_keys:
        if key in keys_to_clear:
            st.session_state.pop(key, None)

    st.session_state.form_step = 1
    st.session_state.create_form_data = {
        'title': '', 'location': '', 'occurrence_date': datetime.now().date(),
        'occurrence_time': datetime.now().time(),
        'reporting_department': UI_TEXTS.selectbox_default_department_select,  # <-- ALTERADO
        'reporting_department_complement': '', 'event_shift': UI_TEXTS.selectbox_default_event_shift,
        'description': '',
        'immediate_actions_taken': UI_TEXTS.selectbox_default_immediate_actions_taken,
        'immediate_action_description': '',
        'patient_involved': UI_TEXTS.selectbox_default_patient_involved,
        'patient_id': '',
        'patient_outcome_obito': UI_TEXTS.selectbox_default_patient_outcome_obito,
        'notified_department': UI_TEXTS.selectbox_default_department_select,  # <-- ALTERADO
        'notified_department_complement': '', 'additional_notes': '', 'attachments': []
    }


# --- FunÃ§Ãµes de RenderizaÃ§Ã£o da Interface (UI) ---

def show_sidebar():
    """Renderiza a barra lateral com navegaÃ§Ã£o e informaÃ§Ãµes do usuÃ¡rio/login."""
    with st.sidebar:
        st.image("logo.png", use_container_width=True)
        st.markdown("<h2 class='sidebar-main-title'>Portal de NotificaÃ§Ãµes</h2>", unsafe_allow_html=True)
        st.markdown("<h3 class='sidebar-subtitle'>Santa Casa de PoÃ§os de Caldas</h3>", unsafe_allow_html=True)
        st.markdown("---")

        if st.session_state.authenticated and st.session_state.user:
            st.markdown(f"""
            <div class="user-info">
                <strong>ðŸ‘¤ {st.session_state.user.get('name', 'UsuÃ¡rio')}</strong><br>
                <small>{st.session_state.user.get('username', UI_TEXTS.text_na)}</small><br>
                <small>FunÃ§Ãµes: {', '.join(st.session_state.user.get('roles', [])) or 'Nenhuma'}</small>
            </div>
            """, unsafe_allow_html=True)
            st.markdown("### ðŸ“‹ Menu Principal")

            user_roles = st.session_state.user.get('roles', [])

            if st.button("ðŸ“ Nova NotificaÃ§Ã£o", key="nav_create_notif", use_container_width=True):
                st.session_state.page = 'create_notification'
                _reset_form_state()
                if 'initial_classification_state' in st.session_state: st.session_state.pop(
                    'initial_classification_state')
                if 'review_classification_state' in st.session_state: st.session_state.pop(
                    'review_classification_state')
                if 'current_initial_classification_id' in st.session_state: st.session_state.pop(
                    'current_initial_classification_id')
                if 'current_review_classification_id' in st.session_state: st.session_state.pop(
                    'current_review_classification_id')
                if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                st.rerun()

            if 'classificador' in user_roles or 'admin' in user_roles:  # Adicione esta linha de verificaÃ§Ã£o
                if st.button("ðŸ“Š Dashboard de NotificaÃ§Ãµes", key="nav_dashboard", use_container_width=True):
                    st.session_state.page = 'dashboard'
                    _reset_form_state()
                    st.rerun()

            if 'classificador' in user_roles or 'admin' in user_roles:
                if st.button("ðŸ” ClassificaÃ§Ã£o/RevisÃ£o", key="nav_classification",
                             use_container_width=True):
                    st.session_state.page = 'classification'
                    _reset_form_state()
                    if 'initial_classification_state' in st.session_state: st.session_state.pop(
                        'initial_classification_state')
                    if 'review_classification_state' in st.session_state: st.session_state.pop(
                        'review_classification_state')
                    if 'current_initial_classification_id' in st.session_state: st.session_state.pop(
                        'current_initial_classification_id')
                    if 'current_review_classification_id' in st.session_state: st.session_state.pop(
                        'current_review_classification_id')
                    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                    st.rerun()

            if 'executor' in user_roles or 'admin' in user_roles:
                if st.button("âš¡ ExecuÃ§Ã£o", key="nav_execution", use_container_width=True):
                    st.session_state.page = 'execution'
                    _reset_form_state()
                    if 'initial_classification_state' in st.session_state: st.session_state.pop(
                        'initial_classification_state')
                    if 'review_classification_state' in st.session_state: st.session_state.pop(
                        'review_classification_state')
                    if 'current_initial_classification_id' in st.session_state: st.session_state.pop(
                        'current_initial_classification_id')
                    if 'current_review_classification_id' in st.session_state: st.session_state.pop(
                        'current_review_classification_id')
                    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                    st.rerun()

            if 'aprovador' in user_roles or 'admin' in user_roles:
                if st.button("âœ… AprovaÃ§Ã£o", key="nav_approval", use_container_width=True):
                    st.session_state.page = 'approval'
                    _reset_form_state()
                    if 'initial_classification_state' in st.session_state: st.session_state.pop(
                        'initial_classification_state')
                    if 'review_classification_state' in st.session_state: st.session_state.pop(
                        'review_classification_state')
                    if 'current_initial_classification_id' in st.session_state: st.session_state.pop(
                        'current_initial_classification_id')
                    if 'current_review_classification_id' in st.session_state: st.session_state.pop(
                        'current_review_classification_id')
                    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                    st.rerun()

            if 'admin' in user_roles:
                if st.button("âš™ï¸ AdministraÃ§Ã£o", key="nav_admin", use_container_width=True):
                    st.session_state.page = 'admin'
                    _reset_form_state()
                    if 'initial_classification_state' in st.session_state: st.session_state.pop(
                        'initial_classification_state')
                    if 'review_classification_state' in st.session_state: st.session_state.pop(
                        'review_classification_state')
                    if 'current_initial_classification_id' in st.session_state: st.session_state.pop(
                        'current_initial_classification_id')
                    if 'current_review_classification_id' in st.session_state: st.session_state.pop(
                        'current_review_classification_id')
                    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                    st.rerun()

            st.markdown("---")
            if st.button("ðŸšª Sair", key="nav_logout", use_container_width=True):
                logout_user()
                st.rerun()  # MANTIDO: O logout exige um reinÃ­cio completo para limpar todo o estado.
        else:
            st.markdown("### ðŸ” Login do Operador")
            with st.form("sidebar_login_form"):
                username = st.text_input("UsuÃ¡rio", key="sidebar_username_form")
                password = st.text_input("Senha", type="password", key="sidebar_password_form")
                if st.form_submit_button("ðŸ”‘ Entrar", use_container_width=True):
                    user = authenticate_user(st.session_state.sidebar_username_form,
                                             st.session_state.sidebar_password_form)
                    if user:
                        st.session_state.authenticated = True
                        st.session_state.user = user
                        st.success(f"Login realizado com sucesso! Bem-vindo, {user.get('name', 'UsuÃ¡rio')}.")
                        st.session_state.pop('sidebar_username_form', None)
                        st.session_state.pop('sidebar_password_form', None)
                        if 'classificador' in user.get('roles', []) or 'admin' in user.get('roles', []):
                            st.session_state.page = 'classification'
                        else:
                            st.session_state.page = 'create_notification'
                        st.rerun()
                    else:
                        st.error("UsuÃ¡rio ou senha invÃ¡lidos!")
            st.markdown("---")

        st.markdown("""
        <div class="sidebar-footer">
            NotificaSanta v2.0.1<br>
            &copy; 2025 Todos os direitos reservados
        </div>
        """, unsafe_allow_html=True)


def display_notification_full_details(notification: Dict, user_id_logged_in: Optional[int] = None,
                                      user_username_logged_in: Optional[str] = None):
    st.markdown("###    Detalhes da NotificaÃ§Ã£o")
    col_det1, col_det2 = st.columns(2)
    with col_det1:
        st.markdown("**ðŸ“ Evento Reportado Original**")
        st.write(f"**TÃ­tulo:** {notification.get('title', UI_TEXTS.text_na)}")
        st.write(f"**Local:** {notification.get('location', UI_TEXTS.text_na)}")
        occurrence_datetime_summary = format_date_time_summary(notification.get('occurrence_date'),
                                                               notification.get('occurrence_time'))
        st.write(f"**Data/Hora OcorrÃªncia:** {occurrence_datetime_summary}")
        st.write(f"**Setor Notificante:** {notification.get('reporting_department', UI_TEXTS.text_na)}")
        if notification.get('immediate_actions_taken') and notification.get('immediate_action_description'):
            st.info(
                f"**AÃ§Ãµes Imediatas Reportadas:** {notification.get('immediate_action_description', UI_TEXTS.text_na)[:300]}...")

    with col_det2:
        st.markdown("**â±ï¸ InformaÃ§Ãµes de GestÃ£o e ClassificaÃ§Ã£o**")
        classif = notification.get('classification') or {}
        st.write(f"**ClassificaÃ§Ã£o NNC:** {classif.get('nnc', UI_TEXTS.text_na)}")
        if classif.get('nivel_dano'): st.write(f"**NÃ­vel de Dano:** {classif.get('nivel_dano', UI_TEXTS.text_na)}")
        st.write(f"**Prioridade:** {classif.get('prioridade', UI_TEXTS.text_na)}")
        st.write(f"**Never Event:** {classif.get('never_event', UI_TEXTS.text_na)}")
        st.write(f"**Evento Sentinela:** {'Sim' if classif.get('is_sentinel_event') else 'NÃ£o'}")
        st.write(f"**Tipo Principal:** {classif.get('event_type_main', UI_TEXTS.text_na)}")
        sub_type_display_closed = ''
        if classif.get('event_type_sub'):
            if isinstance(classif['event_type_sub'], list):
                sub_type_display_closed = ', '.join(classif['event_type_sub'])
            else:
                sub_type_display_closed = str(classif['event_type_sub'])
        if sub_type_display_closed: st.write(f"**EspecificaÃ§Ã£o:** {sub_type_display_closed}")
        st.write(f"**ClassificaÃ§Ã£o OMS:** {', '.join(classif.get('oms', [UI_TEXTS.text_na]))}")
        st.write(f"**Classificado por:** {classif.get('classificador', UI_TEXTS.text_na)}")

        # ExibiÃ§Ã£o do Prazo e Status
        deadline_date_str = classif.get('deadline_date')
        if deadline_date_str:
            deadline_date_formatted = datetime.fromisoformat(deadline_date_str).strftime('%d/%m/%Y')
            completion_timestamp_str = (notification.get('conclusion') or {}).get('timestamp')
            # Em seguida, passe-o para a funÃ§Ã£o get_deadline_status
            deadline_status = get_deadline_status(deadline_date_str, completion_timestamp_str)
            st.markdown(
                f"**Prazo de ConclusÃ£o:** {deadline_date_formatted} (<span class='{deadline_status['class']}'>{deadline_status['text']}</span>)",
                unsafe_allow_html=True)
        else:
            st.write(f"**Prazo de ConclusÃ£o:** {UI_TEXTS.deadline_days_nan}")

    st.markdown("**ðŸ“ DescriÃ§Ã£o Completa do Evento**")
    st.info(notification.get('description', UI_TEXTS.text_na))
    if classif.get('notes'):
        st.markdown("**ðŸ“‹ OrientaÃ§Ãµes / ObservaÃ§Ãµes do Classificador**")
        st.success(classif.get('notes', UI_TEXTS.text_na))

    if notification.get('actions'):
        st.markdown("#### âš¡ HistÃ³rico de AÃ§Ãµes")
        for action in sorted(notification['actions'], key=lambda x: x.get('timestamp', '')):
            action_type = "ðŸ CONCLUSÃƒO (Executor)" if action.get('final_action_by_executor') else "ðŸ“ AÃ‡ÃƒO Registrada"
            action_timestamp = action.get('timestamp', UI_TEXTS.text_na)
            if action_timestamp != UI_TEXTS.text_na:
                try:
                    action_timestamp = datetime.fromisoformat(action_timestamp).strftime('%d/%m/%Y %H:%M:%S')
                except ValueError:
                    pass

            if user_id_logged_in and action.get('executor_id') == user_id_logged_in:
                st.markdown(f"""
                <div class='my-action-entry-card'>
                    <strong>{action_type}</strong> - por <strong>VOCÃŠ ({action.get('executor_name', UI_TEXTS.text_na)})</strong> em {action_timestamp}
                    <br>
                    <em>{action.get('description', UI_TEXTS.text_na)}</em>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class='action-entry-card'>
                    <strong>{action_type}</strong> - por <strong>{action.get('executor_name', UI_TEXTS.text_na)}</strong> em {action_timestamp}
                    <br>
                    <em>{action.get('description', UI_TEXTS.text_na)}</em>
                </div>
                """, unsafe_allow_html=True)

            # NOVO: Exibir evidÃªncias se disponÃ­vel e for uma aÃ§Ã£o final
            if action.get('final_action_by_executor'):
                evidence_desc = action.get('evidence_description', '').strip()
                evidence_atts = action.get('evidence_attachments', [])
                if evidence_desc or evidence_atts:
                    st.markdown(f"""<div class='evidence-section'>""", unsafe_allow_html=True)
                    st.markdown("<h6>EvidÃªncias da ConclusÃ£o:</h6>", unsafe_allow_html=True)
                    if evidence_desc:
                        st.info(evidence_desc)
                    if evidence_atts:
                        for attach_info in evidence_atts:
                            unique_name = attach_info.get('unique_name')
                            original_name = attach_info.get('original_name')
                            if unique_name and original_name:
                                file_content = get_attachment_data(unique_name)
                                if file_content:
                                    st.download_button(
                                        label=f"Baixar EvidÃªncia: {original_name}",
                                        data=file_content,
                                        file_name=original_name,
                                        mime="application/octet-stream",
                                        key=f"download_action_evidence_{notification['id']}_{unique_name}"
                                    )
                                else:
                                    st.write(f"Anexo: {original_name} (arquivo nÃ£o encontrado ou corrompido)")
                    st.markdown(f"""</div>""", unsafe_allow_html=True)

            st.markdown("---")

    if notification.get('review_execution'):
        st.markdown("#### ðŸ› ï¸ RevisÃ£o de ExecuÃ§Ã£o")
        review_exec = notification['review_execution']
        st.write(f"**DecisÃ£o:** {review_exec.get('decision', UI_TEXTS.text_na)}")
        st.write(f"**Revisado por:** {review_exec.get('reviewed_by', UI_TEXTS.text_na)}")
        st.write(f"**ObservaÃ§Ãµes:** {review_exec.get('notes', UI_TEXTS.text_na)}")
        if review_exec.get('rejection_reason'):
            st.write(f"**Motivo RejeiÃ§Ã£o:** {review_exec.get('rejection_reason', UI_TEXTS.text_na)}")
    if notification.get('approval'):
        st.markdown("#### âœ… AprovaÃ§Ã£o Final")
        approval_info = notification['approval']
        if user_username_logged_in and approval_info.get('approved_by') == user_username_logged_in:
            st.markdown(f"""
            <div style='background-color: #e6ffe6; padding: 10px; border-radius: 5px; border-left: 3px solid #4CAF50;'>
                <strong>DecisÃ£o:</strong> {approval_info.get('decision', UI_TEXTS.text_na)}
                <br>
                <strong>Aprovado por:</strong> VOCÃŠ ({approval_info.get('approved_by', UI_TEXTS.text_na)})
                <br>
                <strong>ObservaÃ§Ãµes:</strong> {approval_info.get('notes', UI_TEXTS.text_na)}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.write(f"**DecisÃ£o:** {approval_info.get('decision', UI_TEXTS.text_na)}")
            st.write(f"**Aprovado por:** {approval_info.get('approved_by', UI_TEXTS.text_na)}")
            st.write(f"**ObservaÃ§Ãµes:** {approval_info.get('notes', UI_TEXTS.text_na)}")

    if notification.get('rejection_classification'):
        st.markdown("#### âŒ RejeiÃ§Ã£o na ClassificaÃ§Ã£o Inicial")
        rej_classif = notification['rejection_classification']
        st.write(f"**Motivo:** {rej_classif.get('reason', UI_TEXTS.text_na)}")
        st.write(f"**Rejeitado por:** {rej_classif.get('classified_by', UI_TEXTS.text_na)}")

    if notification.get('rejection_approval'):
        st.markdown("#### â›” Reprovada na AprovaÃ§Ã£o")
        rej_appr = notification['rejection_approval']
        if user_username_logged_in and rej_appr.get('rejected_by') == user_username_logged_in:
            st.markdown(f"""
            <div style='background-color: #ffe6e6; padding: 10px; border-radius: 5px; border-left: 3px solid #f44336;'>
                <strong>Motivo:</strong> {rej_appr.get('reason', UI_TEXTS.text_na)}
                <br>
                <strong>Reprovado por:</strong> VOCÃŠ ({rej_appr.get('rejected_by', UI_TEXTS.text_na)})
            </div>
            """, unsafe_allow_html=True)
        else:
            st.write(f"**Motivo:** {rej_appr.get('reason', UI_TEXTS.text_na)}")
            st.write(f"**Reprovado por:** {rej_appr.get('rejected_by', UI_TEXTS.text_na)}")

    if notification.get('rejection_execution_review'):
        st.markdown("#### ðŸ”„ ExecuÃ§Ã£o Rejeitada (RevisÃ£o do Classificador)")
        rej_exec_review = notification['rejection_execution_review']
        if user_username_logged_in and rej_exec_review.get('reviewed_by') == user_username_logged_in:
            st.markdown(f"""
            <div style='background-color: #ffe6e6; padding: 10px; border-radius: 5px; border-left: 3px solid #f44336;'>
                <strong>Motivo:</strong> {rej_exec_review.get('reason', UI_TEXTS.text_na)}
                <br>
                <strong>Rejeitado por:</strong> VOCÃŠ ({rej_exec_review.get('reviewed_by', UI_TEXTS.text_na)})
            </div>
            """, unsafe_allow_html=True)
        else:
            st.write(f"**Motivo:** {rej_exec_review.get('reason', UI_TEXTS.text_na)}")
            st.write(f"**Rejeitado por:** {rej_exec_review.get('reviewed_by', UI_TEXTS.text_na)}")

    if notification.get('attachments'):
        st.markdown("#### ðŸ“Ž Anexos")
        for attach_info in notification['attachments']:
            unique_name_to_use = None
            original_name_to_use = None
            if isinstance(attach_info, dict) and 'unique_name' in attach_info and 'original_name' in attach_info:
                unique_name_to_use = attach_info['unique_name']
                original_name_to_use = attach_info['original_name']
            elif isinstance(attach_info, str):  # Fallback para compatibilidade antiga
                unique_name_to_use = attach_info
                original_name_to_use = attach_info
            if unique_name_to_use:
                file_content = get_attachment_data(unique_name_to_use)
                if file_content:
                    st.download_button(
                        label=f"Baixar {original_name_to_use}",
                        data=file_content,
                        file_name=original_name_to_use,
                        mime="application/octet-stream",
                        key=f"download_closed_{notification['id']}_{unique_name_to_use}"
                    )
                else:
                    st.write(f"Anexo: {original_name_to_use} (arquivo nÃ£o encontrado ou corrompido)")

    st.markdown("---")


@st_fragment
def show_create_notification():
    """
    Renderiza a pÃ¡gina para criar novas notificaÃ§Ãµes como um formulÃ¡rio multi-etapa.
    Controla as etapas usando st.session_state e gerencia a persistÃªncia explÃ­cita de dados e a validaÃ§Ã£o.
    """
    st.markdown("<h1 class='main-header'>ðŸ“ Nova NotificaÃ§Ã£o (FormulÃ¡rio NNC)</h1>", unsafe_allow_html=True)
    if not st.session_state.authenticated:
        st.info("Para acompanhar o fluxo completo de uma notificaÃ§Ã£o (classificaÃ§Ã£o, execuÃ§Ã£o, aprovaÃ§Ã£o), faÃ§a login.")

    # Inicializa o estado do formulÃ¡rio se nÃ£o existir
    if 'form_step' not in st.session_state:
        _reset_form_state()

    current_data = st.session_state.create_form_data

    # NOVO: LÃ³gica para a etapa de sucesso (Etapa 5)
    if st.session_state.form_step == 5:
        st.balloons()
        st.markdown(r"""
        <div style="text-align: center; margin-top: 100px;">
            <h1 style="color: #2E86AB; font-size: 3em;">
                âœ… NotificaÃ§Ã£o Enviada com Sucesso! ðŸ˜Š
            </h1>
            <p style="font-size: 1.2em; color: #555;">
                Obrigado pela sua participaÃ§Ã£o! Voltando para um novo formulÃ¡rio...
            </p>
        </div>
        """, unsafe_allow_html=True)
        time_module.sleep(2)
        _reset_form_state()  # Limpa o formulÃ¡rio para uma nova notificaÃ§Ã£o
        st.session_state.form_step = 1 # Reinicia a aplicaÃ§Ã£o para a primeira etapa do formulÃ¡rio
        st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render
# Se nÃ£o estiver na etapa de sucesso, exibe as etapas normais do formulÃ¡rio
    st.markdown(f"### Etapa {st.session_state.form_step}") # CORREÃ‡ÃƒO: Acessa diretamente

    if st.session_state.form_step == 1: # CORREÃ‡ÃƒO: Acessa diretamente
        with st.container():
            st.markdown("""
            <div class="form-section">
                <h3>ðŸ“‹ Etapa 1: Detalhes da OcorrÃªncia</h3>
                <p>Preencha as informaÃ§Ãµes bÃ¡sicas sobre o evento ocorrido.</p>
            </div>
            """, unsafe_allow_html=True)
            current_data['title'] = st.text_input(
                "TÃ­tulo da NotificaÃ§Ã£o*", value=current_data['title'], placeholder="Breve resumo da notificaÃ§Ã£o",
                help="Descreva brevemente o evento ocorrido", key="create_title_state_refactored")
            current_data['location'] = st.text_input(
                "Local do Evento*", value=current_data['location'],
                placeholder="Ex: UTI - Leito 05, Centro CirÃºrgico - Sala 3",
                help="Especifique o local exato onde ocorreu o evento", key="create_location_state_refactored")
            col1, col2 = st.columns(2)
            with col1:
                current_data['occurrence_date'] = st.date_input(
                    "Data da OcorrÃªncia do Evento*", value=current_data['occurrence_date'],
                    help="Selecione a data em que o evento ocorreu", key="create_occurrence_date_state_refactored",
                    format="DD/MM/YYYY")
            with col2:
                current_data['occurrence_time'] = st.time_input(
                    "Hora Aproximada do Evento", value=current_data['occurrence_time'],
                    help="Hora aproximada em que o evento ocorreu.", key="create_event_time_state_refactored")
            
            # NOVO: Lista de opÃ§Ãµes para Setor Notificante, incluindo o placeholder
            reporting_dept_options = [UI_TEXTS.selectbox_default_department_select] + FORM_DATA.SETORES
            current_data['reporting_department'] = st.selectbox(
                "Setor Notificante*",
                options=reporting_dept_options, # <-- ALTERADO
                index=reporting_dept_options.index(current_data['reporting_department'])
                      if current_data['reporting_department'] in reporting_dept_options
                      else 0, # <-- O '0' agora aponta para o placeholder
                help="Selecione o setor responsÃ¡vel por notificar o evento",
                key="create_reporting_dept_state_refactored"
            )
            current_data['reporting_department_complement'] = st.text_input(
                "Complemento do Setor Notificante", value=current_data['reporting_department_complement'],
                placeholder="InformaÃ§Ãµes adicionais do setor (opcional)",
                help="Detalhes adicionais sobre o setor notificante (Ex: Equipe A, Sala 101)",
                key="create_reporting_dept_comp_state_refactored")
            event_shift_options = [UI_TEXTS.selectbox_default_event_shift] + FORM_DATA.turnos
            current_data['event_shift'] = st.selectbox(
                "Turno do Evento*", options=event_shift_options,
                index=event_shift_options.index(current_data[
                                                                                    'event_shift']) if current_data[
                                                                                                           'event_shift'] in event_shift_options else 0,
                help="Turno em que o evento ocorreu", key="create_event_shift_state_refactored")
            current_data['description'] = st.text_area(
                "DescriÃ§Ã£o Detalhada do Evento*", value=current_data['description'],
                placeholder="Descreva:\nâ€¢ O que aconteceu?\nâ€¢ Quando aconteceu?\nâ€¢ Onde aconteceu?\nâ€¢ Quem esteve envolvido?\nâ€¢ Como aconteceu?\nâ€¢ ConsequÃªncias observadas",
                height=150,
                key="create_description_state_refactored")

            st.markdown("<span class='required-field'>* Campos obrigatÃ³rios</span>", unsafe_allow_html=True)
            st.markdown("---")


    elif st.session_state.form_step == 2: # CORREÃ‡ÃƒO: Acessa diretamente
        with st.container():
            st.markdown("""
            <div class="form-section">
                <h3>âš¡ Etapa 2: AÃ§Ãµes Imediatas</h3>
                <p>Indique se alguma aÃ§Ã£o foi tomada imediatamente apÃ³s o evento.</p>
            </div>
            """, unsafe_allow_html=True)
            immediate_actions_taken_options = [UI_TEXTS.selectbox_default_immediate_actions_taken, "Sim", "NÃ£o"]
            current_data['immediate_actions_taken'] = st.selectbox(
                "Foram tomadas aÃ§Ãµes imediatas?*", options=immediate_actions_taken_options,
                index=immediate_actions_taken_options.index(current_data[
                                                                'immediate_actions_taken']) if current_data[
                                                                                                   'immediate_actions_taken'] in immediate_actions_taken_options else 0,
                key="immediate_actions_taken_state_refactored", help="Indique se alguma aÃ§Ã£o foi tomada...")
            st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
            if current_data['immediate_actions_taken'] == 'Sim':
                st.markdown("""
                <div class="conditional-field">
                    <h4>ðŸ“ Detalhes das AÃ§Ãµes Imediatas</h4>
                    <p>Descreva detalhadamente as aÃ§Ãµes que foram tomadas.</p>
                </div>
                """, unsafe_allow_html=True)
                current_data['immediate_action_description'] = st.text_area(
                    "DescriÃ§Ã£o detalhada da aÃ§Ã£o realizada*", value=current_data['immediate_action_description'],
                    placeholder="Descreva:\nâ€¢ Quais aÃ§Ãµes foram tomadas?\nâ€¢ Por quem foram executadas?\nâ€¢ Quando foram realizadas?\nâ€¢ Resultados obtidos",
                    height=150,
                    key="create_immediate_action_desc_state_refactored",
                    help="ForneÃ§a um relato completo...")
                st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
            else:
                current_data['immediate_action_description'] = ""
        st.markdown("---")

    elif st.session_state.form_step == 3: # CORREÃ‡ÃƒO: Acessa diretamente
        with st.container():
            st.markdown("""
            <div class="form-section">
                <h3>ðŸ¥ Etapa 3: Impacto no Paciente</h3>
                <p>Indique se o evento teve qualquer tipo de envolvimento ou impacto em um paciente.</p>
            </div>
            """, unsafe_allow_html=True)
            patient_involved_options = [UI_TEXTS.selectbox_default_patient_involved, "Sim", "NÃ£o"]
            current_data['patient_involved'] = st.selectbox(
                "O evento atingiu algum paciente?*", options=patient_involved_options,
                index=patient_involved_options.index(current_data[
                                                         'patient_involved']) if current_data[
                                                                                     'patient_involved'] in patient_involved_options else 0,
                key="patient_involved_state_refactored",
                help="Indique se o evento teve qualquer tipo de envolvimento...")
            st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
            if current_data['patient_involved'] == 'Sim':
                st.markdown("""
                <div class="conditional-field">
                    <h4>ðŸ¥ InformaÃ§Ãµes do Paciente Afetado</h4>
                    <p>Preencha as informaÃ§Ãµes do paciente envolvido no evento.</p>
                </div>
                """, unsafe_allow_html=True)
                col5, col6 = st.columns(2)
                with col5:
                    current_data['patient_id'] = st.text_input(
                        "NÃºmero do Atendimento/ProntuÃ¡rio*", value=current_data['patient_id'],
                        placeholder="Ex: 123456789", key="create_patient_id_refactored",
                        help="NÃºmero de identificaÃ§Ã£o do paciente...")
                with col6:
                    patient_outcome_obito_options = [UI_TEXTS.selectbox_default_patient_outcome_obito, "Sim",
                                                     "NÃ£o"]
                    current_data['patient_outcome_obito'] = st.selectbox(
                        "O paciente evoluiu com Ã³bito?*", options=patient_outcome_obito_options,
                        index=patient_outcome_obito_options.index(current_data['patient_outcome_obito']) if
                        current_data['patient_outcome_obito'] in patient_outcome_obito_options else 0,
                        key="create_patient_outcome_obito_refactored",
                        help="Indique se o evento resultou diretamente no Ã³bito do paciente.")
                st.markdown("<span class='required-field'>* Campos obrigatÃ³rios</span>", unsafe_allow_html=True)
            else:
                current_data['patient_id'] = ""
                current_data['patient_outcome_obito'] = UI_TEXTS.selectbox_default_patient_outcome_obito
        st.markdown("---")

    elif st.session_state.form_step == 4: # CORREÃ‡ÃƒO: Acessa diretamente
        with st.container():
            st.markdown("""
            <div class="form-section">
                <h3>ðŸ“„ Etapa 4: InformaÃ§Ãµes Adicionais e EvidÃªncias</h3>
                <p>Complete as informaÃ§Ãµes adicionais e anexe documentos, se aplicÃ¡vel.</p>
            </div>
            """, unsafe_allow_html=True)
            col7, col8 = st.columns(2)
            with col7:
                # NOVO: Lista de opÃ§Ãµes para Setor Notificado, incluindo o placeholder
                notified_dept_options = [UI_TEXTS.selectbox_default_department_select] + FORM_DATA.SETORES
                current_data['notified_department'] = st.selectbox(
                    "Setor Notificado*",
                    options=notified_dept_options, # <-- ALTERADO
                    index=notified_dept_options.index(current_data['notified_department'])
                          if current_data['notified_department'] in notified_dept_options
                          else 0, # <-- O '0' agora aponta para o placeholder
                    help="Selecione o setor que serÃ¡ notificado sobre o evento",
                    key="create_notified_dept_refactored"
                )
            with col8:
                current_data['notified_department_complement'] = st.text_input(
                    "Complemento do Setor Notificado", value=current_data['notified_department_complement'],
                    placeholder="InformaÃ§Ãµes adicionais (opcional)",
                    help="Detalhes adicionais sobre o setor notificante (Ex: Equipe A, Sala 101)",
                    key="create_notified_dept_comp_refactored"
                )
            st.markdown("<span class='required-field'>* Campo obrigatÃ³rio (Setor Notificado)</span>",
                        unsafe_allow_html=True)
            current_data['additional_notes'] = st.text_area(
                "ObservaÃ§Ãµes Adicionais", value=current_data['additional_notes'],
                placeholder="Qualquer outra informaÃ§Ã£o que considere relevante.",
                height=100, key="additional_notes_refactored",
                help="Adicione qualquer outra informaÃ§Ã£o relevante...")
            st.markdown("---")

            st.markdown("###    Documentos e EvidÃªncias")
            uploaded_files_list_widget = st.file_uploader(
                "Anexar arquivos relacionados ao evento (Opcional)", type=None, accept_multiple_files=True,
                help="Anexe fotos, documentos...", key="create_attachments_refactored"
            )

            current_data['attachments'] = st.session_state.get('create_attachments_refactored', [])
            if current_data.get('attachments'):
                st.info(
                    f"   {len(current_data['attachments'])} arquivo(s) selecionado(s): {', '.join([f.name for f in current_data['attachments']])}")

            st.markdown("---")

    # DEFINIÃ‡ÃƒO DAS COLUNAS: ESTA LINHA PRECISA ESTAR AQUI!
    col_prev, col_cancel_btn, col_next_submit = st.columns(3)

    with col_prev:
        if st.session_state.form_step > 1 and st.session_state.form_step < 5: # CORREÃ‡ÃƒO: Acessa diretamente
            if st.button("â—€ï¸ Voltar", key=f"step_back_btn_refactored_{st.session_state.form_step}", # CORREÃ‡ÃƒO: Acessa diretamente
                         use_container_width=True):
                st.session_state.form_step -= 1
                st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render

    with col_cancel_btn:
        if st.session_state.form_step < 5: # CORREÃ‡ÃƒO: Acessa diretamente
            if st.button("ðŸš« Cancelar NotificaÃ§Ã£o", key="step_cancel_btn_refactored",
                         use_container_width=True):
                _reset_form_state()
                st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render
    with col_next_submit:
        if st.session_state.form_step < 4: # CORREÃ‡ÃƒO: Acessa diretamente
            if st.button(f"âž¡ï¸ PrÃ³ximo",
                         key=f"step_next_btn_refactored_{st.session_state.form_step}", use_container_width=True): # CORREÃ‡ÃƒO: Acessa diretamente
                validation_errors = []
                if st.session_state.form_step == 1: # CORREÃ‡ÃƒO: Acessa diretamente
                    if not current_data['title'].strip(): validation_errors.append(
                        'Etapa 1: TÃ­tulo da NotificaÃ§Ã£o Ã© obrigatÃ³rio.')
                    if not current_data['description'].strip(): validation_errors.append(
                        'Etapa 1: DescriÃ§Ã£o Detalhada Ã© obrigatÃ³ria.')
                    if not current_data['location'].strip(): validation_errors.append(
                        'Etapa 1: Local do Evento Ã© obrigatÃ³rio.')
                    if current_data['occurrence_date'] is None or not isinstance(current_data['occurrence_date'],
                                                                                 dt_date_class): validation_errors.append(
                        'Etapa 1: Data da OcorrÃªncia Ã© obrigatÃ³ria.')
                    if current_data['reporting_department'] == UI_TEXTS.selectbox_default_department_select: # <-- ALTERADO
                        validation_errors.append('Etapa 1: Setor Notificante Ã© obrigatÃ³rio.')
                    if current_data['event_shift'] == UI_TEXTS.selectbox_default_event_shift: validation_errors.append(
                        'Etapa 1: Turno do Evento Ã© obrigatÃ³rio.')
                elif st.session_state.form_step == 2: # CORREÃ‡ÃƒO: Acessa diretamente
                    if current_data[
                        'immediate_actions_taken'] == UI_TEXTS.selectbox_default_immediate_actions_taken: validation_errors.append(
                        'Etapa 2: Ã‰ obrigatÃ³rio indicar se foram tomadas AÃ§Ãµes Imediatas (Sim/NÃ£o).')
                    if current_data['immediate_actions_taken'] == "Sim" and not current_data[
                        'immediate_action_description'].strip(): validation_errors.append(
                        "Etapa 2: DescriÃ§Ã£o das aÃ§Ãµes imediatas Ã© obrigatÃ³ria quando hÃ¡ aÃ§Ãµes imediatas.")
                elif st.session_state.form_step == 3: # CORREÃ‡ÃƒO: Acessa diretamente
                    if current_data[
                        'patient_involved'] == UI_TEXTS.selectbox_default_patient_involved: validation_errors.append(
                        'Etapa 3: Ã‰ obrigatÃ³rio indicar se o Paciente foi Afetado (Sim/NÃ£o).')
                    if current_data['patient_involved'] == "Sim":
                        if not current_data['patient_id'].strip(): validation_errors.append(
                            "Etapa 3: NÃºmero do Atendimento/ProntuÃ¡rio Ã© obrigatÃ³rio quando paciente Ã© afetado.")
                        if current_data[
                            'patient_outcome_obito'] == UI_TEXTS.selectbox_default_patient_outcome_obito: validation_errors.append(
                            "Etapa 3: EvoluÃ§Ã£o para Ã³bito Ã© obrigatÃ³rio quando paciente Ã© afetado.")
                if validation_errors:
                    st.error("âš ï¸ **Por favor, corrija os seguintes erros:**")
                    for error in validation_errors:
                        st.warning(error)
                else:
                    st.session_state.form_step += 1
                    st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render

        elif st.session_state.form_step == 4: # CORREÃ‡ÃƒO: Acessa diretamente
            with st.form("submit_form_refactored_step4", clear_on_submit=False):
                submit_button = st.form_submit_button("ðŸ“¤ Enviar NotificaÃ§Ã£o", use_container_width=True)

                if submit_button:
                    st.subheader("Validando e Enviando NotificaÃ§Ã£o...")
                    validation_errors = []
                    # Re-valida TODOS os campos obrigatÃ³rios de TODAS as etapas (1-4) antes do envio final
                    if not current_data['title'].strip(): validation_errors.append(
                        'Etapa 1: TÃ­tulo da NotificaÃ§Ã£o Ã© obrigatÃ³rio.')
                    if not current_data['description'].strip(): validation_errors.append(
                        'Etapa 1: DescriÃ§Ã£o Detalhada Ã© obrigatÃ³ria.')
                    if not current_data['location'].strip(): validation_errors.append(
                        'Etapa 1: Local do Evento Ã© obrigatÃ³rio.')
                    if current_data['occurrence_date'] is None or not isinstance(current_data['occurrence_date'],
                                                                                 dt_date_class): validation_errors.append(
                        'Etapa 1: Data da OcorrÃªncia Ã© obrigatÃ³ria.')
                    if not current_data['reporting_department'] or \
                       current_data['reporting_department'] == UI_TEXTS.selectbox_default_department_select: # <-- ALTERADO
                        validation_errors.append("Etapa 1: Setor Notificante Ã© obrigatÃ³rio.")
                    if current_data['event_shift'] == UI_TEXTS.selectbox_default_event_shift: validation_errors.append(
                        'Etapa 1: Turno do Evento Ã© obrigatÃ³rio.')
                    if current_data[
                        'immediate_actions_taken'] == UI_TEXTS.selectbox_default_immediate_actions_taken: validation_errors.append(
                        'Etapa 2: Ã‰ obrigatÃ³rio indicar se foram tomadas AÃ§Ãµes Imediatas (Sim/NÃ£o).')
                    if current_data['immediate_actions_taken'] == "Sim" and not current_data[
                        'immediate_action_description'].strip(): validation_errors.append(
                        "Etapa 2: DescriÃ§Ã£o das aÃ§Ãµes imediatas Ã© obrigatÃ³ria quando hÃ¡ aÃ§Ãµes imediatas.")
                    if current_data[
                        'patient_involved'] == UI_TEXTS.selectbox_default_patient_involved: validation_errors.append(
                        'Etapa 3: Ã‰ obrigatÃ³rio indicar se o Paciente foi Afetado (Sim/NÃ£o).')
                    if current_data['patient_involved'] == "Sim":
                        if not current_data['patient_id'].strip(): validation_errors.append(
                            "Etapa 3: NÃºmero do Atendimento/ProntuÃ¡rio Ã© obrigatÃ³rio quando paciente Ã© afetado.")
                        if current_data[
                            'patient_outcome_obito'] == UI_TEXTS.selectbox_default_patient_outcome_obito: validation_errors.append(
                            "Etapa 3: EvoluÃ§Ã£o para Ã³bito Ã© obrigatÃ³rio quando paciente Ã© afetado.")
                    if not current_data['notified_department'] or \
                       current_data['notified_department'] == UI_TEXTS.selectbox_default_department_select: # <-- ALTERADO
                        validation_errors.append("Etapa 4: Setor Notificado Ã© obrigatÃ³rio.")
                    if validation_errors:
                        st.error("âš ï¸ **Por favor, corrija os seguintes erros antes de enviar:**")
                        for error in validation_errors:
                            st.warning(error)
                    else:
                        notification_data_to_save = current_data.copy()
                        uploaded_files_list = notification_data_to_save.pop('attachments', [])
                        try:
                            # Chama a funÃ§Ã£o de criaÃ§Ã£o que agora interage com o DB
                            notification = create_notification(notification_data_to_save, uploaded_files_list)
                            st.success(f"âœ… **NotificaÃ§Ã£o #{notification['id']} criada com sucesso!**")
                            st.info(
                                "ðŸ“‹ Sua notificaÃ§Ã£o foi enviada para classificaÃ§Ã£o e serÃ¡ processada pela equipe responsÃ¡vel.")
                            with st.expander("   Resumo da NotificaÃ§Ã£o Enviada", expanded=False):
                                occurrence_datetime_summary = format_date_time_summary(
                                    notification_data_to_save.get('occurrence_date'),
                                    notification_data_to_save.get('occurrence_time')
                                )
                                st.write(f"**ID:** #{notification['id']}")
                                st.write(f"**TÃ­tulo:** {notification_data_to_save.get('title', UI_TEXTS.text_na)}")
                                st.write(f"**Local:** {notification_data_to_save.get('location', UI_TEXTS.text_na)}")
                                st.write(f"**Data/Hora do Evento:** {occurrence_datetime_summary}")
                                st.write(
                                    f"**Turno:** {notification_data_to_save.get('event_shift', UI_TEXTS.text_na)}")
                                reporting_department = notification_data_to_save.get('reporting_department',
                                                                                    UI_TEXTS.text_na)
                                reporting_complement = notification_data_to_save.get('reporting_department_complement')
                                reporting_dept_display = f"{reporting_department}{f' ({reporting_complement})' if reporting_complement else ''}"
                                st.write(f"**Setor Notificante:** {reporting_dept_display}")
                                notified_department = notification_data_to_save.get('notified_department',
                                                                                    UI_TEXTS.text_na)
                                notified_complement = notification_data_to_save.get('notified_department_complement')
                                notified_dept_display = f"{notified_department}{f' ({notified_complement})' if notified_complement else ''}"
                                st.write(f"**Setor Notificado:** {notified_dept_display}")
                                st.write(
                                    f"**DescriÃ§Ã£o:** {notification_data_to_save.get('description', UI_TEXTS.text_na)[:200]}..." if len(
                                        notification_data_to_save.get('description',
                                                                      '')) > 200 else notification_data_to_save.get(
                                        'description', UI_TEXTS.text_na))
                                st.write(
                                    f"**AÃ§Ãµes Imediatas Tomadas:** {notification_data_to_save.get('immediate_actions_taken', UI_TEXTS.text_na)}")
                                if notification_data_to_save.get('immediate_actions_taken') == 'Sim':
                                    st.write(
                                        f"**DescriÃ§Ã£o AÃ§Ãµes Imediatas:** {notification_data_to_save.get('immediate_action_description', UI_TEXTS.text_na)[:200]}..." if len(
                                            notification_data_to_save.get('immediate_action_description',
                                                                          '')) > 200 else notification_data_to_save.get(
                                            'immediate_action_description', UI_TEXTS.text_na))
                                st.write(
                                    f"**Paciente Envolvido:** {notification_data_to_save.get('patient_involved', UI_TEXTS.text_na)}")
                                if notification_data_to_save.get('patient_involved') == 'Sim':
                                    st.write(
                                        f"**NÂ° Atendimento:** {notification_data_to_save.get('patient_id', UI_TEXTS.text_na)}")
                                    outcome_text = 'Sim' if notification_data_to_save.get(
                                        'patient_outcome_obito') is True else 'NÃ£o' if notification_data_to_save.get(
                                        'patient_outcome_obito') is False else 'NÃ£o informado'
                                    st.write(f"**Evoluiu para Ã³bito:** {outcome_text}")
                                if notification_data_to_save.get('additional_notes'):
                                    st.write(
                                        f"**ObservaÃ§Ãµes Adicionais:** {notification_data_to_save.get('additional_notes', UI_TEXTS.text_na)[:200]}..." if len(
                                            notification_data_to_save.get('additional_notes',
                                                                          '')) > 200 else notification_data_to_save.get(
                                        'additional_notes', UI_TEXTS.text_na))
                                if uploaded_files_list:
                                    st.write(
                                        f"**Anexos:** {len(uploaded_files_list)} arquivo(s) - {', '.join([f.name for f in uploaded_files_list])}")
                                else:
                                    st.write("**Anexos:** Nenhum arquivo anexado.")
                            st.session_state.form_step = 5  # Muda para a etapa 5
                            st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render

                        except Exception as e:
                            st.error(f"âŒ Ocorreu um erro ao finalizar a notificaÃ§Ã£o: {e}")
                            st.warning("Por favor, revise as informaÃ§Ãµes e tente enviar novamente.")

@st_fragment
def show_classification():
    """
    Renders the page for classifiers to perform initial classification of new notifications
    and review the execution of completed actions by responsible parties.
    """
    if not check_permission('classificador'):
        st.error("âŒ Acesso negado! VocÃª nÃ£o tem permissÃ£o para classificar notificaÃ§Ãµes.")
        return
    st.markdown("<h1 class='main-header'>ðŸ” ClassificaÃ§Ã£o e RevisÃ£o de NotificaÃ§Ãµes</h1>", unsafe_allow_html=True)
    st.info(
        "ðŸ“‹ Nesta Ã¡rea, vocÃª pode realizar a classificaÃ§Ã£o inicial de novas notificaÃ§Ãµes e revisar a execuÃ§Ã£o das aÃ§Ãµes concluÃ­das pelos responsÃ¡veis.")

    all_notifications = load_notifications()  # Carrega do DB
    pending_initial_classification = [n for n in all_notifications if n.get('status') == "pendente_classificacao"]
    pending_execution_review = [n for n in all_notifications if n.get('status') == "revisao_classificador_execucao"]
    closed_statuses = ['aprovada', 'rejeitada', 'reprovada', 'concluida']
    closed_notifications = [n for n in all_notifications if n.get('status') in closed_statuses]

    if not pending_initial_classification and not pending_execution_review and not closed_notifications:
        st.info(
            "âœ… NÃ£o hÃ¡ notificaÃ§Ãµes pendentes de classificaÃ§Ã£o inicial, revisÃ£o de execuÃ§Ã£o ou encerradas no momento.")
        return

    tab_initial_classif, tab_review_exec, tab_closed_notifs = st.tabs(
        [f"â³ Pendentes ClassificaÃ§Ã£o Inicial ({len(pending_initial_classification)})",
         f"ðŸ› ï¸ RevisÃ£o de ExecuÃ§Ã£o ConcluÃ­da ({len(pending_execution_review)})",
         f"âœ… NotificaÃ§Ãµes Encerradas ({len(closed_notifications)})"]
    )

    with tab_initial_classif:
        st.markdown("### NotificaÃ§Ãµes Aguardando ClassificaÃ§Ã£o Inicial")
        if not pending_initial_classification:
            st.info("âœ… NÃ£o hÃ¡ notificaÃ§Ãµes aguardando classificaÃ§Ã£o inicial no momento.")
        else:
            st.markdown("#### ðŸ“‹ Selecionar NotificaÃ§Ã£o para ClassificaÃ§Ã£o Inicial")
            notification_options_initial = [UI_TEXTS.selectbox_default_notification_select] + [
                f"#{n['id']} | Criada em: {n.get('created_at', UI_TEXTS.text_na)[:10]} | {n.get('title', 'Sem tÃ­tulo')[:60]}..."
                for n in pending_initial_classification
            ]

            pending_initial_ids_str = ",".join(str(n['id']) for n in pending_initial_classification)
            selectbox_key_initial = f"classify_selectbox_initial_{pending_initial_ids_str}"
            if selectbox_key_initial not in st.session_state or st.session_state[
                selectbox_key_initial] not in notification_options_initial:
                previous_selection = st.session_state.get(selectbox_key_initial, notification_options_initial[0])
                if previous_selection in notification_options_initial:
                    st.session_state[selectbox_key_initial] = previous_selection
                else:
                    st.session_state[selectbox_key_initial] = notification_options_initial[0]

            selected_option_initial = st.selectbox(
                "Escolha uma notificaÃ§Ã£o para analisar e classificar inicial:",
                options=notification_options_initial,
                index=notification_options_initial.index(st.session_state[selectbox_key_initial]),
                key=selectbox_key_initial,
                help="Selecione na lista a notificaÃ§Ã£o pendente que vocÃª deseja classificar.")
            notification_id_initial = None
            notification_initial = None

            if selected_option_initial != UI_TEXTS.selectbox_default_notification_select:
                try:
                    parts = selected_option_initial.split('#')
                    if len(parts) > 1:
                        id_part = parts[1].split(' |')[0]
                        notification_id_initial = int(id_part)
                        notification_initial = next(
                            (n for n in all_notifications if n.get('id') == notification_id_initial), None)
                except (IndexError, ValueError):
                    st.error("Erro ao processar a seleÃ§Ã£o da notificaÃ§Ã£o para classificaÃ§Ã£o inicial.")
                    notification_initial = None
            if notification_id_initial and (
                    st.session_state.get('current_initial_classification_id') != notification_id_initial):
                # Se uma nova notificaÃ§Ã£o foi selecionada, inicializa seu estado de classificaÃ§Ã£o
                st.session_state.initial_classification_state = st.session_state.get('initial_classification_state', {})
                st.session_state.initial_classification_state[notification_id_initial] = {
                    'step': 1,
                    'data': {
                        'procede': UI_TEXTS.selectbox_default_procede_classification,
                        'motivo_rejeicao': '',
                        'classificacao_nnc': UI_TEXTS.selectbox_default_classificacao_nnc,
                        'nivel_dano': UI_TEXTS.selectbox_default_nivel_dano,
                        'prioridade_selecionada': UI_TEXTS.selectbox_default_prioridade_resolucao,
                        'never_event_selecionado': UI_TEXTS.text_na,
                        'evento_sentinela_sim_nao': UI_TEXTS.selectbox_default_evento_sentinela,
                        'tipo_evento_principal_selecionado': UI_TEXTS.selectbox_default_tipo_principal,
                        'tipo_evento_sub_selecionado': [],
                        'tipo_evento_sub_texto_livre': '',
                        'classificacao_oms_selecionada': [],
                        'observacoes_classificacao': '',
                        'requires_approval': UI_TEXTS.selectbox_default_requires_approval,
                        'approver_selecionado': UI_TEXTS.selectbox_default_approver,
                        'executores_selecionados': [],
                        # Adicionados para ediÃ§Ã£o do setor notificado
                        'temp_notified_department': notification_initial.get(
                            'notified_department') if notification_initial else None,
                        'temp_notified_department_complement': notification_initial.get(
                            'notified_department_complement') if notification_initial else None,
                    }
                }
                st.session_state.current_initial_classification_id = notification_id_initial
                if 'current_review_classification_id' in st.session_state: st.session_state.pop(
                    'current_review_classification_id')
                st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render

            current_classification_state = st.session_state.initial_classification_state.get(notification_id_initial,
                                                                                             {})
            current_step = current_classification_state.get('step', 1)
            current_data = current_classification_state.get('data', {})

            if notification_initial:
                st.markdown(
                    f"### NotificaÃ§Ã£o Selecionada para ClassificaÃ§Ã£o Inicial: #{notification_initial.get('id', UI_TEXTS.text_na)}")
                with st.expander(
                        f"ðŸ“„ Detalhes Reportados Originalmente (NotificaÃ§Ã£o #{notification_initial.get('id', UI_TEXTS.text_na)})",
                        expanded=False):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**ðŸ“ InformaÃ§Ãµes BÃ¡sicas**")
                        st.write(f"**TÃ­tulo:** {notification_initial.get('title', UI_TEXTS.text_na)}")
                        st.write(f"**Local:** {notification_initial.get('location', UI_TEXTS.text_na)}")
                        occurrence_datetime_summary = format_date_time_summary(
                            notification_initial.get('occurrence_date'), notification_initial.get('occurrence_time'))
                        st.write(f"**Data/Hora:** {occurrence_datetime_summary}")
                        st.write(f"**Turno:** {notification_initial.get('event_shift', UI_TEXTS.text_na)}")
                        reporting_department = notification_initial.get('reporting_department', UI_TEXTS.text_na)
                        reporting_complement = notification_initial.get('reporting_department_complement')
                        reporting_dept_display = f"{reporting_department}{f' ({reporting_complement})' if reporting_complement else ''}"
                        st.write(f"**Setor Notificante:** {reporting_dept_display}")
                        # --- Setor Notificado na visualizaÃ§Ã£o original ---
                        notified_department = notification_initial.get('notified_department', UI_TEXTS.text_na)
                        notified_complement = notification_initial.get('notified_department_complement')
                        notified_dept_display = f"{notified_department}{f' ({notified_complement})' if notified_complement else ''}"
                        st.write(f"**Setor Notificado:** {notified_dept_display}")
                        # --- FIM DA ADIÃ‡ÃƒO: Setor Notificado na visualizaÃ§Ã£o original ---
                        if notification_initial.get('immediate_actions_taken') and notification_initial.get(
                                'immediate_action_description'):
                            st.info(
                                f"**AÃ§Ãµes Imediatas Reportadas:** {notification_initial.get('immediate_action_description', UI_TEXTS.text_na)[:300]}...")
                    with col2:
                        st.markdown("**ðŸ“Š Detalhes de Paciente e ObservaÃ§Ãµes Iniciais**")
                        st.write(
                            f"**Paciente Envolvido:** {'Sim' if notification_initial.get('patient_involved') else 'NÃ£o'}")
                        if notification_initial.get('patient_involved'):
                            st.write(f"**ProntuÃ¡rio:** {notification_initial.get('patient_id', UI_TEXTS.text_na)}")
                            outcome = notification_initial.get('patient_outcome_obito')
                            if outcome is True:
                                st.write("**Evoluiu para Ã“bito:** Sim")
                            elif outcome is False:
                                st.write("**Evoluiu para Ã“bito:** NÃ£o")
                            else:
                                st.write("**Evoluiu para Ã“bito:** NÃ£o informado")
                    st.markdown("**ðŸ“ DescriÃ§Ã£o Detalhada do Evento**")
                    st.info(notification_initial.get('description', UI_TEXTS.text_na))
                    if notification_initial.get('additional_notes'):
                        st.markdown("**â„¹ï¸ ObservaÃ§Ãµes Adicionais do Notificante**")
                        st.info(notification_initial.get('additional_notes', UI_TEXTS.text_na))
                    if notification_initial.get('attachments'):
                        st.markdown("**ðŸ“Ž Anexos**")
                        for attach_info in notification_initial['attachments']:
                            unique_name_to_use = None
                            original_name_to_use = None
                            if isinstance(attach_info,
                                          dict) and 'unique_name' in attach_info and 'original_name' in attach_info:
                                unique_name_to_use = attach_info['unique_name']
                                original_name_to_use = attach_info['original_name']
                            elif isinstance(attach_info, str):
                                unique_name_to_use = attach_info
                                original_name_to_use = attach_info
                            if unique_name_to_use:
                                file_content = get_attachment_data(unique_name_to_use)
                                if file_content:
                                    st.download_button(
                                        label=f"Baixar {original_name_to_use}",
                                        data=file_content,
                                        file_name=original_name_to_use,
                                        mime="application/octet-stream",
                                        key=f"download_init_{notification_initial['id']}_{unique_name_to_use}"
                                    )
                                else:
                                    st.write(f"Anexo: {original_name_to_use} (arquivo nÃ£o encontrado ou corrompido)")
                st.markdown("---")
# --- Renderiza a etapa atual do formulÃ¡rio de classificaÃ§Ã£o inicial ---
                if current_step == 1:
                    with st.container():
                        st.markdown("""
                             <div class="form-section">
                                 <h3>ðŸ“‹ Etapa 1: Aceite da NotificaÃ§Ã£o</h3>
                                 <p>Analise os detalhes da notificaÃ§Ã£o e decida se ela procede para classificaÃ§Ã£o.</p>
                             </div>
                             """, unsafe_allow_html=True)
                        procede_options = [UI_TEXTS.selectbox_default_procede_classification, "Sim",
                                           "NÃ£o"]
                        current_data['procede'] = st.selectbox(
                            "ApÃ³s anÃ¡lise, a notificaÃ§Ã£o procede e deve ser classificada?*",
                            options=procede_options,
                            index=procede_options.index(
                                current_data.get('procede', UI_TEXTS.selectbox_default_procede_classification)),
                            key=f"procede_select_{notification_id_initial}_step1_initial_refactored",
                            help="Selecione 'Sim' para classificar a notificaÃ§Ã£o ou 'NÃ£o' para rejeitÃ¡-la.")
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                        if current_data['procede'] == "NÃ£o":
                            current_data['motivo_rejeicao'] = st.text_area(
                                "Justificativa para RejeiÃ§Ã£o*", value=current_data.get('motivo_rejeicao', ''),
                                key=f"motivo_rejeicao_{notification_id_initial}_step1_initial_refactored",
                                help="Explique detalhadamente por que esta notificaÃ§Ã£o serÃ¡ rejeitada.").strip()
                        else:
                            current_data['motivo_rejeicao'] = ""
                elif current_step == 2:
                    with st.container():
                        st.markdown("""
                             <div class="form-section">
                                 <h3>ðŸ“˜ Etapa 2: ClassificaÃ§Ã£o NNC, Dano e Prioridade</h3>
                                 <p>ForneÃ§a a classificaÃ§Ã£o de NÃ£o Conformidade, o nÃ­vel de dano (se aplicÃ¡vel) e defina a prioridade.</p>
                             </div>
                             """, unsafe_allow_html=True)
                        classificacao_nnc_options = [
                                                        UI_TEXTS.selectbox_default_classificacao_nnc] + FORM_DATA.classificacao_nnc
                        current_data['classificacao_nnc'] = st.selectbox(
                            "ClassificaÃ§Ã£o:*", options=classificacao_nnc_options,
                            index=classificacao_nnc_options.index(
                                current_data.get('classificacao_nnc', UI_TEXTS.selectbox_default_classificacao_nnc)),
                            key=f"class_nnc_{notification_id_initial}_step2_initial_refactored",
                            help="Selecione o tipo de classificaÃ§Ã£o principal do evento.")
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                        if current_data['classificacao_nnc'] == "Evento com dano":
                            nivel_dano_options = [UI_TEXTS.selectbox_default_nivel_dano] + FORM_DATA.niveis_dano
                            current_data['nivel_dano'] = st.selectbox(
                                "NÃ­vel de Dano ao Paciente:*", options=nivel_dano_options,
                                index=nivel_dano_options.index(
                                    current_data.get('nivel_dano', UI_TEXTS.selectbox_default_nivel_dano)),
                                key=f"dano_nivel_{notification_id_initial}_step2_initial_refactored",
                                help="Selecione o nÃ­vel de dano ao paciente.")
                            st.markdown(
                                "<span class='required-field'>* Campo obrigatÃ³rio quando Evento com Dano</span>",
                                unsafe_allow_html=True)
                        else:
                            current_data['nivel_dano'] = UI_TEXTS.selectbox_default_nivel_dano
                        prioridades_options = [UI_TEXTS.selectbox_default_prioridade_resolucao] + FORM_DATA.prioridades
                        current_data['prioridade_selecionada'] = st.selectbox(
                            "Prioridade de ResoluÃ§Ã£o:*", options=prioridades_options,
                            index=prioridades_options.index(
                                current_data.get('prioridade_selecionada',
                                                 UI_TEXTS.selectbox_default_prioridade_resolucao)),
                            key=f"prioridade_select_{notification_id_initial}_step2_initial_refactored",
                            help="Defina a prioridade para investigaÃ§Ã£o e resoluÃ§Ã£o do evento.")
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                elif current_step == 3:
                    with st.container():
                        st.markdown("""
                             <div class="form-section">
                                 <h3>âš ï¸ Etapa 3: Eventos Especiais (Never Event / Sentinela)</h3>
                                 <p>Identifique se o evento se enquadra em categorias de alta relevÃ¢ncia para a seguranÃ§a do paciente.</p>
                             </div>
                             """, unsafe_allow_html=True)
                        never_event_options = [UI_TEXTS.selectbox_default_never_event] + FORM_DATA.never_events + [
                            UI_TEXTS.text_na]

                        selected_never_event_for_index = current_data.get('never_event_selecionado', UI_TEXTS.text_na)

                        try:
                            default_index = never_event_options.index(selected_never_event_for_index)
                        except ValueError:
                            default_index = 0
                        current_data['never_event_selecionado'] = st.selectbox(
                            "Never Event:*",
                            options=never_event_options,
                            index=default_index,
                            format_func=lambda
                                x: UI_TEXTS.selectbox_never_event_na_text if x == UI_TEXTS.text_na else x,
                            key=f"never_event_select_{notification_id_initial}_step3_initial_refactored",
                            help="Selecione se o evento se enquadra como um Never Event. Utilize 'Selecione uma opÃ§Ã£o...' caso nÃ£o se aplique ou nÃ£o haja um Never Event identificado."
                        )
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                        evento_sentinela_options = [UI_TEXTS.selectbox_default_evento_sentinela, "Sim", "NÃ£o"]
                        current_data['evento_sentinela_sim_nao'] = st.selectbox(
                            "Evento Sentinela?*", options=evento_sentinela_options,
                            index=evento_sentinela_options.index(
                                current_data.get('evento_sentinela_sim_nao',
                                                 UI_TEXTS.selectbox_default_evento_sentinela)),
                            key=f"is_sentinel_event_select_{notification_id_initial}_step3_initial_refactored",
                            help="Indique se o evento Ã© considerado um Evento Sentinela.")
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                elif current_step == 4:
                    with st.container():
                        st.markdown("""
                             <div class="form-section">
                                 <h3> categorizaÃ§Ã£o do evento (Tipo Principal e EspecificaÃ§Ã£o)</h3>
                                 <p>Classifique o evento pelo tipo principal e especifique, se necessÃ¡rio.</p>
                             </div>
                             """, unsafe_allow_html=True)
                        tipo_evento_principal_options = [UI_TEXTS.selectbox_default_tipo_principal] + list(
                            FORM_DATA.tipos_evento_principal.keys())
                        current_data['tipo_evento_principal_selecionado'] = st.selectbox(
                            "Tipo Principal:*", options=tipo_evento_principal_options,
                            index=tipo_evento_principal_options.index(
                                current_data.get('tipo_evento_principal_selecionado',
                                                 UI_TEXTS.selectbox_default_tipo_principal)),
                            key="event_type_main_refactored", help="ClassificaÃ§Ã£o do tipo principal de evento.")
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                        sub_options = FORM_DATA.tipos_evento_principal.get(
                            current_data.get('tipo_evento_principal_selecionado'), [])
                        if current_data.get('tipo_evento_principal_selecionado') in ["ClÃ­nico", "NÃ£o-clÃ­nico",
                                                                                     "Ocupacional"] and sub_options:
                            multiselect_display_options = [UI_TEXTS.multiselect_instruction_placeholder] + sub_options
                            default_sub_selection = current_data.get('tipo_evento_sub_selecionado', [])

                            if not default_sub_selection or not any(
                                    item in sub_options for item in default_sub_selection):
                                default_sub_selection = [UI_TEXTS.multiselect_instruction_placeholder]
                            selected_sub_raw = st.multiselect(
                                f"{UI_TEXTS.multiselect_event_spec_label_prefix}{current_data['tipo_evento_principal_selecionado']}{UI_TEXTS.multiselect_event_spec_label_suffix}",
                                options=multiselect_display_options,
                                default=default_sub_selection,
                                key=f"event_type_sub_select_{notification_id_initial}_step4_initial_refactored",
                                help="Selecione as sub-categorias aplicÃ¡veis.")
                            current_data['tipo_evento_sub_selecionado'] = [
                                opt for opt in selected_sub_raw if
                                opt != UI_TEXTS.multiselect_instruction_placeholder
                            ]
                            current_data['tipo_evento_sub_texto_livre'] = ""
                        elif current_data.get('tipo_evento_principal_selecionado') in ["Queixa tÃ©cnica", "Outros"]:
                            label_text = f"Especifique o tipo '{current_data['tipo_evento_principal_selecionado']}'*" if \
                                current_data[
                                    'tipo_evento_principal_selecionado'] == "Outros" else f"Especifique o tipo '{current_data['tipo_evento_principal_selecionado']}':"
                            current_data['tipo_evento_sub_texto_livre'] = st.text_input(
                                label_text, value=current_data.get('tipo_evento_sub_texto_livre', ''),
                                key=f"event_type_sub_text_{notification_id_initial}_step4_initial_refactored",
                                help="Descreva o tipo de evento 'Queixa TÃ©cnica' ou 'Outro'.")
                            current_data['tipo_evento_sub_selecionado'] = []
                            if current_data.get('tipo_evento_principal_selecionado') == "Outros":
                                st.markdown(
                                    "<span class='required-field'>* Campo obrigatÃ³rio quando Tipo Principal Ã© 'Outros'</span>",
                                    unsafe_allow_html=True)
                        else:
                            current_data['tipo_evento_sub_selecionado'] = []
                            current_data['tipo_evento_sub_texto_livre'] = ""
                elif current_step == 5:
                    with st.container():
                        st.markdown("""
                            <div class="form-section">
                                <h3>ðŸŒ Etapa 5: ClassificaÃ§Ã£o OMS</h3>
                                <p>Classifique o evento de acordo com a ClassificaÃ§Ã£o Internacional de SeguranÃ§a do Paciente da OMS.</p>
                            </div>
                            """, unsafe_allow_html=True)
                        oms_options = FORM_DATA.classificacao_oms
                        multiselect_display_options = [UI_TEXTS.multiselect_instruction_placeholder] + oms_options
                        default_oms_selection = current_data.get('classificacao_oms_selecionada', [])
                        if not default_oms_selection or not any(item in oms_options for item in default_oms_selection):
                            default_oms_selection = [UI_TEXTS.multiselect_instruction_placeholder]
                        selected_oms_raw = st.multiselect(
                            UI_TEXTS.multiselect_classification_oms_label,
                            options=multiselect_display_options,
                            default=default_oms_selection,
                            key=f"oms_classif_{notification_id_initial}_step5_initial_refactored",
                            help="Selecione um ou mais tipos de incidente da ClassificaÃ§Ã£o da OMS.")

                        current_data['classificacao_oms_selecionada'] = [
                            opt for opt in selected_oms_raw if opt != UI_TEXTS.multiselect_instruction_placeholder
                        ]
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio (selecionar ao menos um)</span>",
                                    unsafe_allow_html=True)
                elif current_step == 6:
                    with st.container():
                        st.markdown("""
                             <div class="form-section">
                                 <h3>ðŸ“„ Etapa 6: ObservaÃ§Ãµes da ClassificaÃ§Ã£o</h3>
                                 <p>Adicione quaisquer observaÃ§Ãµes relevantes sobre a classificaÃ§Ã£o do evento.</p>
                             </div>
                             """, unsafe_allow_html=True)
                        current_data['observacoes_classificacao'] = st.text_area(
                            "ObservaÃ§Ãµes da ClassificaÃ§Ã£o (opcional)",
                            value=current_data.get('observacoes_classificacao', ''),
                            key=f"obs_classif_{notification_id_initial}_step6_initial_refactored",
                            help="Adicione observaÃ§Ãµes relevantes sobre a classificaÃ§Ã£o do evento.").strip()
                elif current_step == 7:
                    with st.container():
                        st.markdown("""
                             <div class="form-section">
                                 <h3>ðŸ‘¥ Etapa 7: AtribuiÃ§Ã£o e Fluxo PÃ³s-ClassificaÃ§Ã£o</h3>
                                 <p>Defina quem serÃ¡ responsÃ¡vel pela execuÃ§Ã£o das aÃ§Ãµes e se aprovaÃ§Ã£o superior Ã© necessÃ¡ria.</p>
                             </div>
                             """, unsafe_allow_html=True)
# --- Resumo da NotificaÃ§Ã£o Original ---
                        st.markdown("#### ðŸ“ Resumo da NotificaÃ§Ã£o Original")
                        st.write(f"**TÃ­tulo Original:** {notification_initial.get('title', UI_TEXTS.text_na)}")
                        st.write(f"**Local Original:** {notification_initial.get('location', UI_TEXTS.text_na)}")
                        original_occurrence_datetime_summary = format_date_time_summary(
                            notification_initial.get('occurrence_date'), notification_initial.get('occurrence_time'))
                        st.write(f"**Data/Hora Original:** {original_occurrence_datetime_summary}")
                        reporting_department = notification_initial.get('reporting_department', UI_TEXTS.text_na)
                        reporting_complement = notification_initial.get('reporting_department_complement')
                        reporting_dept_display = f"{reporting_department}{f' ({reporting_complement})' if reporting_complement else ''}"
                        st.write(f"**Setor Notificante Original:** {reporting_dept_display}")
                        # Setor Notificado Original para o resumo
                        original_notified_department = notification_initial.get('notified_department', UI_TEXTS.text_na)
                        original_notified_complement = notification_initial.get('notified_department_complement')
                        original_notified_dept_display = f"{original_notified_department}{f' ({original_notified_complement})' if original_notified_complement else ''}"
                        st.write(f"**Setor Notificado Original:** {original_notified_dept_display}")
                        patient_involved_display = 'Sim' if notification_initial.get('patient_involved') else 'NÃ£o'
                        if notification_initial.get('patient_involved'):
                            patient_id_display = notification_initial.get('patient_id', UI_TEXTS.text_na)
                            patient_outcome_obito_display = "Sim" if notification_initial.get(
                                'patient_outcome_obito') is True else "NÃ£o" if notification_initial.get(
                                'patient_outcome_obito') is False else UI_TEXTS.text_na
                            st.write(
                                f"**Paciente Envolvido Original:** {patient_involved_display} (ID: {patient_id_display}, Ã“bito: {patient_outcome_obito_display})")
                        else:
                            st.write(f"**Paciente Envolvido Original:** {patient_involved_display}")
                        st.write(
                            f"**DescriÃ§Ã£o Original:** {notification_initial.get('description', UI_TEXTS.text_na)[:200]}...")  # Trunca para um resumo conciso
                        st.markdown("---")
# --- EdiÃ§Ã£o do Setor Notificado ---
                        st.markdown("#### ðŸ¢ Ajustar Setor Notificado")
                        st.info(
                            "VocÃª pode ajustar o setor que receberÃ¡ esta notificaÃ§Ã£o e seu complemento, se necessÃ¡rio.")

                        # Inicializa os valores selecionados com os dados da notificaÃ§Ã£o original ou o que jÃ¡ estÃ¡ no estado temporÃ¡rio
                        # Prioriza o que estÃ¡ no current_data do state, depois a notificaÃ§Ã£o original
                        initial_notified_department = current_data.get('temp_notified_department')
                        if initial_notified_department is None and notification_initial:
                            initial_notified_department = notification_initial.get('notified_department')
                        initial_notified_department_complement = current_data.get('temp_notified_department_complement')
                        if initial_notified_department_complement is None and notification_initial:
                            initial_notified_department_complement = notification_initial.get(
                                'notified_department_complement')
                        notified_dept_options_classif = [UI_TEXTS.selectbox_default_department_select] + FORM_DATA.SETORES
                        current_data['temp_notified_department'] = st.selectbox(
                            "Setor Notificado:*",
                            options=notified_dept_options_classif,
                            index=notified_dept_options_classif.index(
                                initial_notified_department) if initial_notified_department in notified_dept_options_classif else 0,
                            key=f"classifier_notified_dept_{notification_id_initial}_refactored",
                            help="Selecione o setor que serÃ¡ o responsÃ¡vel principal por receber e gerenciar esta notificaÃ§Ã£o."
                        )
                        current_data['temp_notified_department_complement'] = st.text_input(
                            "Complemento do Setor Notificado",
                            value=initial_notified_department_complement,
                            placeholder="InformaÃ§Ãµes adicionais (opcional)",
                            key=f"classifier_notified_dept_comp_{notification_id_initial}_refactored",
                            help="Detalhes adicionais sobre o setor notificado (Ex: Equipe A, Sala 101).")
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                        st.markdown("---")

                        executors = get_users_by_role('executor')
                        executor_options = {
                            f"{e.get('name', UI_TEXTS.text_na)} ({e.get('username', UI_TEXTS.text_na)})": e['id']
                            for e in executors
                        }

                        executor_display_options = [UI_TEXTS.multiselect_instruction_placeholder] + list(
                            executor_options.keys())
                        default_executor_selection = [
                            name for name, uid in executor_options.items() if
                            uid in current_data.get('executores_selecionados', [])
                        ]
                        if not default_executor_selection or not any(
                                item in list(executor_options.keys()) for item in default_executor_selection):
                            default_executor_selection = [UI_TEXTS.multiselect_instruction_placeholder]
                        selected_executor_names_raw = st.multiselect(
                            UI_TEXTS.multiselect_assign_executors_label,
                            options=executor_display_options,
                            default=default_executor_selection,
                            key=f"executors_multiselect_{notification_id_initial}_step7_initial_refactored",
                            help="Selecione os usuÃ¡rios que serÃ£o responsÃ¡veis pela execuÃ§Ã£o das aÃ§Ãµes corretivas/preventivas.")
                        current_data['executores_selecionados'] = [
                            opt for opt in selected_executor_names_raw if
                            opt != UI_TEXTS.multiselect_instruction_placeholder
                        ]

                        st.markdown(
                            "<span class='required-field'>* Campo obrigatÃ³rio (selecionar ao menos um executor)</span>",
                            unsafe_allow_html=True)
                        st.markdown("---")
                        requires_approval_options = [UI_TEXTS.selectbox_default_requires_approval, "Sim", "NÃ£o"]
                        current_data['requires_approval'] = st.selectbox(
                            "Requer AprovaÃ§Ã£o Superior apÃ³s ExecuÃ§Ã£o?*",
                            options=requires_approval_options,
                            index=requires_approval_options.index(
                                current_data.get('requires_approval', UI_TEXTS.selectbox_default_requires_approval)),
                            key=f"requires_approval_select_{notification_id_initial}_step7_initial_refactored",
                            help="Indique se esta notificaÃ§Ã£o, apÃ³s o execuÃ§Ã£o das aÃ§Ãµes, precisa ser aprovada por um usuÃ¡rio com a funÃ§Ã£o 'aprovador'.")
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                        approvers = get_users_by_role('aprovador')
                        approver_options = {
                            f"{a.get('name', UI_TEXTS.text_na)} ({a.get('username', UI_TEXTS.text_na)})": a['id']
                            for a in approvers
                        }
                        approver_select_options = [UI_TEXTS.selectbox_default_approver] + list(
                            approver_options.keys())
                        if current_data['requires_approval'] == 'Sim':
                            selected_approver_name = st.selectbox(
                                "Selecionar Aprovador ResponsÃ¡vel:*",
                                options=approver_select_options,
                                index=approver_select_options.index(next(
                                    (name for name, uid in approver_options.items() if
                                     uid == current_data.get('approver_selecionado')),
                                    UI_TEXTS.selectbox_default_approver)),
                                key=f"approver_select_{notification_id_initial}_step7_initial_refactored",
                                help="Selecione o usuÃ¡rio 'aprovador' que serÃ¡ responsÃ¡vel pela aprovaÃ§Ã£o final.")
                            current_data['approver_selecionado'] = approver_options.get(selected_approver_name)
                            st.markdown(
                                "<span class='required-field'>* Campo obrigatÃ³rio quando requer aprovaÃ§Ã£o</span>",
                                unsafe_allow_html=True)
                        else:
                            current_data['approver_selecionado'] = UI_TEXTS.selectbox_default_approver
                        st.markdown("---")
                        st.markdown("#### âœ… Resumo da ClassificaÃ§Ã£o Final")
                        st.write(f"**ClassificaÃ§Ã£o NNC:** {current_data.get('classificacao_nnc', UI_TEXTS.text_na)}")
                        if current_data.get('classificacao_nnc') == "Evento com dano" and current_data.get(
                                'nivel_dano') != UI_TEXTS.selectbox_default_nivel_dano:
                            st.write(f"**NÃ­vel de Dano:** {current_data.get('nivel_dano', UI_TEXTS.text_na)}")
                        st.write(f"**Prioridade:** {current_data.get('prioridade_selecionada', UI_TEXTS.text_na)}")
                        st.write(f"**Never Event:** {current_data.get('never_event_selecionado', UI_TEXTS.text_na)}")
                        st.write(
                            f"**Evento Sentinela:** {'Sim' if current_data.get('evento_sentinela_sim_nao') == 'Sim' else 'NÃ£o'}")
                        st.write(
                            f"**Tipo Principal:** {current_data.get('tipo_evento_principal_selecionado', UI_TEXTS.text_na)}")
                        sub_type_display = ''
                        if current_data.get('tipo_evento_sub_selecionado'):
                            sub_type_display = ', '.join(current_data.get('tipo_evento_sub_selecionado'))
                        elif current_data.get('tipo_evento_sub_texto_livre'):
                            sub_type_display = current_data.get('tipo_evento_sub_texto_livre')
                        if sub_type_display:
                            st.write(f"**EspecificaÃ§Ã£o:** {sub_type_display}")
                        st.write(
                            f"**ClassificaÃ§Ã£o OMS:** {', '.join(current_data.get('classificacao_oms_selecionada', [UI_TEXTS.text_na]))}")
                        st.write(
                            f"**ObservaÃ§Ãµes:** {current_data.get('observacoes_classificacao') or UI_TEXTS.text_na}")
                        st.write(
                            f"**Setor Notificado (Ajustado):** {current_data.get('temp_notified_department', UI_TEXTS.text_na)}")
                        if current_data.get('temp_notified_department_complement'):
                            st.write(
                                f"**Complemento Setor Notificado (Ajustado):** {current_data.get('temp_notified_department_complement')}")
                        displayed_executors = [name for name, uid in executor_options.items() if
                                               uid in current_data.get('executores_selecionados', [])]
                        st.write(f"**Executores AtribuÃ­dos:** {', '.join(displayed_executors) or 'Nenhum'}")
                        requires_approval_display = current_data.get('requires_approval', UI_TEXTS.text_na)
                        st.write(f"**Requer AprovaÃ§Ã£o:** {requires_approval_display}")
                        if requires_approval_display == 'Sim':
                            approver_name_display = next((name for name, uid in approver_options.items() if
                                                          uid == current_data.get('approver_selecionado')),
                                                         UI_TEXTS.selectbox_default_approver)
                            st.write(f"**Aprovador AtribuÃ­do:** {approver_name_display}")
                col_prev_initial, col_cancel_initial, col_next_submit_initial = st.columns(3)

                with col_prev_initial:
                    if current_step > 1 and current_step <= 7 and current_data.get('procede') != 'NÃ£o':
                        if st.button("â—€ï¸ Voltar", use_container_width=True,
                                     key=f"back_btn_{notification_id_initial}_step{current_step}_initial_refactored"):
                            current_classification_state['step'] -= 1
                            st.session_state.initial_classification_state[
                                notification_id_initial] = current_classification_state
                            st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render
                with col_cancel_initial:
                    if current_step <= 7:
                        if st.button("ðŸš« Cancelar ClassificaÃ§Ã£o", use_container_width=True,
                                     key=f"cancel_btn_{notification_id_initial}_step{current_step}_initial_refactored"):
                            st.session_state.initial_classification_state.pop(notification_id_initial,
                                                                              None)
                            st.session_state.pop('current_initial_classification_id', None)
                            st.info(f"A classificaÃ§Ã£o inicial da notificaÃ§Ã£o #{notification_id_initial} foi cancelada.")
                            st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render
                with col_next_submit_initial:
                    if current_step < 7 and current_data.get('procede') != 'NÃ£o':
                        if st.button(f"âž¡ï¸ PrÃ³ximo",
                                     key=f"next_btn_{notification_id_initial}_step{current_step}_initial_refactored",
                                     use_container_width=True):
                            validation_errors = []
                            if current_step == 1:
                                if current_data.get('procede') != 'Sim': validation_errors.append(
                                    'Etapa 1: Para avanÃ§ar, a notificaÃ§Ã£o deve proceder (selecione \'Sim\').')
                            elif current_step == 2:
                                if current_data.get(
                                        'classificacao_nnc') == UI_TEXTS.selectbox_default_classificacao_nnc: validation_errors.append(
                                    "Etapa 2: ClassificaÃ§Ã£o NNC Ã© obrigatÃ³ria.")
                                if current_data.get('classificacao_nnc') == "Evento com dano" and current_data.get(
                                        'nivel_dano') == UI_TEXTS.selectbox_default_nivel_dano: validation_errors.append(
                                    "Etapa 2: NÃ­vel de dano Ã© obrigatÃ³rio para evento com dano.")
                                if current_data.get(
                                        'prioridade_selecionada') == UI_TEXTS.selectbox_default_prioridade_resolucao: validation_errors.append(
                                    "Etapa 2: Prioridade de ResoluÃ§Ã£o Ã© obrigatÃ³ria.")
                            elif current_step == 3:
                                if current_data.get(
                                        'never_event_selecionado') == UI_TEXTS.selectbox_default_never_event: validation_errors.append(
                                    "Etapa 3: Never Event Ã© obrigatÃ³rio (selecione 'N/A' se nÃ£o se aplica).")
                                if current_data.get(
                                        'evento_sentinela_sim_nao') == UI_TEXTS.selectbox_default_evento_sentinela: validation_errors.append(
                                    "Etapa 3: Evento Sentinela Ã© obrigatÃ³rio (Sim/NÃ£o).")
                            ### INÃCIO DA CORREÃ‡ÃƒO: LÃ³gica de validaÃ§Ã£o da Etapa 4
                            elif current_step == 4:
                                if current_data.get(
                                        'tipo_evento_principal_selecionado') == UI_TEXTS.selectbox_default_tipo_principal:
                                    validation_errors.append("Etapa 4: Tipo Principal de Evento Ã© obrigatÃ³rio.")
                                # MODIFICAÃ‡ÃƒO AQUI: Verifica se o tipo principal requer sub-especificaÃ§Ã£o e se nenhuma foi selecionada.
                                elif current_data.get('tipo_evento_principal_selecionado') in ["ClÃ­nico", "NÃ£o-clÃ­nico",
                                                                                               "Ocupacional"] and not current_data.get(
                                    'tipo_evento_sub_selecionado'):
                                    validation_errors.append(
                                        "Etapa 4: Ã‰ obrigatÃ³rio selecionar ao menos uma EspecificaÃ§Ã£o do Evento.")
                                elif current_data.get(
                                        'tipo_evento_principal_selecionado') == 'Outros' and not current_data.get(
                                    'tipo_evento_sub_texto_livre'):
                                    validation_errors.append("Etapa 4: A especificaÃ§Ã£o do tipo 'Outros' Ã© obrigatÃ³ria.")
                                elif current_data.get(
                                        'tipo_evento_principal_selecionado') == 'Queixa tÃ©cnica' and not current_data.get(
                                    'tipo_evento_sub_texto_livre'):
                                    validation_errors.append(
                                        "Etapa 4: A especificaÃ§Ã£o do tipo 'Queixa tÃ©cnica' Ã© obrigatÃ³ria.")
                            ### FIM DA CORREÃ‡ÃƒO
                            elif current_step == 5:
                                if not current_data.get('classificacao_oms_selecionada'): validation_errors.append(
                                    "Etapa 5: ClassificaÃ§Ã£o OMS Ã© obrigatÃ³ria (selecione ao menos um item).")

                            if validation_errors:
                                st.error("âš ï¸ **Por favor, corrija os seguintes erros para avanÃ§ar:**")
                                for error in validation_errors: st.warning(error)
                            else:
                                current_classification_state['step'] += 1
                                st.session_state.initial_classification_state[
                                    notification_id_initial] = current_classification_state
                                st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render
                    is_final_classification_submit_step_initial = current_step == 7 and current_data.get(
                        'procede') == 'Sim'
                    is_rejection_submit_step_initial = current_step == 1 and current_data.get('procede') == 'NÃ£o'
                    if is_final_classification_submit_step_initial or is_rejection_submit_step_initial:
                        with st.form(
                                key=f"final_classification_submit_form_{notification_id_initial}_step{current_step}_initial_refactored",
                                clear_on_submit=False):
                            submit_button_label = "âŒ Rejeitar NotificaÃ§Ã£o" if is_rejection_submit_step_initial else "ðŸ“¤ Enviar ClassificaÃ§Ã£o Final"
                            submit_final_action = st.form_submit_button(
                                submit_button_label, use_container_width=True
                            )
                            if submit_final_action:
                                st.subheader("Processando sua decisÃ£o final...")
                                validation_errors = []
                                if is_rejection_submit_step_initial:
                                    if not current_data.get('motivo_rejeicao'): validation_errors.append(
                                        "Justificativa de rejeiÃ§Ã£o Ã© obrigatÃ³ria.")
                                elif is_final_classification_submit_step_initial:
                                    if current_data.get('procede') != 'Sim': validation_errors.append(
                                        'Erro interno: Status "procede" invÃ¡lido para finalizaÃ§Ã£o.')
                                    if current_data.get(
                                            'classificacao_nnc') == UI_TEXTS.selectbox_default_classificacao_nnc: validation_errors.append(
                                        "Etapa 2: ClassificaÃ§Ã£o NNC Ã© obrigatÃ³ria.")
                                    if current_data.get('classificacao_nnc') == "Evento com dano" and current_data.get(
                                            'nivel_dano') == UI_TEXTS.selectbox_default_nivel_dano: validation_errors.append(
                                        "Etapa 2: NÃ­vel de dano Ã© obrigatÃ³rio para evento com dano.")
                                    if current_data.get(
                                            'prioridade_selecionada') == UI_TEXTS.selectbox_default_prioridade_resolucao: validation_errors.append(
                                        "Etapa 2: Prioridade de ResoluÃ§Ã£o Ã© obrigatÃ³ria.")
                                    if current_data.get(
                                            'never_event_selecionado') == UI_TEXTS.selectbox_default_never_event: validation_errors.append(
                                        "Etapa 3: Never Event Ã© obrigatÃ³rio (selecione 'N/A' se nÃ£o se aplica).")
                                    if current_data.get(
                                            'evento_sentinela_sim_nao') == UI_TEXTS.selectbox_default_evento_sentinela: validation_errors.append(
                                        "Etapa 3: Evento Sentinela Ã© obrigatÃ³rio (Sim/NÃ£o).")
                                    if current_data.get(
                                            'tipo_evento_principal_selecionado') == UI_TEXTS.selectbox_default_tipo_principal: validation_errors.append(
                                        "Etapa 4: Tipo Principal de Evento Ã© obrigatÃ³rio.")
                                    if current_data.get('tipo_evento_principal_selecionado') in ["ClÃ­nico",
                                                                                                 "NÃ£o-clÃ­nico",
                                                                                                 "Ocupacional"] and not current_data.get(
                                        'tipo_evento_sub_selecionado'):
                                        validation_errors.append(
                                            "Etapa 4: Ã‰ obrigatÃ³rio selecionar ao menos uma EspecificaÃ§Ã£o do Evento.")
                                    elif current_data.get(
                                            'tipo_evento_principal_selecionado') == 'Outros' and not current_data.get(
                                        'tipo_evento_sub_texto_livre'):
                                        validation_errors.append(
                                            "Etapa 4: A especificaÃ§Ã£o do tipo 'Outros' Ã© obrigatÃ³ria.")
                                    elif current_data.get(
                                            'tipo_evento_principal_selecionado') == 'Queixa tÃ©cnica' and not current_data.get(
                                        'tipo_evento_sub_texto_livre'):
                                        validation_errors.append(
                                            "Etapa 4: A especificaÃ§Ã£o do tipo 'Queixa tÃ©cnica' Ã© obrigatÃ³ria.")
                                    if not current_data.get('classificacao_oms_selecionada'): validation_errors.append(
                                        "Etapa 5: ClassificaÃ§Ã£o OMS Ã© obrigatÃ³ria (selecione ao menos um item).")
                                    # ValidaÃ§Ã£o do Setor Notificado Ajustado
                                    if not current_data.get('temp_notified_department') or \
                                       current_data['temp_notified_department'] == UI_TEXTS.selectbox_default_department_select:
                                        validation_errors.append("Etapa 7: Ã‰ obrigatÃ³rio definir o Setor Notificado.")
                                    if not current_data.get('executores_selecionados'): validation_errors.append(
                                        "Etapa 7: Ã‰ obrigatÃ³rio atribuir ao menos um Executor ResponsÃ¡vel.")
                                    if current_data.get(
                                            'requires_approval') == UI_TEXTS.selectbox_default_requires_approval: validation_errors.append(
                                        "Etapa 7: Ã‰ obrigatÃ³rio indicar se requer AprovaÃ§Ã£o Superior (Sim/NÃ£o).")
                                    if current_data.get('requires_approval') == "Sim" and (
                                            current_data.get('approver_selecionado') is None or current_data.get(
                                        'approver_selecionado') == UI_TEXTS.selectbox_default_approver): validation_errors.append(
                                        "Etapa 7: Ã‰ obrigatÃ³rio selecionar o Aprovador ResponsÃ¡vel quando requer aprovaÃ§Ã£o.")
                                if validation_errors:
                                    st.error("âš ï¸ **Por favor, corrija os seguintes erros antes de enviar:**")
                                    for error in validation_errors: st.warning(error)
                                    st.stop()
                                else:
                                    user_name = st.session_state.user.get('name', 'UsuÃ¡rio')
                                    user_username = st.session_state.user.get('username', UI_TEXTS.text_na)
                                    if is_rejection_submit_step_initial:
                                        updates = {
                                            "status": "rejeitada",
                                            "classification": None,  # Limpa classificaÃ§Ã£o
                                            "executors": [],  # Limpa executores
                                            "approver": None,  # Limpa aprovador
                                            "rejection_classification": {  # Adiciona motivo da rejeiÃ§Ã£o
                                                "reason": current_data.get('motivo_rejeicao'),
                                                "classified_by": user_username,
                                                "timestamp": datetime.now().isoformat()
                                            }
                                        }
                                        update_notification(notification_id_initial, updates)  # Atualiza no DB
                                        add_history_entry(  # Adiciona histÃ³rico no DB
                                            notification_id_initial, "NotificaÃ§Ã£o rejeitada na ClassificaÃ§Ã£o Inicial",
                                            user_name,
                                            f"Motivo da rejeiÃ§Ã£o: {current_data.get('motivo_rejeicao', '')[:200]}..." if len(
                                                current_data.get('motivo_rejeicao',
                                                                 '')) > 200 else f"Motivo da rejeiÃ§Ã£o: {current_data.get('motivo_rejeicao', '')}"
                                        )
                                        st.success(f"âœ… NotificaÃ§Ã£o #{notification_id_initial} rejeitada com sucesso!")
                                        st.info(
                                            "VocÃª serÃ¡ redirecionado para a lista atualizada de notificaÃ§Ãµes pendentes.")
                                    elif is_final_classification_submit_step_initial:
                                        # Recalculate executor_options here to ensure it's fresh
                                        all_executors_list_for_map = get_users_by_role('executor')
                                        executor_name_to_id_map = {
                                            f"{e.get('name', UI_TEXTS.text_na)} ({e.get('username', UI_TEXTS.text_na)})":
                                                e['id']
                                            for e in all_executors_list_for_map
                                        }
                                        # Convert the selected executor names (strings) back to their IDs (integers)
                                        selected_executor_ids_for_db = [
                                            executor_name_to_id_map[name]
                                            for name in current_data.get('executores_selecionados', [])
                                            if name in executor_name_to_id_map  # Ensure the name exists in the map
                                        ]
                                        # Calculate deadline
                                        deadline_days = 0
                                        nnc_type = current_data.get('classificacao_nnc')
                                        if nnc_type == "Evento com dano":
                                            dano_level = current_data.get('nivel_dano')
                                            deadline_days = DEADLINE_DAYS_MAPPING[nnc_type].get(dano_level, 0)
                                        else:
                                            deadline_days = DEADLINE_DAYS_MAPPING.get(nnc_type, 0)
                                        # Ensure deadline_days is an integer, default to 0 if not found/invalid
                                        if not isinstance(deadline_days, int):
                                            deadline_days = 0  # Default to 0 days if mapping fails
                                        deadline_date_calculated = (
                                                dt_date_class.today() + timedelta(days=deadline_days)).isoformat()
                                        classification_data_to_save = {
                                            "nnc": nnc_type,
                                            "nivel_dano": current_data.get(
                                                'nivel_dano') if nnc_type == "Evento com dano" else None,
                                            "prioridade": current_data.get('prioridade_selecionada'),
                                            "never_event": current_data.get('never_event_selecionado'),
                                            "is_sentinel_event": True if current_data.get(
                                                'evento_sentinela_sim_nao') == "Sim" else False if current_data.get(
                                                'evento_sentinela_sim_nao') == "NÃ£o" else None,
                                            "oms": current_data.get('classificacao_oms_selecionada'),
                                            "event_type_main": current_data.get('tipo_evento_principal_selecionado'),
                                            "event_type_sub": current_data.get(
                                                'tipo_evento_sub_selecionado') if current_data.get(
                                                'tipo_evento_principal_selecionado') in ["ClÃ­nico", "NÃ£o-clÃ­nico",
                                                                                         "Ocupacional"] else (
                                                [current_data.get('tipo_evento_sub_texto_livre')] if current_data.get(
                                                    'tipo_evento_sub_texto_livre') else []),
                                            "notes": current_data.get('observacoes_classificacao'),
                                            "classificador": user_username,
                                            "classification_timestamp": datetime.now().isoformat(),
                                            "requires_approval": True if current_data.get(
                                                'requires_approval') == "Sim" else False if current_data.get(
                                                'requires_approval') == "NÃ£o" else None,
                                            "deadline_date": deadline_date_calculated  # ADDED DEADLINE DATE
                                        }
                                        updates = {
                                            "status": "classificada",
                                            "classification": classification_data_to_save,
                                            "rejection_classification": None,
                                            "executors": selected_executor_ids_for_db,
                                            "approver": current_data.get('approver_selecionado') if current_data.get(
                                                'requires_approval') == 'Sim' else None,
                                            # Adicionado para salvar o setor notificado ajustado
                                            "notified_department": current_data.get('temp_notified_department'),
                                            "notified_department_complement": current_data.get(
                                                'temp_notified_department_complement')
                                        }
                                        update_notification(notification_id_initial, updates)  # Atualiza no DB
                                        details_hist = f"ClassificaÃ§Ã£o NNC: {classification_data_to_save['nnc']}, Prioridade: {classification_data_to_save.get('prioridade', UI_TEXTS.text_na)}"
                                        if classification_data_to_save["nnc"] == "Evento com dano" and \
                                                classification_data_to_save["nivel_dano"]:
                                            details_hist += f", NÃ­vel Dano: {classification_data_to_save['nivel_dano']}"
                                        details_hist += f", Never Event: {classification_data_to_save.get('never_event', UI_TEXTS.text_na)}"
                                        details_hist += f", Evento Sentinela: {'Sim' if classification_data_to_save.get('is_sentinel_event') else 'NÃ£o'}"
                                        details_hist += f", Tipo Principal: {classification_data_to_save.get('event_type_main', UI_TEXTS.text_na)}"
                                        sub_detail = classification_data_to_save.get('event_type_sub')
                                        if sub_detail:
                                            if isinstance(sub_detail, list):
                                                details_hist += f" ({', '.join(sub_detail)[:100]}...)" if len(
                                                    ', '.join(sub_detail)) > 100 else f" ({', '.join(sub_detail)})"
                                            else:
                                                details_hist += f" ({str(sub_detail)[:100]}...)" if len(
                                                    str(sub_detail)) > 100 else f" ({str(sub_detail)})"
                                        details_hist += f", Requer AprovaÃ§Ã£o: {'Sim' if classification_data_to_save.get('requires_approval') else 'NÃ£o'}"
                                        # Melhoria para exibir nomes de executores no histÃ³rico
                                        all_users = load_users()  # Recarrega todos os usuÃ¡rios para ter os nomes
                                        exec_ids_in_updates = updates.get('executors', [])
                                        exec_names_for_history = [
                                            u.get('name', UI_TEXTS.text_na) for u in all_users
                                            if u.get('id') in exec_ids_in_updates
                                        ]
                                        details_hist += f", Executores: {', '.join(exec_names_for_history) or 'Nenhum'}"
                                        if updates.get('approver'):
                                            approvers_list_hist = all_users
                                            approver_name_hist = next(
                                                (a.get('name', UI_TEXTS.text_na) for a in approvers_list_hist if
                                                 a.get('id') == updates.get('approver')), UI_TEXTS.text_na)
                                            details_hist += f", Aprovador: {approver_name_hist}"
                                        # Adiciona o setor notificado (ajustado ou original) ao histÃ³rico
                                        notified_dept_hist = updates.get('notified_department', UI_TEXTS.text_na)
                                        notified_comp_hist = updates.get('notified_department_complement', '')
                                        details_hist += f", Setor Notificado: {notified_dept_hist}"
                                        if notified_comp_hist:
                                            details_hist += f" ({notified_comp_hist})"
                                        add_history_entry(  # Adiciona histÃ³rico no DB
                                            notification_id_initial, "NotificaÃ§Ã£o classificada e atribuÃ­da",
                                            user_name, details_hist
                                        )
                                        st.success(
                                            f"âœ… NotificaÃ§Ã£o #{notification_id_initial} classificada e atribuÃ­da com sucesso!")
                                        st.info(
                                            "A notificaÃ§Ã£o foi movida para a fase de execuÃ§Ã£o e atribuÃ­da aos responsÃ¡veis.")
                                    st.session_state.initial_classification_state.pop(notification_id_initial,
                                                                                      None)
                                    st.session_state.pop('current_initial_classification_id', None)
                                    st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render

            else:
                if pending_initial_classification:
                    st.info(f"ðŸ‘† Selecione uma notificaÃ§Ã£o da lista acima para visualizar e iniciar a classificaÃ§Ã£o.")

    with tab_review_exec:
        st.markdown("### NotificaÃ§Ãµes Aguardando RevisÃ£o da ExecuÃ§Ã£o")

        if not pending_execution_review:
            st.info("âœ… NÃ£o hÃ¡ notificaÃ§Ãµes aguardando revisÃ£o da execuÃ§Ã£o no momento.")
        else:
            st.markdown("#### ðŸ“‹ Selecionar NotificaÃ§Ã£o para RevisÃ£o")
            notification_options_review = [UI_TEXTS.selectbox_default_notification_select] + [
                f"#{n['id']} | Classificada em: {n.get('classification', {}).get('classification_timestamp', UI_TEXTS.text_na)[:10]} | {n.get('title', 'Sem tÃ­tulo')[:60]}..."
                for n in pending_execution_review
            ]

            pending_review_ids_str = ",".join(str(n['id']) for n in pending_execution_review)
            selectbox_key_review = f"classify_selectbox_review_{pending_review_ids_str}"
            if selectbox_key_review not in st.session_state or st.session_state[
                selectbox_key_review] not in notification_options_review:
                previous_selection = st.session_state.get(selectbox_key_review, notification_options_review[0])
                if previous_selection in notification_options_review:
                    st.session_state[selectbox_key_review] = previous_selection
                else:
                    st.session_state[selectbox_key_review] = notification_options_review[0]

            selected_option_review = st.selectbox(
                "Escolha uma notificaÃ§Ã£o para revisar a execuÃ§Ã£o:",
                options=notification_options_review,
                index=notification_options_review.index(st.session_state[selectbox_key_review]),
                key=selectbox_key_review,
                help="Selecione na lista a notificaÃ§Ã£o cuja execuÃ§Ã£o vocÃª deseja revisar.")
            notification_id_review = None
            notification_review = None

            if selected_option_review != UI_TEXTS.selectbox_default_notification_select:
                try:
                    parts = selected_option_review.split('#')
                    if len(parts) > 1:
                        id_part = parts[1].split(' |')[0]
                        notification_id_review = int(id_part)
                        notification_review = next(
                            (n for n in all_notifications if n.get('id') == notification_id_review), None)
                except (IndexError, ValueError):
                    st.error("Erro ao processar a seleÃ§Ã£o da notificaÃ§Ã£o para revisÃ£o.")
                    notification_review = None
            if notification_id_review and (
                    st.session_state.get('current_review_classification_id') != notification_id_review):
                st.session_state.review_classification_state = st.session_state.get('review_classification_state', {})
                st.session_state.review_classification_state[notification_id_review] = {
                    'decision': UI_TEXTS.selectbox_default_decisao_revisao,
                    'rejection_reason_review': '',
                    'notes': '',
                }
                st.session_state.current_review_classification_id = notification_id_review
                if 'current_initial_classification_id' in st.session_state: st.session_state.pop(
                    'current_initial_classification_id')

                st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render

            current_review_data = st.session_state.review_classification_state.get(notification_id_review or 0, {})
            if notification_review is not None:
                st.markdown(
                    f"### NotificaÃ§Ã£o Selecionada para RevisÃ£o de ExecuÃ§Ã£o: #{notification_review.get('id', UI_TEXTS.text_na)}")

                # Obter informaÃ§Ãµes de prazo para o card
                raw_classification_data = notification_review.get('classification')
                if isinstance(raw_classification_data, dict):
                    classif_info = raw_classification_data
                else:
                    # Se nÃ£o for um dicionÃ¡rio (ex: None, ou um tipo inesperado como string/lista de dados antigos),
                    # o tratamos como um dicionÃ¡rio vazio para evitar o AttributeError.
                    classif_info = {}

                deadline_date_str = classif_info.get('deadline_date')

                # Acessa 'timestamp' de 'conclusion' de forma segura
                concluded_timestamp_str = (notification_review.get('conclusion') or {}).get('timestamp')
# Determinar o status do prazo (cor do texto)
                deadline_status = get_deadline_status(deadline_date_str, concluded_timestamp_str)
                # Determinar a classe do cartÃ£o (fundo) com APENAS DOIS STATUS
                card_class = ""
                if deadline_status['class'] == "deadline-ontrack" or deadline_status['class'] == "deadline-duesoon":
                    card_class = "card-prazo-dentro"  # SerÃ¡ verde para "No Prazo" e "Prazo PrÃ³ximo"
                elif deadline_status['class'] == "deadline-overdue":
                    card_class = "card-prazo-fora"  # SerÃ¡ vermelho para "Atrasada"
# Renderizar o card com o estilo apropriado
                st.markdown(f"""
                    <div class="notification-card {card_class}">
                        <h4>#{notification_review.get('id', UI_TEXTS.text_na)} - {notification_review.get('title', UI_TEXTS.text_na)}</h4>
                        <p><strong>Status:</strong> <span class="status-{notification_review.get('status', UI_TEXTS.text_na).replace('_', '-')}">{notification_review.get('status', UI_TEXTS.text_na).replace('_', ' ').title()}</span></p>
                        <p><strong>Prazo:</strong> {deadline_status['text']}</p>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown("#### ðŸ“‹ Detalhes para RevisÃ£o")
                col_rev1, col_rev2 = st.columns(2)
                with col_rev1:
                    st.markdown("**ðŸ“ Evento Reportado Original**")
                    st.write(f"**TÃ­tulo:** {notification_review.get('title', UI_TEXTS.text_na)}")
                    st.write(f"**Local:** {notification_review.get('location', UI_TEXTS.text_na)}")
                    occurrence_datetime_summary = format_date_time_summary(notification_review.get('occurrence_date'),
                                                                           notification_review.get('occurrence_time'))
                    st.write(f"**Data/Hora OcorrÃªncia:** {occurrence_datetime_summary}")
                    st.write(
                        f"**Setor Notificante:** {notification_review.get('reporting_department', UI_TEXTS.text_na)}")
                    if notification_review.get('immediate_actions_taken') and notification_review.get(
                            'immediate_action_description'):
                        st.info(
                            f"**AÃ§Ãµes Imediatas Reportadas:** {notification_review.get('immediate_action_description', UI_TEXTS.text_na)[:300]}...")
                with col_rev2:
                    st.markdown("**â±ï¸ InformaÃ§Ãµes de GestÃ£o e ClassificaÃ§Ã£o**")
                    classif_review = classif_info # JÃ¡ Ã© um dicionÃ¡rio seguro
                    st.write(f"**ClassificaÃ§Ã£o NNC:** {classif_review.get('nnc', UI_TEXTS.text_na)}")
                    if classif_review.get('nivel_dano'): st.write(
                        f"**NÃ­vel de Dano:** {classif_review.get('nivel_dano', UI_TEXTS.text_na)}")
                    st.write(f"**Prioridade:** {classif_review.get('prioridade', UI_TEXTS.text_na)}")
                    st.write(f"**Never Event:** {classif_review.get('never_event', UI_TEXTS.text_na)}")
                    st.write(f"**Evento Sentinela:** {'Sim' if classif_review.get('is_sentinel_event') else 'NÃ£o'}")
                    st.write(f"**Tipo Principal:** {classif_review.get('event_type_main', UI_TEXTS.text_na)}")
                    sub_type_display_review = ''
                    if classif_review.get('event_type_sub'):
                        if isinstance(classif_review['event_type_sub'], list):
                            sub_type_display_review = ', '.join(classif_review['event_type_sub'])
                        else:
                            sub_type_display_review = str(classif_review['event_type_sub'])
                    if sub_type_display_review: st.write(f"**EspecificaÃ§Ã£o:** {sub_type_display_review}")
                    st.write(f"**ClassificaÃ§Ã£o OMS:** {', '.join(classif_review.get('oms', [UI_TEXTS.text_na]))}")
                    st.write(
                        f"**Requer AprovaÃ§Ã£o Superior (Classif. Inicial):** {'Sim' if classif_review.get('requires_approval') else 'NÃ£o'}")
                    st.write(f"**Classificado por:** {classif_review.get('classificador', UI_TEXTS.text_na)}")
# ExibiÃ§Ã£o do Prazo e Status na RevisÃ£o
                    if deadline_date_str:
                        deadline_date_formatted = datetime.fromisoformat(deadline_date_str).strftime('%d/%m/%Y')
                        st.markdown(
                            f"**Prazo de ConclusÃ£o:** {deadline_date_formatted} (<span class='{deadline_status['class']}'>{deadline_status['text']}</span>)",
                            unsafe_allow_html=True)
                    else:
                        st.write(f"**Prazo de ConclusÃ£o:** {UI_TEXTS.deadline_days_nan}")
                st.markdown("---")
                st.markdown("#### âš¡ AÃ§Ãµes Executadas pelos ResponsÃ¡veis")
                if notification.get('actions'):
                    for action in sorted(notification['actions'], key=lambda x: x.get('timestamp', '')):
                        action_type = "ðŸ CONCLUSÃƒO (Executor)" if action.get(
                            'final_action_by_executor') else "ðŸ“ AÃ‡ÃƒO Registrada"
                        action_timestamp = action.get('timestamp', UI_TEXTS.text_na)
                        if action_timestamp != UI_TEXTS.text_na:
                            try:
                                action_timestamp = datetime.fromisoformat(action_timestamp).strftime(
                                    '%d/%m/%Y %H:%M:%S')
                            except ValueError:
                                pass
                        st.markdown(f"""
                                   <strong>{action_type}</strong> - por <strong>{action.get('executor_name', UI_TEXTS.text_na)}</strong> em {action_timestamp}
                                   <br>
                                   <em>{action.get('description', UI_TEXTS.text_na)}</em>
                                   """, unsafe_allow_html=True)
                        # Exibir evidÃªncias se disponÃ­vel e for uma aÃ§Ã£o final
                        if action.get('final_action_by_executor'):
                            evidence_desc = action.get('evidence_description', '').strip()
                            evidence_atts = action.get('evidence_attachments', [])
                            if evidence_desc or evidence_atts:
                                st.markdown(f"""<div class='evidence-section'>""", unsafe_allow_html=True)
                                st.markdown("<h6>EvidÃªncias da ConclusÃ£o:</h6>", unsafe_allow_html=True)
                                if evidence_desc:
                                    st.info(evidence_desc)
                                if evidence_atts:
                                    for attach_info in evidence_atts:
                                        unique_name = attach_info.get('unique_name')
                                        original_name = attach_info.get('original_name')
                                        if unique_name and original_name:
                                            file_content = get_attachment_data(unique_name)
                                            if file_content:
                                                st.download_button(
                                                    label=f"Baixar EvidÃªncia: {original_name}",
                                                    data=file_content,
                                                    file_name=original_name,
                                                    mime="application/octet-stream",
                                                    key=f"download_action_evidence_review_{notification_review['id']}_{unique_name}"
                                                )
                                            else:
                                                st.write(
                                                    f"Anexo: {original_name} (arquivo nÃ£o encontrado ou corrompido)")
                                st.markdown(f"""</div>""", unsafe_allow_html=True)
                        st.markdown("---")
                else:
                    st.warning("âš ï¸ Nenhuma aÃ§Ã£o foi registrada pelos executores para esta notificaÃ§Ã£o ainda.")
                users_review = get_users_by_role('executor')
                # Mapeia nomes de exibiÃ§Ã£o para IDs de usuÃ¡rio para executores
                executor_name_to_id_map_review = {
                    f"{u.get('name', UI_TEXTS.text_na)} ({u.get('username', UI_TEXTS.text_na)})": u['id']
                    for u in users_review
                }
                # Pega os nomes de exibiÃ§Ã£o dos executores atribuÃ­dos
                executor_names_review = [
                    name for name, uid in executor_name_to_id_map_review.items()
                    if uid in notification_review.get('executors', [])
                ]
                st.markdown(
                    f"**ðŸ‘¥ Executores AtribuÃ­dos Originalmente:** {', '.join(executor_names_review) or 'Nenhum'}")
                if notification_review.get('attachments'):
                    st.markdown("#### ðŸ“Ž Anexos")
                    for attach_info in notification_review['attachments']:
                        unique_name_to_use = None
                        original_name_to_use = None
                        if isinstance(attach_info,
                                      dict) and 'unique_name' in attach_info and 'original_name' in attach_info:
                            unique_name_to_use = attach_info['unique_name']
                            original_name_to_use = attach_info['original_name']
                        elif isinstance(attach_info, str):
                            unique_name_to_use = attach_info
                            original_name_to_use = attach_info
                        if unique_name_to_use:
                            file_content = get_attachment_data(unique_name_to_use)
                            if file_content:
                                st.download_button(
                                    label=f"Baixar {original_name_to_use}",
                                    data=file_content,
                                    file_name=original_name_to_use,
                                    mime="application/octet-stream",
                                    key=f"download_review_{notification_review['id']}_{unique_name_to_use}"
                                )
                            else:
                                st.write(f"Anexo: {original_name_to_use} (arquivo nÃ£o encontrado ou corrompido)")
                st.markdown("---")
                with st.form(key=f"review_decision_form_{notification_id_review}_refactored", clear_on_submit=False):
                    st.markdown("### ðŸŽ¯ DecisÃ£o de RevisÃ£o da ExecuÃ§Ã£o")
                    decision_options = [UI_TEXTS.selectbox_default_decisao_revisao, "Aceitar ConclusÃ£o",
                                        "Rejeitar ConclusÃ£o"]
                    current_review_data['decision'] = st.selectbox(
                        "DecisÃ£o:*", options=decision_options,
                        key=f"review_decision_{notification_id_review}_refactored",
                        index=decision_options.index(
                            current_review_data.get('decision', UI_TEXTS.selectbox_default_decisao_revisao)),
                        help="Selecione 'Aceitar ConclusÃ£o' se a execuÃ§Ã£o foi satisfatÃ³ria ou 'Rejeitar ConclusÃ£o' para devolvÃª-la para correÃ§Ã£o/revisÃ£o.")
                    st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                    if current_review_data['decision'] == "Rejeitar ConclusÃ£o":
                        st.markdown("""
                           <div class="conditional-field">
                               <h4>ðŸ“ Detalhes da RejeiÃ§Ã£o</h4>
                               <p>Explique por que a execuÃ§Ã£o foi rejeitada e o que precisa ser feito.</p>
                           </div>
                           """, unsafe_allow_html=True)
                        current_review_data['rejection_reason_review'] = st.text_area(
                            "Justificativa para RejeiÃ§Ã£o da ConclusÃ£o*",
                            value=current_review_data.get('rejection_reason_review', ''),
                            key=f"rejection_reason_review_{notification_id_review}_refactored",
                            help="Descreva os motivos da rejeiÃ§Ã£o e as aÃ§Ãµes corretivas necessÃ¡rias.").strip()
                        st.markdown("<span class='required-field'>* Campo obrigatÃ³rio ao rejeitar</span>",
                                    unsafe_allow_html=True)
                    else:
                        current_review_data['rejection_reason_review'] = ""
                    current_review_data['notes'] = st.text_area(
                        "ObservaÃ§Ãµes da RevisÃ£o (opcional)",
                        value=current_review_data.get('notes', ''),
                        key=f"review_notes_{notification_id_review}_refactored",
                        help="Adicione quaisquer observaÃ§Ãµes relevantes sobre a revisÃ£o da execuÃ§Ã£o.").strip()
                    submit_button_review = st.form_submit_button("âœ”ï¸ Confirmar DecisÃ£o",
                                                                 use_container_width=True)

                    if submit_button_review:
                        review_decision_state = current_review_data.get('decision',
                                                                        UI_TEXTS.selectbox_default_decisao_revisao)
                        validation_errors = []
                        if review_decision_state == UI_TEXTS.selectbox_default_decisao_revisao: validation_errors.append(
                            "Ã‰ obrigatÃ³rio selecionar a decisÃ£o da revisÃ£o (Aceitar/Rejeitar).")
                        if review_decision_state == "Rejeitar ConclusÃ£o" and not current_review_data.get(
                                'rejection_reason_review'): validation_errors.append(
                            "Justificativa para RejeiÃ§Ã£o da ConclusÃ£o Ã© obrigatÃ³ria.")
                        if validation_errors:
                            st.error("âš ï¸ **Por favor, corrija os seguintes erros:**")
                            for error in validation_errors: st.warning(error)
                            st.stop()
                        else:
                            user_name = st.session_state.user.get('name', 'UsuÃ¡rio')
                            user_username = st.session_state.user.get('username', UI_TEXTS.text_na)
                            review_notes = current_review_data.get('notes')
                            review_details_to_save = {
                                'decision': review_decision_state.replace(' ConclusÃ£o', ''),
                                'reviewed_by': user_username,
                                'timestamp': datetime.now().isoformat(),
                                'notes': review_notes or None
                            }
                            if review_decision_state == "Rejeitar ConclusÃ£o":
                                review_details_to_save['rejection_reason'] = current_review_data.get(
                                    'rejection_reason_review')
                            if review_decision_state == "Aceitar ConclusÃ£o":
                                original_classification = notification_review.get('classification', {})
                                requires_approval_after_execution = original_classification.get('requires_approval')
                                if requires_approval_after_execution is True:
                                    new_status = 'aguardando_aprovacao'
                                    updates = {
                                        'status': new_status,
                                        'review_execution': review_details_to_save
                                    }
                                    add_history_entry(
                                        notification_id_review, "RevisÃ£o de ExecuÃ§Ã£o: ConclusÃ£o Aceita",
                                        user_name,
                                        f"ExecuÃ§Ã£o aceita pelo classificador. Encaminhada para aprovaÃ§Ã£o superior." + (
                                            f" Obs: {review_notes}" if review_notes else ""))
                                    st.success(
                                        f"âœ… ExecuÃ§Ã£o da NotificaÃ§Ã£o #{notification_id_review} aceita! Encaminhada para aprovaÃ§Ã£o superior.")
                                else:
                                    new_status = 'aprovada'
                                    updates = {
                                        'status': new_status,
                                        'review_execution': review_details_to_save,
                                        'conclusion': {  # Conclui direto se nÃ£o precisa de aprovaÃ§Ã£o
                                            'concluded_by': user_username,
                                            'notes': review_notes or "ExecuÃ§Ã£o revisada e aceita pelo classificador.",
                                            'timestamp': datetime.now().isoformat(),
                                            'status_final': 'aprovada'
                                        },
                                        'approver': None  # Remove aprovador se nÃ£o precisa de aprovaÃ§Ã£o
                                    }
                                    add_history_entry(
                                        notification_id_review, "RevisÃ£o de ExecuÃ§Ã£o: ConclusÃ£o Aceita e Finalizada",
                                        user_name,
                                        f"ExecuÃ§Ã£o revisada e aceita pelo classificador. Ciclo de gestÃ£o do evento concluÃ­do (nÃ£o requeria aprovaÃ§Ã£o superior)." + (
                                            f" Obs: {review_notes}" if review_notes else ""))
                                    st.success(
                                        f"âœ… ExecuÃ§Ã£o da NotificaÃ§Ã£o #{notification_id_review} revisada e aceita. NotificaÃ§Ã£o concluÃ­da!")
                            elif review_decision_state == "Rejeitar ConclusÃ£o":
                                new_status = 'pendente_classificacao'  # Retorna para classif. inicial
                                updates = {
                                    'status': new_status,
                                    'approver': None,
                                    'executors': [],
                                    'classification': None,
                                    'review_execution': None,  # Limpa revisÃ£o
                                    'approval': None,
                                    'conclusion': None,
                                    'rejection_execution_review': {  # Adiciona o motivo da rejeiÃ§Ã£o da execuÃ§Ã£o
                                        'reason': current_review_data.get('rejection_reason_review'),
                                        'reviewed_by': user_username,
                                        'timestamp': datetime.now().isoformat()
                                    }
                                }
                                add_history_entry(
                                    notification_id_review,
                                    "RevisÃ£o de ExecuÃ§Ã£o: ConclusÃ£o Rejeitada e ReclassificaÃ§Ã£o NecessÃ¡ria",
                                    user_name,
                                    f"ExecuÃ§Ã£o rejeitada. NotificaÃ§Ã£o movida para classificaÃ§Ã£o inicial para reanÃ¡lise e reatribuiÃ§Ã£o. Motivo: {current_review_data.get('rejection_reason_review', '')[:150]}..." if len(
                                        current_review_data.get('rejection_reason_review',
                                                                '')) > 150 else f"ExecuÃ§Ã£o rejeitada. NotificaÃ§Ã£o movida para classificaÃ§Ã£o inicial para reanÃ¡lise e reatribuiÃ§Ã£o. Motivo: {current_review_data.get('rejection_reason_review', '')}" + (
                                        f" Obs: {review_notes}" if review_notes else ""))
                                st.warning(
                                    f"âš ï¸ ExecuÃ§Ã£o da NotificaÃ§Ã£o #{notification_id_review} rejeitada! Devolvida para classificaÃ§Ã£o inicial para reanÃ¡lise e reatribuiÃ§Ã£o.")
                                st.info(
                                    "A notificaÃ§Ã£o foi movida para o status 'pendente_classificacao' e aparecerÃ¡ na aba 'Pendentes ClassificaÃ§Ã£o Inicial' para que a equipe de classificaÃ§Ã£o possa reclassificÃ¡-la e redefinir o fluxo.")
                            update_notification(notification_id_review, updates)  # Atualiza no DB
                            st.session_state.review_classification_state.pop(notification_id_review, None)
                            st.session_state.pop('current_review_classification_id', None)
                            st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render
            else:
                if pending_execution_review:
                    st.info(f"ðŸ‘† Selecione uma notificaÃ§Ã£o da lista acima para revisar a execuÃ§Ã£o concluÃ­da.")
    with tab_closed_notifs:
        st.markdown("### NotificaÃ§Ãµes Encerradas")

        if not closed_notifications:
            st.info("âœ… NÃ£o hÃ¡ notificaÃ§Ãµes encerradas no momento.")
        else:
            st.info(f"Total de notificaÃ§Ãµes encerradas: {len(closed_notifications)}.")

            search_query = st.text_input(
                "ðŸ”Ž Buscar NotificaÃ§Ã£o Encerrada (TÃ­tulo, DescriÃ§Ã£o, ID):",
                key="closed_notif_search_input",
                placeholder="Ex: 'queda paciente', '12345', 'medicamento errado'"
            ).lower()
            filtered_closed_notifications = []
            if search_query:
                for notif in closed_notifications:
                    if search_query.isdigit() and int(search_query) == notif.get('id'):
                        filtered_closed_notifications.append(notif)
                    elif (search_query in notif.get('title', '').lower() or
                          search_query in notif.get('description', '').lower()):
                        filtered_closed_notifications.append(notif)
            else:
                filtered_closed_notifications = closed_notifications

            if not filtered_closed_notifications:
                st.warning(
                    "âš ï¸ Nenhuma notificaÃ§Ã£o encontrada com os critÃ©rios de busca especificados.")
            else:
                filtered_closed_notifications.sort(key=lambda x: x.get('created_at', ''), reverse=True)
                st.markdown(f"**NotificaÃ§Ãµes Encontradas ({len(filtered_closed_notifications)})**:")
                for notification in filtered_closed_notifications:
                    status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
                    created_at_str = notification.get('created_at', UI_TEXTS.text_na)
                    if created_at_str != UI_TEXTS.text_na:
                        try:
                            created_at_str = datetime.fromisoformat(created_at_str).strftime('%d/%m/%Y %H:%M:%S')
                        except ValueError:
                            pass
                    concluded_by = UI_TEXTS.text_na
                    if notification.get('conclusion') and notification['conclusion'].get('concluded_by'):
                        concluded_by = notification['conclusion']['concluded_by']
                    elif notification.get('approval') and (notification.get('approval') or {}).get('approved_by'):
                        concluded_by = (notification.get('approval') or {}).get('approved_by')
                    elif notification.get('rejection_classification') and (
                            notification.get('rejection_classification') or {}).get('classified_by'):
                        concluded_by = (notification.get('rejection_classification') or {}).get('classified_by')
                    elif notification.get('rejection_approval') and (notification.get('rejection_approval') or {}).get(
                            'rejected_by'):
                        concluded_by = (notification.get('rejection_approval') or {}).get('rejected_by')
# Determinar o status do prazo para notificaÃ§Ãµes encerradas
                    raw_classification_data = notification.get('classification')
                    if isinstance(raw_classification_data, dict):
                        classif_info_closed = raw_classification_data
                    else:
                        classif_info_closed = {}
                    deadline_date_str_closed = classif_info_closed.get('deadline_date')
# Acessa 'timestamp' de 'conclusion' de forma segura
                    concluded_timestamp_str = (notification.get('conclusion') or {}).get('timestamp')
                    # Verificar se a conclusÃ£o foi dentro ou fora do prazo
                    deadline_status = get_deadline_status(deadline_date_str_closed, concluded_timestamp_str)
                    card_class = ""
                    if deadline_status['class'] == "deadline-ontrack" or deadline_status['class'] == "deadline-duesoon":
                        card_class = "card-prazo-dentro"
                    elif deadline_status['class'] == "deadline-overdue":
                        card_class = "card-prazo-fora"
                    st.markdown(f"""
                            <div class="notification-card {card_class}">
                                <h4>#{notification.get('id', UI_TEXTS.text_na)} - {notification.get('title', UI_TEXTS.text_na)}</h4>
                                <p><strong>Status Final:</strong> <span class="{status_class}">{notification.get('status', UI_TEXTS.text_na).replace('_', ' ').title()}</span></p>
                                <p><strong>Encerrada por:</strong> {concluded_by} | <strong>Data de CriaÃ§Ã£o:</strong> {created_at_str}</p>
                                <p><strong>Prazo:</strong> {deadline_status['text']}</p>
                            </div>
                            """, unsafe_allow_html=True)
                    with st.expander(
                            f"ðŸ‘ï¸ Visualizar Detalhes - NotificaÃ§Ã£o #{notification.get('id', UI_TEXTS.text_na)}"):
                        display_notification_full_details(notification, st.session_state.user.get('id'),
                                                          st.session_state.user.get('username'))

@st_fragment
def show_execution():
    """Renderiza a pÃ¡gina para executores visualizarem notificaÃ§Ãµes atribuÃ­das e registrarem aÃ§Ãµes."""
    if not check_permission('executor'):
        st.error("âŒ Acesso negado! VocÃª nÃ£o tem permissÃ£o para executar notificaÃ§Ãµes.")
        return
    st.markdown("<h1 class='main-header'>âš¡ ExecuÃ§Ã£o de NotificaÃ§Ãµes</h1>", unsafe_allow_html=True)
    st.info(
        "Nesta pÃ¡gina, vocÃª pode visualizar as notificaÃ§Ãµes atribuÃ­das a vocÃª, registrar as aÃ§Ãµes executadas e marcar sua parte como concluÃ­da.")
    all_notifications = load_notifications()  # Carrega do DB
    user_id_logged_in = st.session_state.user.get('id')
    user_username_logged_in = st.session_state.user.get('username')

    all_users = load_users()  # Carrega usuÃ¡rios do DB
    display_name_to_id_map = {
        f"{user.get('name', UI_TEXTS.text_na)} ({user.get('username', UI_TEXTS.text_na)})": user['id']
        for user in all_users
    }

    user_active_notifications = []
    active_execution_statuses = ['classificada', 'em_execucao']
    for notification in all_notifications:
        is_assigned_to_current_user = False
        assigned_executors_raw = notification.get('executors', [])
        for executor_entry in assigned_executors_raw:  # assigned_executors_raw agora Ã© uma lista de IDs inteiros
            if isinstance(executor_entry, int) and executor_entry == user_id_logged_in:
                is_assigned_to_current_user = True
                break
            # Remover ou ajustar a lÃ³gica para o caso de string se os IDs estiverem sempre em int
            # elif isinstance(executor_entry, str):
            #     resolved_id = display_name_to_id_map.get(executor_entry)
            #     if resolved_id == user_id_logged_in:
            #         is_assigned_to_current_user = True
            #         break

        if is_assigned_to_current_user and notification.get('status') in active_execution_statuses:
            user_active_notifications.append(notification)
    closed_statuses = ['aprovada', 'rejeitada', 'reprovada', 'concluida']
    closed_my_exec_notifications = [
        n for n in all_notifications
        if n.get('status') in closed_statuses and user_id_logged_in in n.get('executors', [])
        # IDs dos executores sÃ£o inteiros
    ]

    if not user_active_notifications and not closed_my_exec_notifications:
        st.info("âœ… NÃ£o hÃ¡ notificaÃ§Ãµes ativas atribuÃ­das a vocÃª no momento. Verifique com seu gestor ou classificador.")
        return

    st.success(f"VocÃª tem {len(user_active_notifications)} notificaÃ§Ã£o(es) atribuÃ­da(s) aguardando ou em execuÃ§Ã£o.")
    tab_active_notifications, tab_closed_my_exec_notifs = st.tabs(
        ["ðŸ”„ NotificaÃ§Ãµes AtribuÃ­das (Ativas)", f"âœ… Minhas AÃ§Ãµes Encerradas ({len(closed_my_exec_notifications)})"]
    )
    with tab_active_notifications:
        st.markdown("### NotificaÃ§Ãµes Aguardando ou Em ExecuÃ§Ã£o")
        priority_order = {p: i for i, p in enumerate(FORM_DATA.prioridades)}
        user_active_notifications.sort(key=lambda x: (
            priority_order.get(x.get('classification', {}).get('prioridade', 'Baixa'), len(FORM_DATA.prioridades)),
            datetime.fromisoformat(x.get('created_at', '1900-01-01T00:00:00')).timestamp()
        ))

        for notification in user_active_notifications:
            status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
            classif_info = notification.get('classification') or {}
            prioridade_display = classif_info.get('prioridade', UI_TEXTS.text_na)
            prioridade_display = prioridade_display if prioridade_display != 'Selecionar' else f"{UI_TEXTS.text_na} (NÃ£o Classificado)"
            deadline_date_str = classif_info.get('deadline_date')
            concluded_timestamp_str = (notification.get('conclusion') or {}).get('timestamp')

            deadline_status = get_deadline_status(deadline_date_str, concluded_timestamp_str)

            card_class = ""
            if deadline_status['class'] == "deadline-ontrack" or deadline_status['class'] == "deadline-duesoon":
                card_class = "card-prazo-dentro"
            elif deadline_status['class'] == "deadline-overdue":
                card_class = "card-prazo-fora"
            st.markdown(f"""
                    <div class="notification-card {card_class}">
                        <h4>#{notification.get('id', UI_TEXTS.text_na)} - {notification.get('title', UI_TEXTS.text_na)}</h4>
                        <p><strong>Status Atual:</strong> <span class="{status_class}">{notification.get('status', UI_TEXTS.text_na).replace('_', ' ').title()}</span></p>
                        <p><strong>Local do Evento:</strong> {notification.get('location', UI_TEXTS.text_na)} | <strong>Prioridade:</strong> {prioridade_display} <strong class='{deadline_status['class']}'>Prazo: {deadline_status['text']}</strong></p>
                    </div>
                    """, unsafe_allow_html=True)
# --- NOVO CARD EXPANSÃVEL: Detalhes Completos da NotificaÃ§Ã£o e ClassificaÃ§Ã£o ---
            with st.expander(
                    f"âœ¨ Ver Detalhes Completos e ClassificaÃ§Ã£o - NotificaÃ§Ã£o #{notification.get('id', UI_TEXTS.text_na)}"):
                display_notification_full_details(notification, user_id_logged_in, user_username_logged_in)
            # --- FIM DO NOVO CARD EXPANSÃVEL ---
# NOVO: Card para exibir aÃ§Ãµes recentes para esta notificaÃ§Ã£o
            if notification.get('actions'):
                st.markdown("#### âš¡ HistÃ³rico de AÃ§Ãµes Realizadas")
                with st.expander(
                        f"Ver histÃ³rico de aÃ§Ãµes para NotificaÃ§Ã£o #{notification.get('id', UI_TEXTS.text_na)}"):
                    sorted_actions = sorted(notification['actions'], key=lambda x: x.get('timestamp', ''))
                    for action in sorted_actions:
                        action_type = "ðŸ CONCLUSÃƒO (Executor)" if action.get(
                            'final_action_by_executor') else "   AÃ‡ÃƒO Registrada"
                        action_timestamp = action.get('timestamp', UI_TEXTS.text_na)
                        if action_timestamp != UI_TEXTS.text_na:
                            try:
                                action_timestamp = datetime.fromisoformat(action_timestamp).strftime(
                                    '%d/%m/%Y %H:%M:%S')
                            except ValueError:
                                pass
                        if user_id_logged_in and action.get('executor_id') == user_id_logged_in:
                            st.markdown(f"""
                            <div class='my-action-entry-card'>
                                <strong>{action_type}</strong> - por <strong>VOCÃŠ ({action.get('executor_name', UI_TEXTS.text_na)})</strong> em {action_timestamp}
                                <br>
                                <em>{action.get('description', UI_TEXTS.text_na)}</em>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                            <div class='action-entry-card'>
                                <strong>{action_type}</strong> - por <strong>{action.get('executor_name', UI_TEXTS.text_na)}</strong> em {action_timestamp}
                                <br>
                                <em>{action.get('description', UI_TEXTS.text_na)}</em>
                            </div>
                            """, unsafe_allow_html=True)
# Exibir evidÃªncias se disponÃ­vel e for uma aÃ§Ã£o final
                        if action.get('final_action_by_executor'):
                            evidence_desc = action.get('evidence_description', '').strip()
                            evidence_atts = action.get('evidence_attachments', [])
                            if evidence_desc or evidence_atts:
                                st.markdown(f"""<div class='evidence-section'>""", unsafe_allow_html=True)
                                st.markdown("<h6>EvidÃªncias da ConclusÃ£o:</h6>", unsafe_allow_html=True)
                                if evidence_desc:
                                    st.info(evidence_desc)
                                if evidence_atts:
                                    for attach_info in evidence_atts:
                                        unique_name = attach_info.get('unique_name')
                                        original_name = attach_info.get('original_name')
                                        if unique_name and original_name:
                                            file_content = get_attachment_data(unique_name)
                                            if file_content:
                                                st.download_button(
                                                    label=f"Baixar EvidÃªncia: {original_name}",
                                                    data=file_content,
                                                    file_name=original_name,
                                                    mime="application/octet-stream",
                                                    key=f"download_action_evidence_exec_{notification['id']}_{unique_name}"
                                                )
                                            else:
                                                st.write(
                                                    f"Anexo: {original_name} (arquivo nÃ£o encontrado ou corrompido)")
                                st.markdown(f"""</div>""", unsafe_allow_html=True)
                        st.markdown("---")
            # FIM DO NOVO CARD DE HISTÃ“RICO DE AÃ‡Ã•ES
            executor_has_already_concluded_their_part = False
            if user_id_logged_in:
                # Agora buscando as aÃ§Ãµes do DB
                notif_actions = get_notification_actions(notification.get('id'))
                for action_entry in notif_actions:
                    if action_entry.get('executor_id') == user_id_logged_in and action_entry.get(
                            'final_action_by_executor') == True:
                        executor_has_already_concluded_their_part = True
                        break

            action_choice_key = f"exec_action_choice_{notification.get('id', UI_TEXTS.text_na)}_refactored"

            if action_choice_key not in st.session_state:
                st.session_state[action_choice_key] = UI_TEXTS.selectbox_default_acao_realizar
            if executor_has_already_concluded_their_part:
                st.info(
                    f"âœ… Sua parte na execuÃ§Ã£o da NotificaÃ§Ã£o #{notification.get('id')} jÃ¡ foi concluÃ­da. VocÃª nÃ£o pode adicionar mais aÃ§Ãµes para esta notificaÃ§Ã£o.")
            else:
                st.markdown("### ðŸ“ Registrar AÃ§Ã£o Executada ou Concluir Sua Parte")
                action_type_choice_options = [UI_TEXTS.selectbox_default_acao_realizar, "Registrar AÃ§Ã£o",
                                              "Concluir Minha Parte"]

                st.selectbox(
                    "Qual aÃ§Ã£o deseja realizar?*", options=action_type_choice_options,
                    key=action_choice_key,
                    index=action_type_choice_options.index(st.session_state[action_choice_key]),
                    help="Selecione 'Registrar AÃ§Ã£o' para adicionar um passo ao histÃ³rico ou 'Concluir Minha Parte' para finalizar sua execuÃ§Ã£o."
                )
                with st.form(f"action_form_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                             clear_on_submit=False):
                    st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                    action_description_state = st.text_area(
                        "DescriÃ§Ã£o detalhada da aÃ§Ã£o realizada*",
                        value=st.session_state.get(
                            f"exec_action_desc_{notification.get('id', UI_TEXTS.text_na)}_refactored", ""),
                        placeholder="Descreva:\nâ€¢ O QUÃŠ foi feito?\nâ€¢ POR QUÃŠ foi feito (qual o objetivo)?\nâ€¢ ONDE foi realizado?\nâ€¢ QUANDO foi realizado (data/hora)?\nâ€¢ QUEM executou (se aplicÃ¡vel)?\nâ€¢ COMO foi executado (passos, mÃ©todos)?\nâ€¢ QUANTO CUSTOU (recursos, tempo)?\nâ€¢ QUÃƒO FREQUENTE (se for uma aÃ§Ã£o contÃ­nua)?",
                        height=180,
                        key=f"exec_action_desc_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                        help="ForneÃ§a um relato completo e estruturado da aÃ§Ã£o executada ou da conclusÃ£o da sua parte, utilizando os pontos do 5W3H como guia."
                    ).strip()
                    evidence_description_state = ""
                    uploaded_evidence_files = []
                    if st.session_state[action_choice_key] == "Concluir Minha Parte":
                        st.markdown("""
                           <div class="conditional-field">
                               <h4>âœ… EvidÃªncias da Tratativa</h4>
                               <p>Descreva e anexe as evidÃªncias da tratativa realizada para esta notificaÃ§Ã£o.</p>
                           </div>
                           """, unsafe_allow_html=True)
                        evidence_description_state = st.text_area(
                            "DescriÃ§Ã£o da EvidÃªncia (Opcional)",
                            value=st.session_state.get(
                                f"exec_evidence_desc_{notification.get('id', UI_TEXTS.text_na)}_refactored", ""),
                            placeholder="Descreva o resultado da tratativa, evidÃªncias de conclusÃ£o, etc.",
                            height=100,
                            key=f"exec_evidence_desc_{notification.get('id', UI_TEXTS.text_na)}_refactored"
                        ).strip()
                        uploaded_evidence_files = st.file_uploader(
                            "Anexar arquivos de EvidÃªncia (Opcional)", type=None, accept_multiple_files=True,
                            key=f"exec_evidence_attachments_{notification.get('id', UI_TEXTS.text_na)}_refactored"
                        )

                    submit_button = st.form_submit_button("âœ”ï¸ Confirmar AÃ§Ã£o",
                                                          use_container_width=True)
                    st.markdown("---")

                    if submit_button:
                        validation_errors = []
                        if st.session_state[action_choice_key] == UI_TEXTS.selectbox_default_acao_realizar:
                            validation_errors.append("Ã‰ obrigatÃ³rio selecionar o tipo de aÃ§Ã£o (Registrar ou Concluir).")
                        if not action_description_state:
                            validation_errors.append("A descriÃ§Ã£o detalhada da aÃ§Ã£o Ã© obrigatÃ³ria.")
                        if validation_errors:
                            st.error("âš ï¸ **Por favor, corrija os seguintes erros:**")
                            for error in validation_errors: st.warning(error)
                        else:
                            # Recarrega a notificaÃ§Ã£o para ter a versÃ£o mais atualizada antes de modificar
                            current_notification_in_list = next(
                                (n for n in load_notifications() if n.get('id') == notification.get('id')), None)
                            if not current_notification_in_list:
                                st.error(
                                    "Erro interno: NotificaÃ§Ã£o nÃ£o encontrada na lista principal para atualizaÃ§Ã£o.")
                            else:
                                # Re-verificaÃ§Ã£o de conclusÃ£o final do executor diretamente no DB
                                recheck_executor_already_concluded = False
                                notif_actions_db = get_notification_actions(notification.get('id'))
                                for existing_action_recheck in notif_actions_db:
                                    if existing_action_recheck.get(
                                            'executor_id') == user_id_logged_in and existing_action_recheck.get(
                                        'final_action_by_executor') == True:
                                        recheck_executor_already_concluded = True
                                        break
                                if recheck_executor_already_concluded:
                                    st.error(
                                        "âŒ Sua parte nesta notificaÃ§Ã£o jÃ¡ foi marcada como concluÃ­da anteriormente. OperaÃ§Ã£o abortada.")
                                    st.session_state[action_choice_key] = UI_TEXTS.selectbox_default_acao_realizar
                                    _clear_execution_form_state(notification['id'])
                                    st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render
                                else:
                                    saved_evidence_attachments = []
                                    if st.session_state[
                                        action_choice_key] == "Concluir Minha Parte" and uploaded_evidence_files:
                                        for file in uploaded_evidence_files:
                                            # Salva o arquivo no disco
                                            saved_file_info = save_uploaded_file_to_disk(file, notification.get('id'))
                                            if saved_file_info:
                                                saved_evidence_attachments.append(saved_file_info)
                                    action_data_to_add = {
                                        'executor_id': user_id_logged_in,
                                        'executor_name': user_username_logged_in,
                                        'description': action_description_state,
                                        'timestamp': datetime.now().isoformat(),
                                        'final_action_by_executor': st.session_state[
                                                                        action_choice_key] == "Concluir Minha Parte",
                                        'evidence_description': evidence_description_state if st.session_state[
                                            action_choice_key] == "Concluir Minha Parte" else None,
                                        'evidence_attachments': saved_evidence_attachments if st.session_state[
                                            action_choice_key] == "Concluir Minha Parte" else None
                                    }
# Adiciona a aÃ§Ã£o no banco de dados
                                    add_notification_action(notification['id'], action_data_to_add)
                                    if st.session_state[action_choice_key] == "Registrar AÃ§Ã£o":
                                        if current_notification_in_list.get('status') == 'classificada':
                                            # Atualiza o status no DB
                                            update_notification(notification['id'], {'status': 'em_execucao'})
                                        add_history_entry(notification['id'],
                                                          "AÃ§Ã£o registrada (ExecuÃ§Ã£o)",
                                                          user_username_logged_in,
                                                          f"Registrou aÃ§Ã£o: {action_description_state[:100]}..." if len(
                                                              action_description_state) > 100 else f"Registrou aÃ§Ã£o: {action_description_state}")
                                        st.toast("âœ… AÃ§Ã£o registrada com sucesso!", icon="ðŸŽ‰")
                                    elif st.session_state[action_choice_key] == "Concluir Minha Parte":
                                        # Recarrega as aÃ§Ãµes para ter a lista atualizada do DB
                                        all_actions_for_notif = get_notification_actions(notification['id'])
                                        all_assigned_executors_ids = set(
                                            current_notification_in_list.get('executors', []))
                                        executors_who_concluded_ids = set(
                                            a.get('executor_id') for a in all_actions_for_notif if
                                            a.get('final_action_by_executor'))
                                        all_executors_concluded = all_assigned_executors_ids.issubset(
                                            executors_who_concluded_ids) and len(all_assigned_executors_ids) > 0
                                        updates_to_status = {}
                                        if all_executors_concluded:
                                            updates_to_status['status'] = 'revisao_classificador_execucao'
                                            st.toast(
                                                "âœ… Todos os executores concluÃ­ram suas partes. NotificaÃ§Ã£o encaminhada para revisÃ£o!",
                                                icon="ðŸ")
                                        else:
                                            st.toast("âœ… Sua execuÃ§Ã£o foi concluÃ­da nesta notificaÃ§Ã£o!", icon="âœ…")
                                        history_details = f"Executor {user_username_logged_in} concluiu sua parte das aÃ§Ãµes."
                                        add_history_entry(
                                            notification['id'],
                                            "ExecuÃ§Ã£o concluÃ­da (por executor)",
                                            user_username_logged_in,
                                            history_details
                                        )
# Atualiza apenas o status se necessÃ¡rio
                                        if updates_to_status:
                                            update_notification(notification['id'], updates_to_status)
                                        st.success(
                                            f"âœ… Sua execuÃ§Ã£o foi concluÃ­da nesta notificaÃ§Ã£o! Status atual: '{current_notification_in_list['status'].replace('_', ' ').title()}'.")
                                        if not all_executors_concluded:
                                            users_list_exec = load_users()
                                            remaining_executors_ids = list(
                                                all_assigned_executors_ids - executors_who_concluded_ids)
                                            remaining_executors_names = [u.get('name', UI_TEXTS.text_na) for u in
                                                                         users_list_exec if
                                                                         u.get('id') in remaining_executors_ids]
                                            st.info(
                                                f"Aguardando conclusÃ£o dos seguintes executores: {', '.join(remaining_executors_names) or 'Nenhum'}.")
                                        elif all_executors_concluded:
                                            st.info(
                                                f"Todos os executores concluÃ­ram suas partes. A notificaÃ§Ã£o foi enviada para revisÃ£o final pelo classificador.\n\nEvidÃªncia da tratativa:\n{evidence_description_state}\n\nAnexos: {len(saved_evidence_attachments) if saved_evidence_attachments else 0}")
                                    _clear_execution_form_state(notification['id'])
                                    st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render
                with st.expander("ðŸ‘¥ Adicionar Executor Adicional"):
                    with st.form(f"add_executor_form_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                                 clear_on_submit=True):
                        executors = get_users_by_role('executor')
                        current_executors_ids = notification.get('executors', [])
                        available_executors = [e for e in executors if e.get('id') not in current_executors_ids]
                        if available_executors:
                            executor_options = {
                                f"{e.get('name', UI_TEXTS.text_na)} ({e.get('username', UI_TEXTS.text_na)})": e['id']
                                for e in available_executors
                            }
                            add_executor_display_options = [UI_TEXTS.multiselect_instruction_placeholder] + list(
                                executor_options.keys())
                            default_add_executor_selection = [UI_TEXTS.multiselect_instruction_placeholder]
                            new_executor_name_to_add_raw = st.selectbox(
                                "Selecionar executor para adicionar:*",
                                options=add_executor_display_options,
                                index=add_executor_display_options.index(default_add_executor_selection[0]),
                                key=f"add_executor_select_exec_{notification.get('id', UI_TEXTS.text_na)}_form_refactored",
                                help="Selecione o usuÃ¡rio executor que serÃ¡ adicionado a esta notificaÃ§Ã£o."
                            )
                            new_executor_name_to_add = (
                                new_executor_name_to_add_raw
                                if new_executor_name_to_add_raw != UI_TEXTS.multiselect_instruction_placeholder
                                else None
                            )

                            st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>",
                                        unsafe_allow_html=True)
                            submit_button = st.form_submit_button("âž• Adicionar Executor",
                                                                  use_container_width=True)
                            if submit_button:
                                if new_executor_name_to_add:
                                    new_executor_id = executor_options[new_executor_name_to_add]
                                    # Recarrega a notificaÃ§Ã£o para ter a versÃ£o mais atualizada antes de modificar
                                    current_notification_in_list = next(
                                        (n for n in load_notifications() if n.get('id') == notification.get('id')), None)
                                    if current_notification_in_list:
                                        # Adiciona o novo executor Ã  lista existente (no Python)
                                        updated_executors = current_notification_in_list.get('executors', []) + [
                                            new_executor_id]
                                        # Atualiza no DB
                                        update_notification(notification.get('id'), {'executors': updated_executors})

                                        add_history_entry(
                                            notification.get('id'), "Executor adicionado (durante execuÃ§Ã£o)",
                                            user_username_logged_in,
                                            f"Adicionado o executor: {new_executor_name_to_add}"
                                        )
                                        st.success(
                                            f"âœ… {new_executor_name_to_add} adicionado como executor para esta notificaÃ§Ã£o.")
                                        st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render
                                    else:
                                        st.error("Erro: NotificaÃ§Ã£o nÃ£o encontrada para adicionar executor.")
                                else:
                                    st.error("âš ï¸ Por favor, selecione um executor para adicionar.")
                        else:
                            st.info("NÃ£o hÃ¡ executores adicionais disponÃ­veis para atribuiÃ§Ã£o no momento.")
    with tab_closed_my_exec_notifs:
        st.markdown("### Minhas AÃ§Ãµes Encerradas")
        if not closed_my_exec_notifications:
            st.info("âœ… NÃ£o hÃ¡ notificaÃ§Ãµes encerradas em que vocÃª estava envolvido como executor no momento.")
        else:
            st.info(
                f"Total de notificaÃ§Ãµes encerradas em que vocÃª estava envolvido: {len(closed_my_exec_notifications)}.")

            search_query_exec_closed = st.text_input(
                "ðŸ”Ž Buscar em Minhas AÃ§Ãµes Encerradas (TÃ­tulo, DescriÃ§Ã£o, ID):",
                key="closed_exec_notif_search_input",
                placeholder="Ex: 'reparo', '987', 'instalaÃ§Ã£o'"
            ).lower()
            filtered_closed_my_exec_notifications = []
            if search_query_exec_closed:
                for notif in closed_my_exec_notifications:
                    if search_query_exec_closed.isdigit() and int(search_query_exec_closed) == notif.get('id'):
                        filtered_closed_my_exec_notifications.append(notif)
                    elif (search_query_exec_closed in notif.get('title', '').lower() or
                          search_query_exec_closed in notif.get('description', '').lower()):
                        filtered_closed_my_exec_notifications.append(notif)
            else:
                filtered_closed_my_exec_notifications = closed_my_exec_notifications
            if not filtered_closed_my_exec_notifications:
                st.warning(
                    "âš ï¸ Nenhuma notificaÃ§Ã£o encontrada com os critÃ©rios de busca especificados em suas aÃ§Ãµes encerradas.")
            else:
                filtered_closed_my_exec_notifications.sort(key=lambda x: x.get('created_at', ''), reverse=True)

                st.markdown(f"**NotificaÃ§Ãµes Encontradas ({len(filtered_closed_my_exec_notifications)})**:")
                for notification in filtered_closed_my_exec_notifications:
                    status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
                    created_at_str = notification.get('created_at', UI_TEXTS.text_na)
                    if created_at_str != UI_TEXTS.text_na:
                        try:
                            created_at_str = datetime.fromisoformat(created_at_str).strftime('%d/%m/%Y %H:%M:%S')
                        except ValueError:
                            pass
                    concluded_by = UI_TEXTS.text_na
                    if notification.get('conclusion') and notification['conclusion'].get('concluded_by'):
                        concluded_by = notification['conclusion']['concluded_by']
                    elif notification.get('approval') and (notification.get('approval') or {}).get('approved_by'):
                        concluded_by = (notification.get('approval') or {}).get('approved_by')
                    elif notification.get('rejection_classification') and (
                            notification.get('rejection_classification') or {}).get('classified_by'):
                        concluded_by = (notification.get('rejection_classification') or {}).get('classified_by')
                    elif notification.get('rejection_approval') and (notification.get('rejection_approval') or {}).get(
                            'rejected_by'):
                        concluded_by = (notification.get('rejection_approval') or {}).get('rejected_by')
                    deadline_info = notification.get('classification', {}).get('deadline_date')
                    concluded_timestamp_str = (notification.get('conclusion') or {}).get('timestamp')

                    deadline_status = get_deadline_status(deadline_info, concluded_timestamp_str)
                    card_class = ""
                    if deadline_status['class'] == "deadline-ontrack" or deadline_status['class'] == "deadline-duesoon":
                        card_class = "card-prazo-dentro"
                    elif deadline_status['class'] == "deadline-overdue":
                        card_class = "card-prazo-fora"
                    st.markdown(f"""
                            <div class="notification-card {card_class}">
                                <h4>#{notification.get('id', UI_TEXTS.text_na)} - {notification.get('title', UI_TEXTS.text_na)}</h4>
                                <p><strong>Status Final:</strong> <span class="{status_class}">{notification.get('status', UI_TEXTS.text_na).replace('_', ' ').title()}</span></p>
                                <p><strong>Encerrada por:</strong> {concluded_by} | <strong>Data de CriaÃ§Ã£o:</strong> {created_at_str}</p>
                                <p><strong>Prazo:</strong> {deadline_status['text']}</p>
                            </div>
                            """, unsafe_allow_html=True)
                    with st.expander(
                            f"  ï¸ Visualizar Detalhes - NotificaÃ§Ã£o #{notification.get('id', UI_TEXTS.text_na)}"):
                        display_notification_full_details(notification, user_id_logged_in, user_username_logged_in)


@st_fragment
def show_approval():
    """Renderiza a pÃ¡gina para aprovadores revisarem e aprovarem/rejeitarem notificaÃ§Ãµes."""
    if not check_permission('aprovador'):
        st.error("âŒ Acesso negado! VocÃª nÃ£o tem permissÃ£o para aprovar notificaÃ§Ãµes.")
        return

    st.markdown("<h1 class='main-header'>âœ… AprovaÃ§Ã£o de NotificaÃ§Ãµes</h1>", unsafe_allow_html=True)
    st.info(
        "ðŸ“‹ Analise as notificaÃ§Ãµes que foram concluÃ­das pelos executores e revisadas/aceitas pelo classificador, e que requerem sua aprovaÃ§Ã£o final.")
    all_notifications = load_notifications()  # Carrega do DB
    user_id_logged_in = st.session_state.user.get('id')
    user_username_logged_in = st.session_state.user.get('username')
    pending_approval = [n for n in all_notifications if
                        n.get('status') == 'aguardando_aprovacao' and n.get('approver') == user_id_logged_in]

    closed_statuses = ['aprovada', 'rejeitada', 'reprovada', 'concluida']
    closed_my_approval_notifications = [
        n for n in all_notifications
        if n.get('status') in closed_statuses and (
                (n.get('status') == 'aprovada' and (n.get('approval') or {}).get(
                    'approved_by') == user_username_logged_in) or
                (n.get('status') == 'reprovada' and (n.get('rejection_approval') or {}).get(
                    'rejected_by') == user_username_logged_in)
        )
    ]

    if not pending_approval and not closed_my_approval_notifications:
        st.info("âœ… NÃ£o hÃ¡ notificaÃ§Ãµes aguardando sua aprovaÃ§Ã£o ou que foram encerradas por vocÃª no momento.")
        return

    st.success(f"â³ VocÃª tem {len(pending_approval)} notificaÃ§Ã£o(es) aguardando sua aprovaÃ§Ã£o.")

    tab_pending_approval, tab_closed_my_approval_notifs = st.tabs(
        ["â³ Aguardando Minha AprovaÃ§Ã£o", f"âœ… Minhas AprovaÃ§Ãµes Encerradas ({len(closed_my_approval_notifications)})"]
    )

    with tab_pending_approval:
        priority_order = {p: i for i, p in enumerate(FORM_DATA.prioridades)}
        pending_approval.sort(key=lambda x: (
            priority_order.get(x.get('classification', {}).get('prioridade', 'Baixa'), len(FORM_DATA.prioridades)),
            datetime.fromisoformat(
                x.get('classification', {}).get('classification_timestamp',
                                                '1900-01-01T00:00:00')).timestamp() if x.get(
                'classification', {}).get('classification_timestamp') else 0
        ))
        for notification in pending_approval:
            status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
            classif_info = notification.get('classification') or {}
            prioridade_display = classif_info.get('prioridade', UI_TEXTS.text_na)
            prioridade_display = prioridade_display if prioridade_display != 'Selecionar' else f"{UI_TEXTS.text_na} (NÃ£o Classificado)"

            # Obter informaÃ§Ãµes de prazo para o card
            deadline_date_str = classif_info.get('deadline_date')

                        # Acessa 'timestamp' de 'conclusion' de forma segura
            concluded_timestamp_str = (notification.get('conclusion') or {}).get('timestamp')

            # Determinar o status do prazo (cor do texto)
            deadline_status = get_deadline_status(deadline_date_str, concluded_timestamp_str)
            # Determinar a classe do cartÃ£o (fundo) com APENAS DOIS STATUS
            card_class = ""
            if deadline_status['class'] == "deadline-ontrack" or deadline_status['class'] == "deadline-duesoon":
                card_class = "card-prazo-dentro"  # SerÃ¡ verde para "No Prazo" e "Prazo PrÃ³ximo"
            elif deadline_status['class'] == "deadline-overdue":
                card_class = "card-prazo-fora"  # SerÃ¡ vermelho para "Atrasada"
            st.markdown(f"""
                    <div class="notification-card {card_class}">
                        <h4>#{notification.get('id', UI_TEXTS.text_na)} - {notification.get('title', UI_TEXTS.text_na)}</h4>
                        <p><strong>Status Atual:</strong> <span class="{status_class}">{notification.get('status', UI_TEXTS.text_na).replace('_', ' ').title()}</span></p>
                        <p><strong>Local do Evento:</strong> {notification.get('location', UI_TEXTS.text_na)} | <strong>Prioridade:</strong> {prioridade_display} <strong class='{deadline_status['class']}'>Prazo: {deadline_status['text']}</strong></p>
                    </div>
                    """, unsafe_allow_html=True)
            with st.expander(
                    f"ðŸ“‹ AnÃ¡lise Completa e DecisÃ£o - NotificaÃ§Ã£o #{notification.get('id', UI_TEXTS.text_na)}",
                    expanded=True):
                st.markdown("### ðŸ§ Detalhes para AnÃ¡lise de AprovaÃ§Ã£o")
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**ðŸ“ Evento Original Reportado**")
                    st.write(f"**TÃ­tulo:** {notification.get('title', UI_TEXTS.text_na)}")
                    st.write(f"**Local:** {notification.get('location', UI_TEXTS.text_na)}")
                    occurrence_datetime_summary = format_date_time_summary(notification.get('occurrence_date'),
                                                                           notification.get('occurrence_time'))
                    st.write(f"**Data/Hora OcorrÃªncia:** {occurrence_datetime_summary}")
                    st.write(f"**Setor Notificante:** {notification.get('reporting_department', UI_TEXTS.text_na)}")
                    if notification.get('immediate_actions_taken') and notification.get(
                            'immediate_action_description'):
                        st.info(
                            f"**AÃ§Ãµes Imediatas Reportadas:** {notification.get('immediate_action_description', UI_TEXTS.text_na)[:300]}...")
                with col2:
                    st.markdown("**â±ï¸ InformaÃ§Ãµes de GestÃ£o e ClassificaÃ§Ã£o**")
                    classif = notification.get('classification', {})
                    never_event_display = classif.get('never_event', UI_TEXTS.text_na)
                    st.write(f"**Never Event:** {never_event_display}")
                    sentinel_display = 'Sim' if classif.get('is_sentinel_event') else (
                        'NÃ£o' if classif.get('is_sentinel_event') is False else UI_TEXTS.text_na)
                    st.write(f"**Evento Sentinela:** {sentinel_display}")
                    st.write(f"**ClassificaÃ§Ã£o NNC:** {classif.get('nnc', UI_TEXTS.text_na)}")
                    if classif.get('nivel_dano'): st.write(
                        f"**NÃ­vel de Dano:** {classif.get('nivel_dano', UI_TEXTS.text_na)}")
                    event_type_main_display = classif.get('event_type_main', UI_TEXTS.text_na)
                    st.write(f"**Tipo Principal:** {event_type_main_display}")
                    event_type_sub_display = classif.get('event_type_sub')
                    if event_type_sub_display:
                        if isinstance(event_type_sub_display, list):
                            st.write(
                                f"**EspecificaÃ§Ã£o:** {', '.join(event_type_sub_display)[:100]}..." if len(', '.join(
                                    event_type_sub_display)) > 100 else f"**EspecificaÃ§Ã£o:** {', '.join(event_type_sub_display)}")
                        else:
                            st.write(f"**EspecificaÃ§Ã£o:** {str(event_type_sub_display)[:100]}..." if len(
                                str(event_type_sub_display)) > 100 else f"**EspecificaÃ§Ã£o:** {str(event_type_sub_display)}")
                    st.write(f"**ClassificaÃ§Ã£o OMS:** {', '.join(classif.get('oms', [UI_TEXTS.text_na]))}")
                    st.write(
                        f"**Requer AprovaÃ§Ã£o Superior (Classif. Inicial):** {'Sim' if classif.get('requires_approval') else 'NÃ£o'}")
                    st.write(f"**Classificado por:** {classif.get('classificador', UI_TEXTS.text_na)}")
                    classification_timestamp_str = classif.get('classification_timestamp', UI_TEXTS.text_na)
                    if classification_timestamp_str != UI_TEXTS.text_na:
                        try:
                            classification_timestamp_str = datetime.fromisoformat(
                                classification_timestamp_str).strftime(
                                '%d/%m/%Y %H:%M:%S')
                        except ValueError:
                            pass
                        st.write(f"**Classificado em:** {classification_timestamp_str}")
# ExibiÃ§Ã£o do Prazo e Status na AprovaÃ§Ã£o
                    if deadline_date_str:
                        deadline_date_formatted = datetime.fromisoformat(deadline_date_str).strftime('%d/%m/%Y')
                        st.markdown(
                            f"**Prazo de ConclusÃ£o:** {deadline_date_formatted} (<span class='{deadline_status['class']}'>{deadline_status['text']}</span>)",
                            unsafe_allow_html=True)
                    else:
                        st.write(f"**Prazo de ConclusÃ£o:** {UI_TEXTS.deadline_days_nan}")
                st.markdown("**ðŸ“ DescriÃ§Ã£o Completa do Evento**")
                st.info(notification.get('description', UI_TEXTS.text_na))
                if classif.get('notes'):
                    st.markdown("**ðŸ“‹ OrientaÃ§Ãµes / ObservaÃ§Ãµes do Classificador (ClassificaÃ§Ã£o Inicial)**")
                    st.info(classif.get('notes', UI_TEXTS.text_na))
                if notification.get('patient_involved'):  # Se patient_involved Ã© True
                    st.markdown("**ðŸ¥ InformaÃ§Ãµes do Paciente Afetado**")
                    st.write(f"**NÂ° Atendimento/ProntuÃ¡rio:** {notification.get('patient_id', UI_TEXTS.text_na)}")
                    outcome = notification.get('patient_outcome_obito')
                    if outcome is not None:
                        st.write(f"**Evoluiu com Ã³bito?** {'Sim' if outcome is True else 'NÃ£o'}")
                    else:
                        st.write("**Evoluiu com Ã³bito?** NÃ£o informado")
                if notification.get('additional_notes'):
                    st.markdown("**â„¹ï¸ ObservaÃ§Ãµes Adicionais do Notificante**")
                    st.info(notification.get('additional_notes', UI_TEXTS.text_na))
                st.markdown("---")
                st.markdown("#### âš¡ AÃ§Ãµes Executadas pelos ResponsÃ¡veis")
                if notification.get('actions'):
                    for action in sorted(notification['actions'], key=lambda x: x.get('timestamp', '')):
                        action_type = "ðŸ CONCLUSÃƒO (Executor)" if action.get(
                            'final_action_by_executor') else "ðŸ“ AÃ‡ÃƒO Registrada"
                        action_timestamp = action.get('timestamp', UI_TEXTS.text_na)
                        if action_timestamp != UI_TEXTS.text_na:
                            try:
                                action_timestamp = datetime.fromisoformat(action_timestamp).strftime(
                                    '%d/%m/%Y %H:%M:%S')
                            except ValueError:
                                pass
                        st.markdown(f"""
                            <strong>{action_type}</strong> - por <strong>{action.get('executor_name', UI_TEXTS.text_na)}</strong> em {action_timestamp}
                            <br>
                            <em>{action.get('description', UI_TEXTS.text_na)}</em>
                            """, unsafe_allow_html=True)
                        # Exibir evidÃªncias se disponÃ­vel e for uma aÃ§Ã£o final
                        if action.get('final_action_by_executor'):
                            evidence_desc = action.get('evidence_description', '').strip()
                            evidence_atts = action.get('evidence_attachments', [])
                            if evidence_desc or evidence_atts:
                                st.markdown(f"""<div class='evidence-section'>""", unsafe_allow_html=True)
                                st.markdown("<h6>EvidÃªncias da ConclusÃ£o:</h6>", unsafe_allow_html=True)
                                if evidence_desc:
                                    st.info(evidence_desc)
                                if evidence_atts:
                                    for attach_info in evidence_atts:
                                        unique_name = attach_info.get('unique_name')
                                        original_name = attach_info.get('original_name')
                                        if unique_name and original_name:
                                            file_content = get_attachment_data(unique_name)
                                            if file_content:
                                                st.download_button(
                                                    label=f"Baixar EvidÃªncia: {original_name}",
                                                    data=file_content,
                                                    file_name=original_name,
                                                    mime="application/octet-stream",
                                                    key=f"download_action_evidence_approval_{notification['id']}_{unique_name}"
                                                )
                                            else:
                                                st.write(f"Anexo: {original_name} (arquivo nÃ£o encontrado ou corrompido)")
                                st.markdown(f"""</div>""", unsafe_allow_html=True)
                        st.markdown("---")
                else:
                    st.warning("âš ï¸ Nenhuma aÃ§Ã£o foi registrada pelos executores para esta notificaÃ§Ã£o ainda.")
                users_exec = get_users_by_role('executor')
                # Mapeia nomes de exibiÃ§Ã£o para IDs de usuÃ¡rio para executores
                executor_name_to_id_map_approval = {
                    f"{u.get('name', UI_TEXTS.text_na)} ({u.get('username', UI_TEXTS.text_na)})": u['id']
                    for u in users_exec
                }
                # Pega os nomes de exibiÃ§Ã£o dos executores atribuÃ­dos
                executor_names_approval = [
                    name for name, uid in executor_name_to_id_map_approval.items()
                    if uid in notification.get('executors', [])
                ]
                st.markdown(f"**ðŸ‘¥ Executores AtribuÃ­dos:** {', '.join(executor_names_approval) or 'Nenhum'}")
                review_exec_info = notification.get('review_execution', {})
                if review_exec_info:
                    st.markdown("---")
                    st.markdown("#### ðŸ› ï¸ Resultado da RevisÃ£o do Classificador")
                    review_decision_display = review_exec_info.get('decision', UI_TEXTS.text_na)
                    reviewed_by_display = review_exec_info.get('reviewed_by', UI_TEXTS.text_na)
                    review_timestamp_str = review_exec_info.get('timestamp', UI_TEXTS.text_na)
                    if review_timestamp_str != UI_TEXTS.text_na:
                        try:
                            review_timestamp_str = datetime.fromisoformat(review_timestamp_str).strftime(
                                '%d/%m/%Y %H:%M:%S')
                        except ValueError:
                            pass
                        st.write(f"**DecisÃ£o da RevisÃ£o:** {review_decision_display}")
                    st.write(f"**Revisado por (Classificador):** {reviewed_by_display} em {review_timestamp_str}")
                    if review_decision_display == 'Rejeitada' and review_exec_info.get('rejection_reason'):
                        st.write(
                            f"**Motivo da RejeiÃ§Ã£o:** {review_exec_info.get('rejection_reason', UI_TEXTS.text_na)}")
                    if review_exec_info.get('notes'):
                        st.write(
                            f"**ObservaÃ§Ãµes do Classificador:** {review_exec_info.get('notes', UI_TEXTS.text_na)}")
                if notification.get('attachments'):
                    st.markdown("---")
                    st.markdown("#### ðŸ“Ž Anexos")
                    for attach_info in notification['attachments']:
                        unique_name_to_use = None
                        original_name_to_use = None
                        if isinstance(attach_info,
                                      dict) and 'unique_name' in attach_info and 'original_name' in attach_info:
                            unique_name_to_use = attach_info['unique_name']
                            original_name_to_use = attach_info['original_name']
                        elif isinstance(attach_info, str):
                            unique_name_to_use = attach_info
                            original_name_to_use = attach_info
                        if unique_name_to_use:
                            file_content = get_attachment_data(unique_name_to_use)
                            if file_content:
                                st.download_button(
                                    label=f"Baixar {original_name_to_use}",
                                    data=file_content,
                                    file_name=original_name_to_use,
                                    mime="application/octet-stream",
                                    key=f"download_approval_{notification['id']}_{unique_name_to_use}"
                                )
                            else:
                                st.write(f"Anexo: {original_name_to_use} (arquivo nÃ£o encontrado ou corrompido)")
                st.markdown("---")

                # NOVO: Inicializa ou recupera o estado do formulÃ¡rio de aprovaÃ§Ã£o para esta notificaÃ§Ã£o especÃ­fica
                if 'approval_form_state' not in st.session_state:
                    st.session_state.approval_form_state = {}
                if notification.get('id') not in st.session_state.approval_form_state:
                    st.session_state.approval_form_state[notification.get('id')] = {
                        'decision': UI_TEXTS.selectbox_default_decisao_aprovacao,
                        'notes': '',
                    }
                current_approval_data = st.session_state.approval_form_state[notification.get('id')]
                with st.form(f"approval_form_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                             clear_on_submit=False):
                    st.markdown("### ðŸŽ¯ DecisÃ£o de AprovaÃ§Ã£o Final")
                    approval_decision_options = [UI_TEXTS.selectbox_default_decisao_aprovacao, "Aprovar",
                                                 "Reprovar"]
                    current_approval_data['decision'] = st.selectbox(
                        "DecisÃ£o:*", options=approval_decision_options,
                        key=f"approval_decision_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                        index=approval_decision_options.index(
                            current_approval_data.get('decision', UI_TEXTS.selectbox_default_decisao_aprovacao)),
                        help="Selecione 'Aprovar' para finalizar a notificaÃ§Ã£o ou 'Reprovar' para devolvÃª-la para revisÃ£o pelo classificador."
                    )
                    st.markdown("<span class='required-field'>* Campo obrigatÃ³rio</span>", unsafe_allow_html=True)
                    # Capture o valor do text_area e atribua-o ao `current_approval_data['notes']`
                    approval_notes_input = st.text_area(
                        "ObservaÃ§Ãµes da AprovaÃ§Ã£o/ReprovaÃ§Ã£o:*",
                        value=current_approval_data.get('notes', ''),
                        placeholder="â€¢ Avalie a completude e eficÃ¡cia das aÃ§Ãµes executadas e a revisÃ£o do classificador...\nâ€¢ Indique se as aÃ§Ãµes foram satisfatÃ³rias para mitigar o risco ou resolver o evento.\nâ€¢ ForneÃ§a recomendaÃ§Ãµes adicionais, se necessÃ¡rio.\nâ€¢ Em caso de reprovaÃ§Ã£o, explique claramente o motivo e o que precisa ser revisado ou corrigido pelo classificador.",
                        height=120, key=f"approval_notes_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                        help="ForneÃ§a sua avaliaÃ§Ã£o sobre as aÃ§Ãµes executadas, a revisÃ£o do classificador, e a decisÃ£o final.").strip()
                    current_approval_data['notes'] = approval_notes_input  # Atualiza o estado com o valor do text_area
                    submit_button = st.form_submit_button("âœ”ï¸ Confirmar DecisÃ£o",
                                                          use_container_width=True)
                    st.markdown("---")

                    if submit_button:
                        validation_errors = []
                        if current_approval_data[
                            'decision'] == UI_TEXTS.selectbox_default_decisao_aprovacao: validation_errors.append(
                            "Ã‰ obrigatÃ³rio selecionar a decisÃ£o (Aprovar/Reprovar).")
                        if current_approval_data['decision'] == "Reprovar" and not current_approval_data[
                            'notes']: validation_errors.append(
                            "Ã‰ obrigatÃ³rio informar as observaÃ§Ãµes para reprovar a notificaÃ§Ã£o.")
                        if validation_errors:
                            st.error("âš ï¸ **Por favor, corrija os seguintes erros:**")
                            for error in validation_errors: st.warning(error)
                        else:
                            user_name = st.session_state.user.get('name', 'UsuÃ¡rio')
                            user_username = st.session_state.user.get('username', UI_TEXTS.text_na)
                            approval_notes = current_approval_data['notes']
                            if current_approval_data['decision'] == "Aprovar":
                                new_status = 'aprovada'
                                updates = {
                                    'status': new_status,
                                    'approval': {
                                        'decision': 'Aprovada',
                                        'approved_by': user_username,
                                        'notes': approval_notes or None,
                                        'approved_at': datetime.now().isoformat()
                                    },
                                    'conclusion': {
                                        'concluded_by': user_username,
                                        'notes': approval_notes or "NotificaÃ§Ã£o aprovada superiormente.",
                                        'timestamp': datetime.now().isoformat(),
                                        'status_final': 'aprovada'
                                    },
                                    'approver': None
                                }
                                update_notification(notification['id'], updates)  # Atualiza no DB
                                add_history_entry(notification['id'], "NotificaÃ§Ã£o aprovada e finalizada",
                                                  user_name,
                                                  f"Aprovada superiormente." + (
                                                      f" Obs: {approval_notes[:150]}..." if approval_notes and len(
                                                          approval_notes) > 150 else (
                                                          f" Obs: {approval_notes}" if approval_notes else "")))
                                st.success(
                                    f"âœ… NotificaÃ§Ã£o #{notification['id']} aprovada e finalizada com sucesso! O ciclo de gestÃ£o do evento foi concluÃ­do.")
                            elif current_approval_data['decision'] == "Reprovar":
                                new_status = 'aguardando_classificador'
                                updates = {
                                    'status': new_status,
                                    'rejection_approval': {
                                        'decision': 'Reprovada',
                                        'rejected_by': user_username,
                                        'reason': approval_notes,
                                        'rejected_at': datetime.now().isoformat()
                                    },
                                    'approver': None
                                }
                                update_notification(notification['id'], updates)  # Atualiza no DB
                                add_history_entry(notification['id'], "NotificaÃ§Ã£o reprovada (AprovaÃ§Ã£o)",
                                                  user_name,
                                                  f"Reprovada superiormente. Motivo: {approval_notes[:150]}..." if len(
                                                      approval_notes) > 150 else f"Reprovada superiormente. Motivo: {approval_notes}")
                                st.warning(
                                    f"âš ï¸ NotificaÃ§Ã£o #{notification['id']} reprovada! Devolvida para revisÃ£o pelo classificador.")
                                st.info(
                                    "A notificaÃ§Ã£o foi movida para o status 'aguardando classificador' para que a equipe de classificaÃ§Ã£o possa revisar e redefinir o fluxo.")
                            # For both approve and reject paths, perform cleanup and rerun
                            # (update_notification is already called inside if/elif blocks)
                            st.session_state.approval_form_state.pop(notification['id'], None)
                            _clear_approval_form_state(notification['id'])
                            st.rerun() # CORREÃ‡ÃƒO: ForÃ§a o re-render

    with tab_closed_my_approval_notifs:
        st.markdown("### Minhas AprovaÃ§Ãµes Encerradas")

        if not closed_my_approval_notifications:
            st.info("âœ… NÃ£o hÃ¡ notificaÃ§Ãµes encerradas que vocÃª aprovou ou reprovou no momento.")
        else:
            st.info(
                f"Total de notificaÃ§Ãµes encerradas por vocÃª: {len(closed_my_approval_notifications)}.")
            search_query_app_closed = st.text_input(
                "ðŸ”Ž Buscar em Minhas AprovaÃ§Ãµes Encerradas (TÃ­tulo, DescriÃ§Ã£o, ID):",
                key="closed_app_notif_search_input",
                placeholder="Ex: 'aprovaÃ§Ã£o', 'reprovado', '456'"
            ).lower()
            filtered_closed_my_approval_notifications = []
            if search_query_app_closed:
                for notif in closed_my_approval_notifications:
                    if search_query_app_closed.isdigit() and int(
                            search_query_app_closed) == notif.get('id'):
                        filtered_closed_my_approval_notifications.append(notif)
                    elif (search_query_app_closed in notif.get('title', '').lower() or
                            search_query_app_closed in notif.get('description', '').lower()):
                        filtered_closed_my_approval_notifications.append(notif)
            else:
                filtered_closed_my_approval_notifications = closed_my_approval_notifications
            if not filtered_closed_my_approval_notifications:
                st.warning(
                    "âš ï¸ Nenhuma notificaÃ§Ã£o encontrada com os critÃ©rios de busca especificados em suas aprovaÃ§Ãµes encerradas.")
            else:
                filtered_closed_my_approval_notifications.sort(
                    key=lambda x: x.get('created_at', ''), reverse=True)
                st.markdown(
                    f"**NotificaÃ§Ãµes Encontradas ({len(filtered_closed_my_approval_notifications)})**:")
                for notification in filtered_closed_my_approval_notifications:
                    status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
                    created_at_str = notification.get('created_at', UI_TEXTS.text_na)
                    if created_at_str != UI_TEXTS.text_na:
                        try:
                            created_at_str = datetime.fromisoformat(created_at_str).strftime(
                                '%d/%m/%Y %H:%M:%S')
                        except ValueError:
                            pass
                    concluded_by = UI_TEXTS.text_na
                    if notification.get('conclusion') and notification['conclusion'].get(
                            'concluded_by'):
                        concluded_by = notification['conclusion']['concluded_by']
                    elif notification.get('approval') and (notification.get('approval') or {}).get(
                            'approved_by'):
                        concluded_by = (notification.get('approval') or {}).get('approved_by')
                    elif notification.get('rejection_classification') and (
                            notification.get('rejection_classification') or {}).get(
                        'classified_by'):
                        concluded_by = (notification.get('rejection_classification') or {}).get(
                            'classified_by')
                    elif notification.get('rejection_approval') and (
                            notification.get('rejection_approval') or {}).get(
                        'rejected_by'):
                        concluded_by = (notification.get('rejection_approval') or {}).get(
                            'rejected_by')
# Determinar o status do prazo para notificaÃ§Ãµes encerradas
                    classif_info = notification.get('classification', {})
                    deadline_date_str = classif_info.get('deadline_date')

                    # Acessa 'timestamp' de 'conclusion' de forma segura
                    concluded_timestamp_str = (notification.get('conclusion') or {}).get(
                        'timestamp')
                    # Verificar se a conclusÃ£o foi dentro ou fora do prazo
                    deadline_status = get_deadline_status(deadline_date_str,
                                                          concluded_timestamp_str)
                    card_class = ""
                    if deadline_status['class'] == "deadline-ontrack" or deadline_status[
                        'class'] == "deadline-duesoon":
                        card_class = "card-prazo-dentro"
                    elif deadline_status['class'] == "deadline-overdue":
                        card_class = "card-prazo-fora"
                    st.markdown(f"""
                                        <div class="notification-card {card_class}">
                                            <h4>#{notification.get('id', UI_TEXTS.text_na)} - {notification.get('title', UI_TEXTS.text_na)}</h4>
                                            <p><strong>Status Final:</strong> <span class="{status_class}">{notification.get('status', UI_TEXTS.text_na).replace('_', ' ').title()}</span></p>
                                            <p><strong>Encerrada por:</strong> {concluded_by} | <strong>Data de CriaÃ§Ã£o:</strong> {created_at_str}</p>
                                            <p><strong>Prazo:</strong> {deadline_status['text']}</p>
                                        </div>
                                        """, unsafe_allow_html=True)
                    with st.expander(
                            f"ðŸ‘ï¸ Visualizar Detalhes - NotificaÃ§Ã£o #{notification.get('id', UI_TEXTS.text_na)}"):
                        display_notification_full_details(notification,
                                                          st.session_state.user.get(
                                                              'id') if st.session_state.authenticated else None,
                                                          st.session_state.user.get(
                                                              'username') if st.session_state.authenticated else None)


@st_fragment
def show_admin():
    """Renderiza a pÃ¡gina de administraÃ§Ã£o."""
    if not check_permission('admin'):
        st.error("âŒ Acesso negado! VocÃª nÃ£o tem permissÃ£o de administrador.")
        return

    st.markdown("<h1 class='main-header'>âš™ï¸ AdministraÃ§Ã£o do Sistema</h1>",
                unsafe_allow_html=True)
    st.info(
        "Esta Ã¡rea permite gerenciar usuÃ¡rios, configurar o sistema e acessar ferramentas de desenvolvimento.")

    tab1, tab2, tab3, tab4 = st.tabs(
        ["ðŸ‘¥ UsuÃ¡rios", "ðŸ’¾ ConfiguraÃ§Ãµes e Dados", "ðŸ› ï¸ VisualizaÃ§Ã£o de Desenvolvimento",
         "â„¹ï¸ Sobre o Sistema"])

    with tab1:
        st.markdown("### ðŸ‘¥ Gerenciamento de UsuÃ¡rios")

        with st.expander("âž• Criar Novo UsuÃ¡rio", expanded=False):
            with st.form("create_user_form_refactored", clear_on_submit=True):
                st.markdown("**ðŸ“ Dados do Novo UsuÃ¡rio**")
                col1, col2 = st.columns(2)
                with col1:
                    new_username = st.text_input("Nome de UsuÃ¡rio*",
                                                 placeholder="usuario.exemplo",
                                                 key="admin_new_username_form_refactored").strip()
                    new_password = st.text_input("Senha*", type="password",
                                                 key="admin_new_password_form_refactored",
                                                 placeholder="Senha segura").strip()
                    new_password_confirm = st.text_input("Repetir Senha*", type="password",
                                                         key="admin_new_password_confirm_form_refactored",
                                                         placeholder="Repita a senha").strip()
                with col2:
                    new_name = st.text_input("Nome Completo*", placeholder="Nome Sobrenome",
                                             key="admin_new_name_form_refactored").strip()
                    new_email = st.text_input("Email*", placeholder="usuario@hospital.com",
                                              key="admin_new_email_form_refactored").strip()

                available_roles_options = ["classificador", "executor", "aprovador", "admin"]
                instructional_roles_text = UI_TEXTS.multiselect_instruction_placeholder
                display_roles_options = [instructional_roles_text] + available_roles_options

                current_selected_roles_from_state = st.session_state.get(
                    "admin_new_roles_form_refactored", []
                )

                if instructional_roles_text in current_selected_roles_from_state and len(
                        current_selected_roles_from_state) > 1:
                    default_selection_for_display = [instructional_roles_text]
                elif not current_selected_roles_from_state:
                    default_selection_for_display = [instructional_roles_text]
                else:
                    default_selection_for_display = current_selected_roles_from_state

                new_roles_raw = st.multiselect(
                    UI_TEXTS.multiselect_user_roles_label,
                    options=display_roles_options,
                    default=default_selection_for_display,
                    help="Selecione uma ou mais funÃ§Ãµes para o novo usuÃ¡rio.",
                    key="admin_new_roles_form_refactored"
                )

                st.markdown("<span class='required-field'>* Campos obrigatÃ³rios</span>",
                            unsafe_allow_html=True)
                submit_button = st.form_submit_button("âž• Criar UsuÃ¡rio",
                                                      use_container_width=True)

            if submit_button:
                username_state = st.session_state.get("admin_new_username_form_refactored",
                                                      "").strip()
                password_state = st.session_state.get("admin_new_password_form_refactored",
                                                      "").strip()
                password_confirm_state = st.session_state.get(
                    "admin_new_password_confirm_form_refactored", "").strip()
                name_state = st.session_state.get("admin_new_name_form_refactored", "").strip()
                email_state = st.session_state.get("admin_new_email_form_refactored",
                                                   "").strip()
                roles_to_save = [role for role in new_roles_raw if
                                 role != instructional_roles_text]

                validation_errors = []
                if not username_state: validation_errors.append(
                    "Nome de UsuÃ¡rio Ã© obrigatÃ³rio.")
                if not password_state: validation_errors.append("Senha Ã© obrigatÃ³ria.")
                if password_state != password_confirm_state: validation_errors.append(
                    "As senhas nÃ£o coincidem.")
                if not name_state: validation_errors.append("Nome Completo Ã© obrigatÃ³rio.")
                if not email_state: validation_errors.append("Email Ã© obrigatÃ³rio.")
                if not roles_to_save: validation_errors.append(
                    "Pelo menos uma FunÃ§Ã£o Ã© obrigatÃ³ria.")

                if validation_errors:
                    st.error("âš ï¸ **Por favor, corrija os seguintes erros:**")
                    for error in validation_errors: st.warning(error)
                else:
                    user_data = {'username': username_state, 'password': password_state,
                                 'name': name_state,
                                 'email': email_state, 'roles': roles_to_save}
                    if create_user(user_data):
                        st.success(f"âœ… UsuÃ¡rio '{name_state}' criado com sucesso!\n\n")
                        st.rerun()
                    else:
                        st.error("âŒ Nome de usuÃ¡rio jÃ¡ existe. Por favor, escolha outro.")

        st.markdown("### ðŸ“‹ UsuÃ¡rios Cadastrados no Sistema")
        users = load_users()
        if users:
            if 'editing_user_id' not in st.session_state:
                st.session_state.editing_user_id = None

            # Filtra o prÃ³prio usuÃ¡rio logado da lista para ediÃ§Ã£o
            users_to_display = [u for u in users if u['id'] != st.session_state.editing_user_id]
            users_to_display.sort(key=lambda x: x.get('name', ''))

            for user in users_to_display:
                status_icon = "ðŸŸ¢" if user.get('active', True) else "ðŸ”´"

                expander_key = f"user_expander_{user.get('id', UI_TEXTS.text_na)}"
                with st.expander(
                        f"**{user.get('name', UI_TEXTS.text_na)}** ({user.get('username', UI_TEXTS.text_na)}) {status_icon}",
                        expanded=(st.session_state.editing_user_id == user['id'])):
                    col_display, col_actions = st.columns([0.7, 0.3])

                    with col_display:
                        st.write(f"**ID:** {user.get('id', UI_TEXTS.text_na)}")
                        st.write(f"**Email:** {user.get('email', UI_TEXTS.text_na)}")
                        st.write(
                            f"**FunÃ§Ãµes:** {', '.join(user.get('roles', [UI_TEXTS.text_na]))}")
                        st.write(
                            f"**Status:** {'âœ… Ativo' if user.get('active', True) else 'âŒ Inativo'}")
                        created_at_str = user.get('created_at', UI_TEXTS.text_na)
                        if created_at_str != UI_TEXTS.text_na:
                            try:
                                created_at_str = datetime.fromisoformat(
                                    created_at_str).strftime('%d/%m/%Y %H:%M:%S')
                            except ValueError:
                                pass
                        st.write(f"**Criado em:** {created_at_str}")

                    with col_actions:
                        # Regras para botÃµes de aÃ§Ã£o:
                        # 1. Admin inicial (ID 1) nÃ£o pode ser editado/desativado por seguranÃ§a
                        # 2. O usuÃ¡rio logado nÃ£o pode editar/desativar a prÃ³pria conta
                        if user.get('id') != 1 and user.get('id') != st.session_state.user.get(
                                'id'):
                            if st.button("âœï¸ Editar",
                                         key=f"edit_user_{user.get('id', UI_TEXTS.text_na)}",
                                         use_container_width=True):
                                st.session_state.editing_user_id = user['id']
                                # Pre-popula os campos de ediÃ§Ã£o
                                st.session_state[f"edit_name_{user['id']}"] = user.get('name',
                                                                                       '')
                                st.session_state[f"edit_email_{user['id']}"] = user.get('email',
                                                                                        '')
                                st.session_state[f"edit_roles_{user['id']}"] = user.get('roles',
                                                                                        [])
                                st.session_state[f"edit_active_{user['id']}"] = user.get(
                                    'active', True)
                                st.rerun()

                            action_text = "ðŸ”’ Desativar" if user.get('active',
                                                                    True) else "ðŸ”“ Ativar"
                            if st.button(action_text,
                                         key=f"toggle_user_{user.get('id', UI_TEXTS.text_na)}",
                                         use_container_width=True):
                                current_user_status = user.get('active', True)
                                updates = {'active': not current_user_status}
                                updated_user = update_user(user['id'], updates)
                                if updated_user:
                                    status_msg = "desativado" if not updated_user[
                                        'active'] else "ativado"
                                    st.success(
                                        f"âœ… UsuÃ¡rio '{user.get('name', UI_TEXTS.text_na)}' {status_msg} com sucesso.")
                                    st.rerun() 
                                else:
                                    st.error("âŒ Erro ao atualizar status do usuÃ¡rio.")
                        elif user.get('id') == 1:
                            st.info("ðŸ‘‘ Admin inicial nÃ£o editÃ¡vel.")
                        elif user.get('id') == st.session_state.user.get('id'):
                            st.info("ðŸ‘¤ VocÃª nÃ£o pode editar sua prÃ³pria conta.")
                            st.info(
                                "Para alterar sua senha ou dados, faÃ§a logout e use a opÃ§Ã£o de recuperaÃ§Ã£o de senha ou peÃ§a a outro admin para editar.")

            # FormulÃ¡rio de ediÃ§Ã£o flutuante para o usuÃ¡rio selecionado
            if st.session_state.editing_user_id:
                edited_user = next(
                    (u for u in users if u['id'] == st.session_state.editing_user_id), None)
                if edited_user:
                    st.markdown(
                        f"### âœï¸ Editando UsuÃ¡rio: {edited_user.get('name', UI_TEXTS.text_na)} ({edited_user.get('username', UI_TEXTS.text_na)})")
                    with st.form(key=f"edit_user_form_{edited_user['id']}",
                                 clear_on_submit=False):
                        st.text_input("Nome de UsuÃ¡rio", value=edited_user.get('username', ''),
                                      disabled=True)

                        edited_name = st.text_input("Nome Completo*",
                                                    value=st.session_state.get(
                                                        f"edit_name_{edited_user['id']}",
                                                        edited_user.get('name', '')),
                                                    key=f"edit_name_{edited_user['id']}_input").strip()
                        edited_email = st.text_input("Email*",
                                                     value=st.session_state.get(
                                                         f"edit_email_{edited_user['id']}",
                                                         edited_user.get('email', '')),
                                                     key=f"edit_email_{edited_user['id']}_input").strip()
                        available_roles = ["classificador", "executor", "aprovador", "admin"]
                        instructional_roles_text = UI_TEXTS.multiselect_instruction_placeholder
                        display_roles_options = [instructional_roles_text] + available_roles

                        current_edited_roles = st.session_state.get(
                            f"edit_roles_{edited_user['id']}_input",
                            edited_user.get('roles', []))

                        if instructional_roles_text in current_edited_roles and len(current_edited_roles) > 1:
                            default_edit_selection_for_display = [instructional_roles_text]
                        elif not current_edited_roles:
                            default_edit_selection_for_display = [instructional_roles_text]
                        else:
                            default_edit_selection_for_display = current_edited_roles

                        edited_roles_raw = st.multiselect(
                            UI_TEXTS.multiselect_user_roles_label,
                            options=display_roles_options,
                            default=default_edit_selection_for_display,
                            key=f"edit_roles_{edited_user['id']}_input"
                        )
                        edited_roles = [role for role in edited_roles_raw if
                                        role != instructional_roles_text]
                        edited_active = st.checkbox("Ativo",
                                                    value=st.session_state.get(
                                                        f"edit_active_{edited_user['id']}",
                                                        edited_user.get('active', True)),
                                                    key=f"edit_active_{edited_user['id']}_input")
                        st.markdown("---")
                        st.markdown("#### Alterar Senha (Opcional)")
                        new_password = st.text_input("Nova Senha", type="password",
                                                     key=f"new_password_{edited_user['id']}_input").strip()
                        new_password_confirm = st.text_input("Repetir Nova Senha",
                                                             type="password",
                                                             key=f"new_password_confirm_{edited_user['id']}_input").strip()

                        st.markdown(
                            "<span class='required-field'>* Campos obrigatÃ³rios (para nome, email e funÃ§Ãµes)</span>",
                            unsafe_allow_html=True)

                        col_edit_submit, col_edit_cancel = st.columns(2)
                        with col_edit_submit:
                            submit_edit_button = st.form_submit_button("ðŸ’¾ Salvar AlteraÃ§Ãµes",
                                                                       use_container_width=True)
                        with col_edit_cancel:
                            cancel_edit_button = st.form_submit_button("âŒ Cancelar EdiÃ§Ã£o",
                                                                       use_container_width=True)

                        if submit_edit_button:
                            edit_validation_errors = []
                            if not edited_name: edit_validation_errors.append(
                                "Nome Completo Ã© obrigatÃ³rio.")
                            if not edited_email: edit_validation_errors.append(
                                "Email Ã© obrigatÃ³rio.")
                            if not edited_roles: edit_validation_errors.append(
                                "Pelo menos uma FunÃ§Ã£o Ã© obrigatÃ³ria.")
                            if new_password:
                                if new_password != new_password_confirm:
                                    edit_validation_errors.append(
                                        "As novas senhas nÃ£o coincidem.")
                                if len(new_password) < 6:
                                    edit_validation_errors.append(
                                        "A nova senha deve ter no mÃ­nimo 6 caracteres.")

                            if edit_validation_errors:
                                st.error("âš ï¸ **Por favor, corrija os seguintes erros:**")
                                for error in edit_validation_errors: st.warning(error)
                            else:
                                updates_to_apply = {
                                    'name': edited_name,
                                    'email': edited_email,
                                    'roles': edited_roles,
                                    'active': edited_active
                                }
                                if new_password:
                                    updates_to_apply['password'] = new_password

                                updated_user_final = update_user(edited_user['id'],
                                                                 updates_to_apply)
                                if updated_user_final:
                                    st.success(
                                        f"âœ… UsuÃ¡rio '{updated_user_final.get('name', UI_TEXTS.text_na)}' atualizado com sucesso!")
                                    st.session_state.editing_user_id = None
                                    st.rerun()
                                else:
                                    st.error("âŒ Erro ao salvar alteraÃ§Ãµes do usuÃ¡rio.")

                        if cancel_edit_button:
                            st.session_state.editing_user_id = None
                            st.rerun()

        else:
            st.info("ðŸ“‹ Nenhum usuÃ¡rio cadastrado no sistema.")

    with tab2:
        st.markdown("### ðŸ’¾ ConfiguraÃ§Ãµes e Gerenciamento de Dados")
        st.warning(
            "âš ï¸ Esta seÃ§Ã£o Ã© destinada a desenvolvedores para visualizar a estrutura completa dos dados. NÃ£o Ã© para uso operacional normal.")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### ðŸ’¾ Backup dos Dados")
            st.info(
                "Gera um arquivo JSON contendo todos os dados de usuÃ¡rios e notificaÃ§Ãµes cadastrados no sistema.")
            if st.button("ðŸ“¥ Gerar Backup (JSON)", use_container_width=True,
                         key="generate_backup_btn"):
                # Carrega todos os dados do banco para o backup
                all_users_for_backup = load_users()
                all_notifications_for_backup = load_notifications()

                # Garante que os dados do JSONB sejam dicionÃ¡rios e nÃ£o strings JSON
                # E que os objetos datetime sejam strings ISO formatadas
                def prepare_for_json(data):
                    if isinstance(data, dict):
                        return {k: prepare_for_json(v) for k, v in data.items()}
                    elif isinstance(data, list):
                        return [prepare_for_json(elem) for elem in data]
                    elif isinstance(data, (datetime, dt_date_class, dt_time_class)):
                        return data.isoformat()
                    elif isinstance(data, (int, float, str, bool)) or data is None:
                        return data
                    else:
                        # Para JSONB que jÃ¡ vem como dict/list
                        try:
                            # Se for uma string que parece JSON, tentar carregar
                            if isinstance(data, str) and (
                                    data.strip().startswith('{') or data.strip().startswith(
                                '[')):
                                return json.loads(data)
                        except json.JSONDecodeError:
                            pass
                        return str(data)  # Fallback para qualquer outro tipo

                backup_data = {
                    'users': [prepare_for_json(u) for u in all_users_for_backup],
                    'notifications': [prepare_for_json(n) for n in
                                      all_notifications_for_backup],
                    'backup_date': datetime.now().isoformat(),
                    'version': '1.1-db-based'
                }
                backup_json = json.dumps(backup_data, indent=2, ensure_ascii=False)
                st.download_button(
                    label="â¬‡ï¸ Baixar Backup Agora", data=backup_json,
                    file_name=f"hospital_notif_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json", use_container_width=True, key="download_backup_btn"
                )
        with col2:
            st.markdown("#### ðŸ“¤ Restaurar Dados")
            st.info(
                "Carrega um arquivo JSON de backup para restaurar dados de usuÃ¡rios e notificaÃ§Ãµes. **Isso sobrescreverÃ¡ os dados existentes!**")
            uploaded_file = st.file_uploader("Selecione um arquivo de backup (formato JSON):",
                                             type=['json'],
                                             key="admin_restore_file_uploader")
            if uploaded_file:
                with st.form("restore_form", clear_on_submit=False):
                    submit_button = st.form_submit_button("ðŸ”„ Restaurar Dados",
                                                          use_container_width=True,
                                                          key="restore_data_btn")
                    if submit_button:
                        try:
                            uploaded_file_content = st.session_state.admin_restore_file_uploader.getvalue().decode(
                                'utf8')
                            backup_data = json.loads(uploaded_file_content)
                            if isinstance(backup_data,
                                          dict) and 'users' in backup_data and 'notifications' in backup_data:
                                # RestauraÃ§Ã£o: Apaga tudo e reinsere (simples, mas destrutivo)
                                conn = get_db_connection()
                                cur = conn.cursor()
                                try:
                                    # Desabilita triggers de TSVECTOR para restauraÃ§Ã£o massiva
                                    cur.execute(
                                        "ALTER TABLE notifications DISABLE TRIGGER trg_notifications_search_vector;")
                                    # Limpa tabelas em ordem inversa de dependÃªncia
                                    cur.execute(
                                        "TRUNCATE TABLE notification_actions RESTART IDENTITY CASCADE;")
                                    cur.execute(
                                        "TRUNCATE TABLE notification_history RESTART IDENTITY CASCADE;")
                                    cur.execute(
                                        "TRUNCATE TABLE notification_attachments RESTART IDENTITY CASCADE;")
                                    cur.execute(
                                        "TRUNCATE TABLE notifications RESTART IDENTITY CASCADE;")
                                    cur.execute(
                                        "TRUNCATE TABLE users RESTART IDENTITY CASCADE;")

                                    # Restaura usuÃ¡rios
                                    for user_data in backup_data['users']:
                                        cur.execute("""
                                                    INSERT INTO users (id, username, password_hash, name, email, roles, active, created_at)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                                """, (
                                            user_data.get('id'),
                                            user_data.get('username'),
                                            user_data.get('password'),  # JÃ¡ Ã© o hash
                                            user_data.get('name'),
                                            user_data.get('email'),
                                            user_data.get('roles', []),
                                            user_data.get('active', True),
                                            datetime.fromisoformat(
                                                user_data['created_at']) if user_data.get(
                                                'created_at') else datetime.now()
                                        ))
                                    # Ajusta a sequÃªncia SERIAL para o prÃ³ximo ID disponÃ­vel
                                    cur.execute(
                                        f"SELECT setval('users_id_seq', (SELECT MAX(id) FROM users));")

                                    # Restaura notificaÃ§Ãµes e sub-dados (attachments, actions, history)
                                    for notif_data in backup_data['notifications']:
                                        # Converte datas/tempos para o tipo correto para o DB
                                        occurrence_date = datetime.fromisoformat(notif_data[
                                                                                     'occurrence_date']).date() if notif_data.get(
                                            'occurrence_date') else None
                                        occurrence_time = datetime.fromisoformat(notif_data[
                                                                                     'occurrence_time']).time() if notif_data.get(
                                            'occurrence_time') else None
                                        created_at = datetime.fromisoformat(
                                            notif_data['created_at']) if notif_data.get(
                                            'created_at') else datetime.now()
                                        # Insere notificaÃ§Ã£o principal
                                        cur.execute("""
                                                    INSERT INTO notifications (
                                                        id, title, description, location, occurrence_date, occurrence_time,
                                                        reporting_department, reporting_department_complement, notified_department,
                                                        notified_department_complement, event_shift, immediate_actions_taken,
                                                        immediate_action_description, patient_involved, patient_id, patient_outcome_obito,
                                                        additional_notes, status, created_at,
                                                        classification, rejection_classification, review_execution, approval,
                                                        rejection_approval, rejection_execution_review, conclusion,
                                                        executors, approver
                                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                    %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                """, (
                                            notif_data.get('id'),
                                            notif_data.get('title'),
                                            notif_data.get('description'),
                                            notif_data.get('location'),
                                            occurrence_date,
                                            occurrence_time,
                                            notif_data.get('reporting_department'),
                                            notif_data.get('reporting_department_complement'),
                                            notif_data.get('notified_department'),
                                            notif_data.get('notified_department_complement'),
                                            notif_data.get('event_shift'),
                                            notif_data.get('immediate_actions_taken'),
                                            # JÃ¡ Ã© boolean
                                            notif_data.get('immediate_action_description'),
                                            notif_data.get('patient_involved'),  # JÃ¡ Ã© boolean
                                            notif_data.get('patient_id'),
                                            notif_data.get('patient_outcome_obito'),
                                            # JÃ¡ Ã© boolean
                                            notif_data.get('additional_notes'),
                                            notif_data.get('status'),
                                            created_at,
                                            json.dumps(notif_data.get(
                                                'classification')) if notif_data.get(
                                                'classification') else None,
                                            json.dumps(notif_data.get(
                                                'rejection_classification')) if notif_data.get(
                                                'rejection_classification') else None,
                                            json.dumps(notif_data.get(
                                                'review_execution')) if notif_data.get(
                                                'review_execution') else None,
                                            json.dumps(
                                                notif_data.get('approval')) if notif_data.get(
                                                'approval') else None,
                                            json.dumps(notif_data.get(
                                                'rejection_approval')) if notif_data.get(
                                                'rejection_approval') else None,
                                            json.dumps(notif_data.get(
                                                'rejection_execution_review')) if notif_data.get(
                                                'rejection_execution_review') else None,
                                            json.dumps(
                                                notif_data.get('conclusion')) if notif_data.get(
                                                'conclusion') else None,
                                            notif_data.get('executors', []),  # Array de IDs
                                            notif_data.get('approver')
                                        ))

                                        # Restaura anexos
                                        for att in notif_data.get('attachments', []):
                                            cur.execute("""
                                                        INSERT INTO notification_attachments (notification_id, unique_name, original_name, uploaded_at)
                                                        VALUES (%s, %s, %s, %s)
                                                    """, (
                                                notif_data['id'], att.get('unique_name'),
                                                att.get('original_name'),
                                                datetime.fromisoformat(
                                                    att['uploaded_at']) if att.get(
                                                    'uploaded_at') else datetime.now()
                                            ))

                                        # Restaura histÃ³rico
                                        for hist in notif_data.get('history', []):
                                            cur.execute("""
                                                        INSERT INTO notification_history (notification_id, action_type, performed_by, action_timestamp, details)
                                                        VALUES (%s, %s, %s, %s, %s)
                                                    """, (
                                                notif_data['id'], hist.get('action'),
                                                hist.get('user'),
                                                datetime.fromisoformat(
                                                    hist['timestamp']) if hist.get(
                                                    'timestamp') else datetime.now(),
                                                hist.get('details')
                                            ))

                                        # Restaura aÃ§Ãµes
                                        for action_item in notif_data.get('actions', []):
                                            cur.execute("""
                                                        INSERT INTO notification_actions (notification_id, executor_id, executor_name, description, action_timestamp, final_action_by_executor, evidence_description, evidence_attachments)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                                    """, (
                                                notif_data['id'],
                                                action_item.get('executor_id'),
                                                action_item.get('executor_name'),
                                                action_item.get('description'),
                                                datetime.fromisoformat(action_item[
                                                                           'timestamp']) if action_item.get(
                                                    'timestamp') else datetime.now(),
                                                action_item.get('final_action_by_executor',
                                                                False),
                                                action_item.get('evidence_description'),
                                                json.dumps(action_item.get(
                                                    'evidence_attachments')) if action_item.get(
                                                    'evidence_attachments') else None
                                            ))
                                    # Ajusta a sequÃªncia SERIAL para o prÃ³ximo ID disponÃ­vel
                                    cur.execute(
                                        f"SELECT setval('notifications_id_seq', (SELECT MAX(id) FROM notifications));")

                                    conn.commit()
                                    st.success(
                                        "âœ… Dados restaurados com sucesso a partir do arquivo!\n\n")
                                    st.info(
                                        "A pÃ¡gina serÃ¡ recarregada para refletir os dados restaurados.")
                                    st.session_state.pop('admin_restore_file_uploader', None)
                                    _reset_form_state()
                                    st.session_state.initial_classification_state = {}
                                    st.session_state.review_classification_state = {}
                                    st.session_state.current_initial_classification_id = None
                                    st.session_state.current_review_classification_id = None
                                    st.session_state.approval_form_state = {}
                                    st.rerun()
                                except psycopg2.Error as e:
                                    conn.rollback()
                                    st.error(
                                        f"âŒ Erro ao restaurar dados no banco de dados: {e}")
                                finally:
                                    # Habilita triggers de TSVECTOR novamente
                                    cur.execute(
                                        "ALTER TABLE notifications ENABLE TRIGGER trg_notifications_search_vector;")
                                    cur.close()
                                    conn.close()
                            else:
                                st.error(
                                    "âŒ Arquivo de backup invÃ¡lido. O arquivo JSON nÃ£o contÃ©m a estrutura esperada (chaves 'users' e 'notifications').")
                        except json.JSONDecodeError:
                            st.error(
                                "âŒ Erro ao ler o arquivo JSON. Certifique-se de que Ã© um arquivo JSON vÃ¡lido.")
                        except Exception as e:
                            st.error(
                                f"âŒ Ocorreu um erro inesperado ao restaurar os dados: {str(e)}")

    with tab3:
        st.markdown("### ðŸ› ï¸ VisualizaÃ§Ã£o de Desenvolvimento e Debug")
        st.warning(
            "âš ï¸ Esta seÃ§Ã£o Ã© destinada a desenvolvedores para visualizar a estrutura completa dos dados. NÃ£o Ã© para uso operacional normal.")
        notifications = load_notifications()  # Carrega do DB
        if notifications:
            selected_notif_display_options = [UI_TEXTS.selectbox_default_admin_debug_notif] + [
                f"#{n.get('id', UI_TEXTS.text_na)} - {n.get('title', UI_TEXTS.text_na)} (Status: {n.get('status', UI_TEXTS.text_na).replace('_', ' ')})"
                for n in notifications
            ]
            selectbox_key_debug = "admin_debug_notif_select_refactored"
            if selectbox_key_debug not in st.session_state or st.session_state[
                selectbox_key_debug] not in selected_notif_display_options:
                st.session_state[selectbox_key_debug] = selected_notif_display_options[0]

            selected_notif_display = st.selectbox(
                "Selecionar notificaÃ§Ã£o para anÃ¡lise detalhada (JSON):",
                options=selected_notif_display_options,
                index=selected_notif_display_options.index(
                    st.session_state[selectbox_key_debug]) if st.session_state[
                                                                  selectbox_key_debug] in selected_notif_display_options else 0,
                key=selectbox_key_debug
            )
            if selected_notif_display != UI_TEXTS.selectbox_default_admin_debug_notif:
                try:
                    parts = selected_notif_display.split('#')
                    if len(parts) > 1:
                        id_part = parts[1].split(' -')[0]
                        notif_id = int(id_part)
                        notification = next(
                            (n for n in notifications if n.get('id') == notif_id), None)
                        if notification:
                            st.markdown("#### Dados Completos da NotificaÃ§Ã£o (JSON)")
                            # Para exibir JSON puro e bonito no Streamlit, garantimos que todas as datas/tempos e JSONB
                            # jÃ¡ estejam formatados como strings ISO e dicionÃ¡rios/listas Python, respectivamente.
                            # load_notifications jÃ¡ faz grande parte disso.
                            st.json(notification)
                        else:
                            st.error("âŒ NotificaÃ§Ã£o nÃ£o encontrada.")
                    else:
                        st.error("âŒ Formato de seleÃ§Ã£o invÃ¡lido.")
                except (IndexError, ValueError) as e:
                    st.error(f"âŒ Erro ao processar seleÃ§Ã£o ou encontrar notificaÃ§Ã£o: {e}")
        else:
            st.info("ðŸ“‹ Nenhuma notificaÃ§Ã£o encontrada para anÃ¡lise de desenvolvimento.")

    with tab4:
        st.markdown("### â„¹ï¸ InformaÃ§Ãµes do Sistema")
        st.markdown("#### Detalhes do Portal")
        st.write(f"**VersÃ£o do Portal:** 2.0.1")
        st.write(f"**Data da Ãšltima AtualizaÃ§Ã£o:** 19/07/2025")
        st.write(f"**Desenvolvido por:** FIA Softworks")
        st.markdown("#### Contato")
        st.markdown("##### Suporte TÃ©cnico:")
        st.write(f"**Email:** borges@fiasoftworks.com.br")
        

@st_fragment
def show_dashboard():
    # A funÃ§Ã£o force_rerun_dashboard_search nÃ£o Ã© mais necessÃ¡ria aqui
    # def force_rerun_dashboard_search():
    #     st.rerun()

    if not check_permission('admin') and not check_permission('classificador'):
        st.error("âŒ Acesso negado! VocÃª nÃ£o tem permissÃ£o para visualizar o dashboard.")
        return
    """
    Renderiza um dashboard abrangente para visualizaÃ§Ã£o de notificaÃ§Ãµes,
    incluindo mÃ©tricas chave, grÃ¡ficos e uma lista detalhada, filtrÃ¡vel,
    pesquisÃ¡vel e paginada de notificaÃ§Ãµes.
    """
    st.markdown("<h1 class='main-header'>   Dashboard de NotificaÃ§Ãµes</h1>",
                unsafe_allow_html=True)

    all_notifications = load_notifications()  # Carrega do DB
    if not all_notifications:
        st.warning(
            "âš ï¸ Nenhuma notificaÃ§Ã£o encontrada para exibir no dashboard. Comece registrando uma nova notificaÃ§Ã£o.")
        return

    # Converte a lista de notificaÃ§Ãµes em um DataFrame pandas para facilitar a manipulaÃ§Ã£o
    df_notifications = pd.DataFrame(all_notifications)
    df_notifications['created_at_dt'] = pd.to_datetime(df_notifications['created_at'])
    df_notifications['occurrence_date_dt'] = pd.to_datetime(df_notifications['occurrence_date'])

    # Define categorias de status para grÃ¡ficos
    completed_statuses = ['aprovada', 'concluida']
    rejected_statuses = ['rejeitada', 'reprovada']

    # Aba para VisÃ£o Geral e Lista Detalhada (conteÃºdo existente)
    # Aba para Indicadores e GrÃ¡ficos (novo conteÃºdo)
    tab_overview_list, tab_indicators = st.tabs(
        ["ðŸ“Š VisÃ£o Geral e Lista", "ðŸ“ˆ Indicadores e GrÃ¡ficos"])

    with tab_overview_list:
        st.info("VisÃ£o geral e detalhada de todas as notificaÃ§Ãµes registradas no sistema.")

        st.markdown("### VisÃ£o Geral e MÃ©tricas Chave")
        total = len(all_notifications)
        pending_classif = len(
            [n for n in all_notifications if n.get('status') == "pendente_classificacao"])
        in_progress_statuses = ['classificada', 'em_execucao', 'aguardando_classificador',
                                'aguardando_aprovacao', 'revisao_classificador_execucao']
        in_progress = len(
            [n for n in all_notifications if n.get('status') in in_progress_statuses])
        completed = len([n for n in all_notifications if n.get('status') in completed_statuses])
        rejected = len([n for n in all_notifications if n.get('status') in rejected_statuses])

        col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
        with col_m1:
            st.markdown(f"<div class='metric-card'><h4>Total</h4><p>{total}</p></div>",
                        unsafe_allow_html=True)
        with col_m2:
            st.markdown(
                f"<div class='metric-card'><h4>Pendente Classif.</h4><p>{pending_classif}</p></div>",
                unsafe_allow_html=True)
        with col_m3:
            st.markdown(
                f"<div class='metric-card'><h4>Em Andamento</h4><p>{in_progress}</p></div>",
                unsafe_allow_html=True)
        with col_m4:
            st.markdown(f"<div class='metric-card'><h4>ConcluÃ­das</h4><p>{completed}</p></div>",
                        unsafe_allow_html=True)
        with col_m5:
            st.markdown(f"<div class='metric-card'><h4>Rejeitadas</h4><p>{rejected}</p></div>",
                        unsafe_allow_html=True)

        st.markdown("---")

        st.markdown("### GrÃ¡ficos de TendÃªncia e DistribuiÃ§Ã£o")
        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("#### DistribuiÃ§Ã£o de NotificaÃ§Ãµes por Status")
            status_mapping = {
                'pendente_classificacao': 'Pendente Classif. Inicial',
                'classificada': 'Classificada (Aguardando Exec.)',
                'em_execucao': 'Em ExecuÃ§Ã£o',
                'revisao_classificador_execucao': 'Aguardando RevisÃ£o Exec.',
                'aguardando_classificador': 'Aguardando Classif. (RevisÃ£o)',
                'aguardando_aprovacao': 'Aguardando AprovaÃ§Ã£o',
                'aprovada': 'ConcluÃ­da (Aprovada)',
                'rejeitada': 'Rejeitada (Classif. Inicial)',
                'reprovada': 'Reprovada (AprovaÃ§Ã£o)'
            }
            status_count = {}
            for notification in all_notifications:
                status = notification.get('status', UI_TEXTS.text_na)
                mapped_status = status_mapping.get(status, status)
                status_count[mapped_status] = status_count.get(mapped_status, 0) + 1

            if status_count:
                status_df = pd.DataFrame(list(status_count.items()),
                                         columns=['Status', 'Quantidade'])
                status_order = [status_mapping.get(s) for s in
                                ['pendente_classificacao', 'classificada', 'em_execucao',
                                 'revisao_classificador_execucao',
                                 'aguardando_classificador',
                                 'aguardando_aprovacao', 'aprovada', 'rejeitada',
                                 'reprovada']]
                status_order = [s for s in status_order if
                                s and s in status_df['Status'].tolist()]
                if status_order:
                    status_df['Status'] = pd.Categorical(status_df['Status'],
                                                         categories=status_order, ordered=True)
                    status_df = status_df.sort_values('Status')
                st.bar_chart(status_df.set_index('Status'))
            else:
                st.info("Nenhum dado de status para gerar o grÃ¡fico.")

        with col_chart2:
            st.markdown("#### NotificaÃ§Ãµes Criadas ao Longo do Tempo")
            if not df_notifications.empty:
                df_notifications_copy = df_notifications.copy()  # Cria uma cÃ³pia para evitar SettingWithCopyWarning
                df_notifications_copy['month_year'] = df_notifications_copy[
                    'created_at_dt'].dt.to_period('M').astype(
                    str)
                monthly_counts = df_notifications_copy.groupby('month_year').size().reset_index(
                    name='count')
                monthly_counts['month_year'] = pd.to_datetime(monthly_counts['month_year'])
                monthly_counts = monthly_counts.sort_values('month_year')
                monthly_counts['month_year'] = monthly_counts['month_year'].dt.strftime(
                    '%Y-%m')

                st.line_chart(monthly_counts.set_index('month_year'))
            else:
                st.info("Nenhum dado para gerar o grÃ¡fico de tendÃªncia.")

        st.markdown("---")

        st.markdown("### Lista Detalhada de NotificaÃ§Ãµes")

        col_filters1, col_filters2, col_filters3 = st.columns(3)

        all_option_text = UI_TEXTS.multiselect_all_option

        if 'dashboard_filter_status' not in st.session_state: st.session_state.dashboard_filter_status = [
            all_option_text]
        if 'dashboard_filter_nnc' not in st.session_state: st.session_state.dashboard_filter_nnc = [
            all_option_text]
        if 'dashboard_filter_priority' not in st.session_state: st.session_state.dashboard_filter_priority = [
            all_option_text]
        if 'dashboard_filter_date_start' not in st.session_state: st.session_state.dashboard_filter_date_start = None
        if 'dashboard_filter_date_end' not in st.session_state: st.session_state.dashboard_filter_date_end = None
        
        # Inicializa dashboard_search_query_input para evitar KeyError
        if 'dashboard_search_query_input' not in st.session_state:
            st.session_state.dashboard_search_query_input = ""
            
        # NÃ£o Ã© mais necessÃ¡rio inicializar dashboard_search_query aqui, pois serÃ¡ derivado de dashboard_search_query_input
        # if 'dashboard_search_query' not in st.session_state: st.session_state.dashboard_search_query = ""
            
        if 'dashboard_sort_column' not in st.session_state: st.session_state.dashboard_sort_column = 'created_at'
        if 'dashboard_sort_ascending' not in st.session_state: st.session_state.dashboard_sort_ascending = False

        with col_filters1:
            all_status_options_keys = list(status_mapping.keys())
            display_status_options_with_all = [all_option_text] + all_status_options_keys

            current_status_selection_raw = st.session_state.get(
                "dashboard_filter_status_select", [all_option_text])
            if all_option_text in current_status_selection_raw and len(
                    current_status_selection_raw) > 1:
                default_status_selection_for_display = [all_option_text]
            elif not current_status_selection_raw:
                default_status_selection_for_display = [all_option_text]
            else:
                default_status_selection_for_display = current_status_selection_raw

            st.session_state.dashboard_filter_status = st.multiselect(
                UI_TEXTS.multiselect_filter_status_label,
                options=display_status_options_with_all,
                format_func=lambda x: status_mapping.get(x, x.replace('_', ' ').title()),
                default=default_status_selection_for_display,
                key="dashboard_filter_status_select"
            )
            if all_option_text in st.session_state.dashboard_filter_status and len(
                    st.session_state.dashboard_filter_status) > 1:
                st.session_state.dashboard_filter_status = [all_option_text]
            elif not st.session_state.dashboard_filter_status:
                st.session_state.dashboard_filter_status = [all_option_text]

            applied_status_filters = [s for s in st.session_state.dashboard_filter_status if
                                      s != all_option_text]
            all_nnc_options = FORM_DATA.classificacao_nnc
            display_nnc_options_with_all = [all_option_text] + all_nnc_options
            current_nnc_selection_raw = st.session_state.get("dashboard_filter_nnc_select",
                                                             [all_option_text])
            if all_option_text in current_nnc_selection_raw and len(
                    current_nnc_selection_raw) > 1:
                default_nnc_selection_for_display = [all_option_text]
            elif not current_nnc_selection_raw:
                default_nnc_selection_for_display = [all_option_text]
            else:
                default_nnc_selection_for_display = current_nnc_selection_raw

            st.session_state.dashboard_filter_nnc = st.multiselect(
                UI_TEXTS.multiselect_filter_nnc_label,
                options=display_nnc_options_with_all,
                default=default_nnc_selection_for_display,
                key="dashboard_filter_nnc_select"
            )
            if all_option_text in st.session_state.dashboard_filter_nnc and len(
                    st.session_state.dashboard_filter_nnc) > 1:
                st.session_state.dashboard_filter_nnc = [all_option_text]
            elif not st.session_state.dashboard_filter_nnc:
                st.session_state.dashboard_filter_nnc = [all_option_text]
            applied_nnc_filters = [n for n in st.session_state.dashboard_filter_nnc if
                                   n != all_option_text]

        with col_filters2:
            all_priority_options = FORM_DATA.prioridades
            display_priority_options_with_all = [all_option_text] + all_priority_options
            current_priority_selection_raw = st.session_state.get(
                "dashboard_filter_priority_select", [all_option_text])
            if all_option_text in current_priority_selection_raw and len(
                    current_priority_selection_raw) > 1:
                default_priority_selection_for_display = [all_option_text]
            elif not current_priority_selection_raw:
                default_priority_selection_for_display = [all_option_text]
            else:
                default_priority_selection_for_display = current_priority_selection_raw

            st.session_state.dashboard_filter_priority = st.multiselect(
                UI_TEXTS.multiselect_filter_priority_label,
                options=display_priority_options_with_all,
                default=default_priority_selection_for_display,
                key="dashboard_filter_priority_select"
            )
            if all_option_text in st.session_state.dashboard_filter_priority and len(
                    st.session_state.dashboard_filter_priority) > 1:
                st.session_state.dashboard_filter_priority = [all_option_text]
            elif not st.session_state.dashboard_filter_priority:
                st.session_state.dashboard_filter_priority = [all_option_text]
            applied_priority_filters = [p for p in st.session_state.dashboard_filter_priority if
                                        p != all_option_text]
            date_start_default = st.session_state.dashboard_filter_date_start or (
                df_notifications[
                    'created_at_dt'].min().date() if not df_notifications.empty else dt_date_class.today() - timedelta(
                    days=365)
            )
            date_end_default = st.session_state.dashboard_filter_date_end or (
                df_notifications[
                    'created_at_dt'].max().date() if not df_notifications.empty else dt_date_class.today()
            )

            st.session_state.dashboard_filter_date_start = st.date_input(
                "Data Inicial (CriaÃ§Ã£o):", value=date_start_default,
                key="dashboard_filter_date_start_input"
            )
            st.session_state.dashboard_filter_date_end = st.date_input(
                "Data Final (CriaÃ§Ã£o):", value=date_end_default,
                key="dashboard_filter_date_date_end_input"
            )

        with col_filters3:
            # O st.text_input armazena seu valor diretamente em st.session_state.dashboard_search_query_input
            # O parÃ¢metro 'value' serve para definir o valor inicial, que serÃ¡ o que estÃ¡ no session_state (ou vazio)
            st.text_input(
                "Buscar (TÃ­tulo, DescriÃ§Ã£o, ID):",
                value=st.session_state.dashboard_search_query_input, # Usa o valor que estÃ¡ no session_state para persistÃªncia
                key="dashboard_search_query_input", # A chave onde o valor atual do widget Ã© armazenado
                # on_change=force_rerun_dashboard_search # Removido para evitar o problema de "voltar"
            )
            # A variÃ¡vel usada para a lÃ³gica de filtragem Ã© atualizada APÃ“S o text_input ter seu valor persistido.
            # Essa linha serÃ¡ executada em cada rerun (seja por Enter, blur ou outro widget).
            st.session_state.dashboard_search_query = st.session_state.dashboard_search_query_input.lower()


            sort_options_map = {
                'ID': 'id',
                'Data de CriaÃ§Ã£o': 'created_at',
                'TÃ­tulo': 'title',
                'Local': 'location',
                'Prioridade': 'classification.prioridade',
            }
            sort_options_display = [UI_TEXTS.selectbox_sort_by_placeholder] + list(
                sort_options_map.keys())
            selected_sort_option_display = st.selectbox(
                UI_TEXTS.selectbox_sort_by_label,
                options=sort_options_display,
                index=0,
                key="dashboard_sort_column_select"
            )
            if selected_sort_option_display != UI_TEXTS.selectbox_sort_by_placeholder:
                st.session_state.dashboard_sort_column = sort_options_map[
                    selected_sort_option_display]
            else:
                st.session_state.dashboard_sort_column = 'created_at'

            st.session_state.dashboard_sort_ascending = st.checkbox(
                "Ordem Crescente", value=st.session_state.dashboard_sort_ascending,
                key="dashboard_sort_ascending_checkbox"
            )

        filtered_notifications = []
        for notification in all_notifications:
            match = True

            if applied_status_filters:
                if notification.get('status') not in applied_status_filters:
                    match = False
            if match and applied_nnc_filters:
                classif_nnc = notification.get('classification', {}).get('nnc')
                if classif_nnc not in applied_nnc_filters:
                    match = False
            if match and applied_priority_filters:
                priority = notification.get('classification', {}).get('prioridade')
                if priority not in applied_priority_filters:
                    match = False

            if match and st.session_state.dashboard_filter_date_start and st.session_state.dashboard_filter_date_end:
                created_at_date = datetime.fromisoformat(notification['created_at']).date()
                if not (
                        st.session_state.dashboard_filter_date_start <= created_at_date <= st.session_state.dashboard_filter_date_end):
                    match = False

            # Usa st.session_state.dashboard_search_query, que Ã© sempre o valor atualizado e em minÃºsculas
            if match and st.session_state.dashboard_search_query:
                query = st.session_state.dashboard_search_query
                search_fields = [
                    str(notification.get('id', '')).lower(),
                    notification.get('title', '').lower(),
                    notification.get('description', '').lower(),
                    notification.get('location', '').lower()
                ]
                if not any(query in field for field in search_fields):
                    match = False

            if match:
                filtered_notifications.append(notification)

        def get_sort_value(notif, sort_key):
            if sort_key == 'id':
                return notif.get('id', 0)
            elif sort_key == 'created_at':
                return datetime.fromisoformat(notif.get('created_at', '1900-01-01T00:00:00'))
            elif sort_key == 'title':
                return notif.get('title', '')
            elif sort_key == 'location':
                return notif.get('location', '')
            elif sort_key == 'classification.prioridade':
                priority_value = notif.get('classification', {}).get('prioridade', 'Baixa')
                priority_order_val = {'CrÃ­tica': 4, 'Alta': 3, 'MÃ©dia': 2, 'Baixa': 1,
                                      UI_TEXTS.text_na: 0,
                                      UI_TEXTS.selectbox_default_prioridade_resolucao: 0}
                return priority_order_val.get(priority_value, 0)
            return None

        actual_sort_column = st.session_state.dashboard_sort_column
        if actual_sort_column in sort_options_map.values():
            filtered_notifications.sort(
                key=lambda n: get_sort_value(n, actual_sort_column),
                reverse=not st.session_state.dashboard_sort_ascending
            )

        st.write(f"**NotificaÃ§Ãµes Encontradas: {len(filtered_notifications)}**")

        items_per_page_options = [5, 10, 20, 50]
        items_per_page_display_options = [UI_TEXTS.selectbox_items_per_page_placeholder] + [
            str(x) for x in
            items_per_page_options]

        if 'dashboard_items_per_page' not in st.session_state: st.session_state.dashboard_items_per_page = 10

        selected_items_per_page_display = st.selectbox(
            UI_TEXTS.selectbox_items_per_page_label,
            options=items_per_page_display_options,
            index=items_per_page_display_options.index(
                str(st.session_state.dashboard_items_per_page)) if str(
                st.session_state.dashboard_items_per_page) in items_per_page_display_options else 0,
            key="dashboard_items_per_page_select"
        )
        if selected_items_per_page_display != UI_TEXTS.selectbox_items_per_page_placeholder:
            st.session_state.dashboard_items_per_page = int(selected_items_per_page_display)
        else:
            st.session_state.dashboard_items_per_page = 10

        total_pages = (
                              len(filtered_notifications) + st.session_state.dashboard_items_per_page - 1) // st.session_state.dashboard_items_per_page
        if total_pages == 0: total_pages = 1

        if 'dashboard_current_page' not in st.session_state: st.session_state.dashboard_current_page = 1
        st.session_state.dashboard_current_page = st.number_input(
            "PÃ¡gina:", min_value=1, max_value=total_pages,
            value=st.session_state.dashboard_current_page,
            key="dashboard_current_page_input"
        )

        start_idx = (
                            st.session_state.dashboard_current_page - 1) * st.session_state.dashboard_items_per_page
        end_idx = start_idx + st.session_state.dashboard_items_per_page
        paginated_notifications = filtered_notifications[start_idx:end_idx]

        if not paginated_notifications:
            st.info("Nenhuma notificaÃ§Ã£o encontrada com os filtros e busca aplicados.")
        else:
            for notification in paginated_notifications:
                status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
                created_at_str = datetime.fromisoformat(notification['created_at']).strftime(
                    '%d/%m/%Y %H:%M:%S')
                current_status_display = status_mapping.get(
                    notification.get('status', UI_TEXTS.text_na),
                    notification.get('status', UI_TEXTS.text_na).replace('_',
                                                                         ' ').title())

                # Get deadline details for display in dashboard list
                classif_info = notification.get('classification') or {}
                deadline_date_str = classif_info.get('deadline_date')
                deadline_html = ""
                if deadline_date_str:
                    deadline_date_formatted = datetime.fromisoformat(
                        deadline_date_str).strftime('%d/%m/%Y')
                    deadline_status = get_deadline_status(deadline_date_str)
                    deadline_html = f" | <strong class='{deadline_status['class']}'>Prazo: {deadline_date_formatted} ({deadline_status['text']})</strong>"

                st.markdown(f"""
                                    <div class="notification-card">
                                        <h4>#{notification.get('id', UI_TEXTS.text_na)} - {notification.get('title', UI_TEXTS.text_na)}</h4>
                                        <p><strong>Status:</strong> <span class="{status_class}">{current_status_display}</span> {deadline_html}</p>
                                        <p><strong>Local:</strong> {notification.get('location', UI_TEXTS.text_na)} | <strong>Criada em:</strong> {created_at_str}</p>
                                    </div>
                                    """, unsafe_allow_html=True)

                with st.expander(
                        f"ðŸ‘ï¸ Visualizar Detalhes - NotificaÃ§Ã£o #{notification.get('id', UI_TEXTS.text_na)}"):
                    display_notification_full_details(notification,
                                                      st.session_state.user.get(
                                                          'id') if st.session_state.authenticated else None,
                                                      st.session_state.user.get(
                                                          'username') if st.session_state.authenticated else None)

    with tab_indicators:
        st.info("Explore os indicadores e tendÃªncias das notificaÃ§Ãµes, com filtros de perÃ­odo.")
        st.markdown("### SeleÃ§Ã£o de PerÃ­odo para Indicadores")

        # Define as datas padrÃ£o para o filtro de perÃ­odo, usando a data mais antiga e mais recente
        min_date = df_notifications[
            'created_at_dt'].min().date() if not df_notifications.empty else dt_date_class.today() - timedelta(
            days=365)
        max_date = df_notifications[
            'created_at_dt'].max().date() if not df_notifications.empty else dt_date_class.today()
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            start_date_indicators = st.date_input("Data de InÃ­cio", value=min_date,
                                                  key="start_date_indicators")
        with col_date2:
            end_date_indicators = st.date_input("Data de Fim", value=max_date,
                                                key="end_date_indicators")

        # Filtra o DataFrame pelo perÃ­odo selecionado
        df_filtered_by_period = df_notifications[
            (df_notifications['created_at_dt'].dt.date >= start_date_indicators) &
            (df_notifications[
                 'created_at_dt'].dt.date <= end_date_indicators)].copy()

        if df_filtered_by_period.empty:
            st.warning("âš ï¸ NÃ£o hÃ¡ dados para o perÃ­odo selecionado para gerar os indicadores.")
            return

        st.markdown("---")

        st.markdown(
            "#### ðŸ“ˆ Quantidade de NotificaÃ§Ãµes por MÃªs (Abertas, ConcluÃ­das, Rejeitadas)")

        df_monthly = df_filtered_by_period.copy()
        df_monthly['month_year'] = df_monthly['created_at_dt'].dt.to_period('M').astype(str)

        # Categoriza o status da notificaÃ§Ã£o
        df_monthly['status_category'] = 'Aberta'
        df_monthly.loc[
            df_monthly['status'].isin(completed_statuses), 'status_category'] = 'ConcluÃ­da'
        df_monthly.loc[
            df_monthly['status'].isin(rejected_statuses), 'status_category'] = 'Rejeitada'

        monthly_counts = df_monthly.groupby(['month_year', 'status_category']).size().unstack(
            fill_value=0)

        # Garante que todos os meses no perÃ­odo estejam presentes, mesmo que sem dados
        all_months_in_range = pd.period_range(start=start_date_indicators,
                                              end=end_date_indicators, freq='M').astype(
            str)
        monthly_counts = monthly_counts.reindex(all_months_in_range, fill_value=0)
        if not monthly_counts.empty:
            st.line_chart(monthly_counts)
        else:
            st.info("Nenhuma notificaÃ§Ã£o encontrada no perÃ­odo para este grÃ¡fico.")

        st.markdown("---")

        st.markdown("####    PendÃªncia de AnÃ¡lises por MÃªs")
        pending_analysis_statuses = ['pendente_classificacao', 'aguardando_classificador',
                                     'revisao_classificador_execucao']
        df_pending_analysis = df_filtered_by_period[
            df_filtered_by_period['status'].isin(pending_analysis_statuses)].copy()

        # Usar o DataFrame original para a lista completa de setores notificados no filtro
        all_notified_departments_unique = sorted(
            df_notifications['notified_department'].unique().tolist())
        notified_departments_filter_options = ['Todos'] + all_notified_departments_unique
        selected_notified_dept = st.selectbox("Filtrar por Setor Notificado:",
                                              notified_departments_filter_options,
                                              key="pending_dept_filter")

        if selected_notified_dept != 'Todos':
            df_pending_analysis = df_pending_analysis[
                df_pending_analysis['notified_department'] == selected_notified_dept]

        if not df_pending_analysis.empty:
            df_pending_analysis['month_year'] = df_pending_analysis[
                'created_at_dt'].dt.to_period('M').astype(str)
            monthly_pending_counts = df_pending_analysis.groupby(
                'month_year').size().reset_index(name='Quantidade')

            all_months_in_range_pending = pd.period_range(start=start_date_indicators,
                                                          end=end_date_indicators,
                                                          freq='M').astype(str)
            monthly_pending_counts = monthly_pending_counts.set_index('month_year').reindex(
                all_months_in_range_pending,
                fill_value=0).reset_index()
            monthly_pending_counts.columns = ['month_year', 'Quantidade']

            st.bar_chart(monthly_pending_counts.set_index('month_year'))
        else:
            st.info("Nenhuma pendÃªncia de anÃ¡lise encontrada no perÃ­odo e filtro selecionados.")

        st.markdown("---")

        st.markdown("####    Top 10 Setores Notificados e Notificantes")
        col_top1, col_top2 = st.columns(2)

        with col_top1:
            st.markdown("##### Top 10 Setores Notificados")
            if not df_filtered_by_period.empty:
                top_notified = df_filtered_by_period[
                    'notified_department'].value_counts().nlargest(10)
                if not top_notified.empty:
                    st.bar_chart(top_notified)
                else:
                    st.info("Nenhum dado de setor notificado para o perÃ­odo.")
            else:
                st.info("Nenhum dado de setor notificado para o perÃ­odo.")

        with col_top2:
            st.markdown("##### Top 10 Setores Notificantes")
            if not df_filtered_by_period.empty:
                top_reporting = df_filtered_by_period[
                    'reporting_department'].value_counts().nlargest(10)
                if not top_reporting.empty:
                    st.bar_chart(top_reporting)
                else:
                    st.info("Nenhum dado de setor notificante para o perÃ­odo.")
            else:
                st.info("Nenhum dado de setor notificante para o perÃ­odo.")

        st.markdown("---")

        st.markdown("#### ðŸ“Š ClassificaÃ§Ã£o das NotificaÃ§Ãµes (NNC e Tipo Principal)")

        # DataFrame para notificaÃ§Ãµes concluÃ­das no perÃ­odo
        df_completed_period = df_filtered_by_period[
            df_filtered_by_period['status'].isin(completed_statuses)].copy()
        # DataFrame para notificaÃ§Ãµes abertas no perÃ­odo (nÃ£o concluÃ­das e nÃ£o rejeitadas)
        df_open_period = df_filtered_by_period[
            ~df_filtered_by_period['status'].isin(
                completed_statuses + rejected_statuses)].copy()

        col_classif1, col_classif2 = st.columns(2)

        with col_classif1:
            st.markdown("##### NNC - ConcluÃ­das")
            if not df_completed_period.empty:
                # Usa .apply para acessar o dicionÃ¡rio 'classification' e pegar 'nnc'
                completed_nnc = df_completed_period['classification'].apply(
                    lambda x: x.get('nnc') if x else None).value_counts().dropna()
                if not completed_nnc.empty:
                    st.bar_chart(completed_nnc)
                else:
                    st.info(
                        "Nenhuma classificaÃ§Ã£o NNC para notificaÃ§Ãµes concluÃ­das no perÃ­odo.")
            else:
                st.info("Nenhuma notificaÃ§Ã£o concluÃ­da no perÃ­odo.")

        with col_classif2:
            st.markdown("##### NNC - Abertas")
            if not df_open_period.empty:
                open_nnc = df_open_period['classification'].apply(
                    lambda x: x.get('nnc') if x else None).value_counts().dropna()
                if not open_nnc.empty:
                    st.bar_chart(open_nnc)
                else:
                    st.info("Nenhuma classificaÃ§Ã£o NNC para notificaÃ§Ãµes abertas no perÃ­odo.")
            else:
                st.info("Nenhuma notificaÃ§Ã£o aberta no perÃ­odo.")

        col_classif3, col_classif4 = st.columns(2)
        with col_classif3:
            st.markdown("##### Tipo Principal - ConcluÃ­das")
            if not df_completed_period.empty:
                completed_main_type = df_completed_period['classification'].apply(
                    lambda x: x.get('event_type_main') if x else None).value_counts().dropna()
                if not completed_main_type.empty:
                    st.bar_chart(completed_main_type)
                else:
                    st.info("Nenhum tipo principal para notificaÃ§Ãµes concluÃ­das no perÃ­odo.")
            else:
                st.info("Nenhuma notificaÃ§Ã£o concluÃ­da no perÃ­odo.")

        with col_classif4:
            st.markdown("##### Tipo Principal - Abertas")
            if not df_open_period.empty:
                open_main_type = df_open_period['classification'].apply(
                    lambda x: x.get('event_type_main') if x else None).value_counts().dropna()
                if not open_main_type.empty:
                    st.bar_chart(open_main_type)
                else:
                    st.info("Nenhuma tipo principal para notificaÃ§Ãµes abertas no perÃ­odo.")
            else:
                st.info("Nenhuma notificaÃ§Ã£o aberta no perÃ­odo.")


def main():
    """Main function to run the Streamlit application."""
    init_database()  # Garante que o DB e tabelas estÃ£o inicializadas

    if 'authenticated' not in st.session_state: st.session_state.authenticated = False
    if 'user' not in st.session_state: st.session_state.user = None
    if 'page' not in st.session_state: st.session_state.page = 'create_notification'

    if 'initial_classification_state' not in st.session_state: st.session_state.initial_classification_state = {}
    if 'review_classification_state' not in st.session_state: st.session_state.review_classification_state = {}
    if 'current_initial_classification_id' not in st.session_state: st.session_state.current_initial_classification_id = None
    if 'current_review_classification_id' not in st.session_state: st.session_state.current_review_classification_id = None
    # NOVO: Adiciona o estado para o formulÃ¡rio de aprovaÃ§Ã£o
    if 'approval_form_state' not in st.session_state: st.session_state.approval_form_state = {}

    show_sidebar()

    restricted_pages = ['dashboard', 'classification', 'execution', 'approval', 'admin']
    if st.session_state.page in restricted_pages and not st.session_state.authenticated:
        st.warning("âš ï¸ VocÃª precisa estar logado para acessar esta pÃ¡gina.")
        st.session_state.page = 'create_notification'
        st.rerun()  # Permanece, pois Ã© navegaÃ§Ã£o global

    if st.session_state.page == 'create_notification':
        show_create_notification()  # Chama a versÃ£o fragmentada
    elif st.session_state.page == 'dashboard':
        show_dashboard()  # Chama a versÃ£o fragmentada
    elif st.session_state.page == 'classification':
        show_classification()  # Chama a versÃ£o fragmentada
    elif st.session_state.page == 'execution':
        show_execution()  # Chama a versÃ£o fragmentada
    elif st.session_state.page == 'approval':
        show_approval()  # Chama a versÃ£o fragmentada
    elif st.session_state.page == 'admin':
        show_admin()  # Chama a versÃ£o fragmentada
    else:
        st.error("PÃ¡gina solicitada invÃ¡lida. Redirecionando para a pÃ¡gina inicial.")
        st.session_state.page = 'create_notification'
        st.rerun()  # Permanece, pois Ã© navegaÃ§Ã£o global


if __name__ == "__main__":
    main()
