
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
from psycopg2 import sql 
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from streamlit import fragment as st_fragment

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

def get_db_connection():
    """
    Estabelece e retorna uma conexão com o banco de dados PostgreSQL.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        raise

st.set_page_config(
    page_title="NotificaSanta",
    page_icon="favicon/logo.png",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown(r"""
<style>
    /* Esconde botões e decorações padrão do Streamlit */
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

    /* Estilo do cabeçalho principal da aplicação */
    .main-header {
        text-align: center;
        color: #2E86AB;
        margin-bottom: 30px;
    }
    /* Novo Estilo para o Título Principal da Sidebar */
    /* Usamos [data-testid="stSidebarContent"] para aumentar a especificidade e garantir a aplicação */
    [data-testid="stSidebarContent"] .sidebar-main-title {
        text-align: center !important; /* Centraliza o texto */
        color: #00008B !important; /* Cor azul escuro para o título principal */
        font-size: 1.76em !important; /* 2.2em * 0.8 = 1.76em */
        font-weight: 700 !important; /* Negrito forte para o título */
        text-transform: uppercase !important; /* Transforma todo o texto em maiúsculas */
        letter-spacing: 2px !important; /* Aumenta o espaçamento entre as letras para um visual "minimalista" e "estiloso" */
        text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.2) !important; /* Sombra mais suave para profundidade */
        margin-top: -30px !important; /* Move título principal para cima */
    }
    /* Novo Estilo para o Subtítulo da Sidebar */
    /* Usamos [data-testid="stSidebarContent"] para aumentar a especificidade e garantir a aplicação */
    [data-testid="stSidebarContent"] .sidebar-subtitle {
        text-align: center !important; /* Centraliza o texto */
        color: #333 !important; /* Cor mais suave para o subtítulo */
        font-size: 0.72em !important; /* 0.9em * 0.8 = 0.72em */
        font-weight: 400 !important; /* Peso de fonte médio */
        text-transform: uppercase !important; /* Transforma todo o texto em maiúsculas, mantendo a consistência */
        letter-spacing: 1.5px !important; /* Espaçamento entre letras para alinhamento visual */
        margin-top: -30px !important; /* Pull closer to main title */
        margin-bottom: 5px !important; /* Reduce space below image */
    }
    /* Estilo geral para cartões de notificação */
    .notification-card {
        border: 1px solid #ddd;
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
        background-color: #f9f9f9;
        color: #2E86AB; /* Cor do texto padrão para o cartão */
    }
    /* Cores e destaque para diferentes status de notificação */
    .status-pendente_classificacao { color: #ff9800; font-weight: bold; } /* Laranja */
    .status-classificada { color: #2196f3; font-weight: bold; } /* Azul */
    .status-em_execucao { color: #9c27b0; font-weight: bold; } /* Roxo */
    .status-aguardando_classificador { color: #ff5722; font-weight: bold; } /* Laranja avermelhado (Usado para Revisão Rejeitada) */
    .status-revisao_classificador_execucao { color: #8BC34A; font-weight: bold; } /* Verde Lima - Novo Status */
    .status-aguardando_aprovacao { color: #ffc107; font-weight: bold; } /* Amarelo */
    .status-aprovada { color: #4caf50; font-weight: bold; } /* Verde */
    .status-concluida { color: #4caf50; font-weight: bold; } /* Verde (mesmo que aprovada para simplificar) */
    .status-rejeitada { color: #f44336; font-weight: bold; } /* Vermelho (Usado para Rejeição Inicial) */
    .status-reprovada { color: #f44336; font-weight: bold; } /* Vermelho (Usado para Rejeição de Aprovação)*/
    /* Estilo para o conteúdo da barra lateral */
    .sidebar .sidebar-content {
        background-color: #f0f2f6; /* Cinza claro */
    }
    /* Estilo para a caixa de informações do usuário na sidebar */
    .user-info {
        background-color: #e8f4fd; /* Azul claro */
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 20px;
    }

    /* Estilo para seções de formulário */
    .form-section {
        background-color: #f8f9fa; /* Cinza bem claro */
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        border-left: 4px solid #2E86AB; /* Barra lateral azul */
    }

    /* Estilo para campos condicionais em formulários (ex: detalhes de ação imediata) */
    .conditional-field {
        background-color: #fff3cd; /* Amarelo claro */
        padding: 10px;
        border-radius: 5px;
        border-left: 3px solid #ffc107; /* Barra lateral amarela */
        margin: 10px 0;
    }

    /* Estilo para campos obrigatórios */
    .required-field {
        color: #dc3545; /* Vermelho */
        font-weight: bold;
    }
    /* Cores específicas para botões "Sim" e "Não" selecionados */
    div.stButton > button[data-testid="stButton"][data-key*="_sim_step"][data-selected="true"] {
        border-color: #4caf50; /* Verde */
        color: #4caf50;
    }
    div.stButton > button[data-testid="stButton"][data-key*="_nao_step"][data-selected="true"] {
        border-color: #f44336; /* Vermelho */
        color: #f44336;
    }

    /* Negrito geral para labels dentro de blocos horizontais do Streamlit */
    div[data-testid="stHorizontalBlock"] div[data-testid^="st"] label p {
        font-weight: bold;
    }

    /* Estilo para cartões de métricas no dashboard */
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

    /* Estilo para o rodapé da sidebar */
    .sidebar-footer {
        text-align: center;
        margin-top: 20px; /* Adiciona um espaço acima do rodapé */
        padding: 10px;
        color: #888;
        font-size: 0.75em;
        border-top: 1px solid #eee; /* Linha divisória sutil */
    }

    /* Remove padding do container principal, pois o rodapé fixo foi removido */
    div[data-testid="stAppViewContainer"] {
        padding-bottom: 0px; /* Não é mais necessário padding na parte inferior */
    }

    /* Estilos para o fundo do cartão de notificação com base no status do prazo */
    .notification-card.card-prazo-dentro {
        background-color: #e6ffe6; /* Verde claro para "No Prazo" e "Prazo Próximo" */
        border: 1px solid #4CAF50; /* Borda verde */
    }
    .notification-card.card-prazo-fora {
        background-color: #ffe6e6; /* Vermelho claro para "Atrasada" */
        border: 1px solid #F44336; /* Borda vermelha */
    }

    /* Estilos para status de prazo */
    .deadline-ontrack { color: #4CAF50; font-weight: bold; } /* Verde */
    .deadline-duesoon { color: #FFC107; font-weight: bold; } /* Amarelo */
    .deadline-overdue { color: #F44336; font-weight: bold; } /* Vermelho */

    /* Estilo para entrada de ação individual */
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

    /* Estilo para "minhas" ações na execução */
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

    /* Estilo para a seção de evidências dentro de uma ação */
    .evidence-section {
        background-color: #ffffff; /* Fundo branco */
        border-top: 1px dashed #cccccc; /* Linha tracejada superior */
        margin-top: 10px;
        padding-top: 10px;
    }

    .evidence-section h6 { /* Adicionado para subtítulos dentro das evidências */
        color: #666;
        margin-bottom: 5px;
    }
</style>
""", unsafe_allow_html=True)


from typing import Any

def _get_attachments_map_by_ids(conn, ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not ids:
        return {}
    cur = conn.cursor()
    cur.execute(
        """
        SELECT notification_id, unique_name, original_name, uploaded_at
        FROM notification_attachments
        WHERE notification_id = ANY(%s)
        """,
        (ids,)
    )
    rows = cur.fetchall()
    cur.close()

    mp: Dict[int, List[Dict[str, Any]]] = {}
    for nid, uniq, orig, up_at in rows:
        mp.setdefault(nid, []).append({
            "unique_name": uniq,
            "original_name": orig,
            "uploaded_at": up_at.isoformat() if hasattr(up_at, "isoformat") else up_at
        })
    return mp


def _get_history_map_by_ids(conn, ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not ids:
        return {}
    cur = conn.cursor()
    cur.execute(
        """
        SELECT notification_id, action_type, performed_by, action_timestamp, details
        FROM notification_history
        WHERE notification_id = ANY(%s)
        ORDER BY action_timestamp ASC
        """,
        (ids,)
    )
    rows = cur.fetchall()
    cur.close()

    mp: Dict[int, List[Dict[str, Any]]] = {}
    for nid, act, user, ts, det in rows:
        mp.setdefault(nid, []).append({
            "action": act,
            "user": user,
            "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else ts,
            "details": det
        })
    return mp


def _get_actions_map_by_ids(conn, ids: List[int]) -> Dict[int, List[Dict]]:
    if not ids:
        return {}
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            notification_id,
            executor_id,
            executor_name,
            description,
            action_timestamp,
            final_action_by_executor,
            evidence_description,
            evidence_attachments
        FROM notification_actions
        WHERE notification_id = ANY(%s)
        ORDER BY action_timestamp ASC
        """,
        (ids,)
    )
    rows = cur.fetchall()
    cur.close()

    mp: Dict[int, List[Dict]] = {}
    for (nid,
         executor_id,
         executor_name,
         description,
         action_ts,
         final_action_by_executor,
         evidence_description,
         evidence_attachments) in rows:
        mp.setdefault(nid, []).append({
            "executor_id": executor_id,
            "executor_name": executor_name,
            "description": description,
            "timestamp": action_ts.isoformat() if hasattr(action_ts, "isoformat") else action_ts,
            "final_action_by_executor": bool(final_action_by_executor),
            "evidence_description": evidence_description,
            "evidence_attachments": evidence_attachments or []
        })
    return mp


st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


DEADLINE_DAYS_MAPPING = {
    "Não conformidade": 30,
    "Circunstância de Risco": 30,
    "Near Miss": 30,
    "Evento sem dano": 10,
    "Evento com dano": {
        "Dano leve": 7,
        "Dano moderado": 5,
        "Dano grave": 3,
        "Óbito": 3
    }
}

def load_notifications_by_status(status: str):
    """
    Carrega notificações por status específico (consulta otimizada)
    
    Args:
        status: Status da notificação
        
    Returns:
        Lista de notificações no formato dict COM ANEXOS
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT * FROM notifications 
            WHERE status = %s 
            ORDER BY created_at DESC
        """, (status,))
        notifications = [dict(row) for row in cursor.fetchall()]
        
        # ✅ NOVO: Buscar anexos para todas as notificações
        if notifications:
            notification_ids = [n['id'] for n in notifications]
            attachments_map = _get_attachments_map_by_ids(conn, notification_ids)
            
            # Adicionar anexos a cada notificação
            for notif in notifications:
                notif['attachments'] = attachments_map.get(notif['id'], [])
        
        return notifications
        
    except Exception as e:
        st.error(f"Erro ao carregar notificações: {str(e)}")
        return []
    finally:
        conn.close()

def load_notifications_by_statuses(statuses: list):
    """
    Carrega notificações por múltiplos status (consulta otimizada)
    
    Args:
        statuses: Lista de status das notificações
        
    Returns:
        Lista de notificações no formato dict COM ANEXOS
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT * FROM notifications 
            WHERE status = ANY(%s) 
            ORDER BY created_at DESC
        """, (statuses,))
        notifications = [dict(row) for row in cursor.fetchall()]
        
        # ✅ NOVO: Buscar anexos para todas as notificações
        if notifications:
            notification_ids = [n['id'] for n in notifications]
            attachments_map = _get_attachments_map_by_ids(conn, notification_ids)
            
            # Adicionar anexos a cada notificação
            for notif in notifications:
                notif['attachments'] = attachments_map.get(notif['id'], [])
        
        return notifications
        
    except Exception as e:
        st.error(f"Erro ao carregar notificações: {str(e)}")
        return []
    finally:
        conn.close()

class UI_TEXTS:
    selectbox_default_event_shift = "Selecionar Turno"
    selectbox_default_immediate_actions_taken = "Selecione"
    selectbox_default_patient_involved = "Selecione"
    selectbox_default_patient_outcome_obito = "Selecione"
    selectbox_default_initial_event_type = "Selecione"
    selectbox_default_initial_severity = "Selecione"
    selectbox_default_notification_select = "Selecione uma notificação..."
    text_na = "N/A"
    selectbox_default_procede_classification = "Selecione"
    selectbox_default_classificacao_nnc = "Selecione"
    selectbox_default_nivel_dano = "Selecione"
    selectbox_default_prioridade_resolucao = "Selecione"
    selectbox_default_never_event = "Selecione"
    selectbox_default_evento_sentinela = "Selecione"
    selectbox_default_tipo_principal = "Selecione"
    multiselect_instruction_placeholder = "Selecione uma ou mais opções..."
    multiselect_event_spec_label_prefix = "Especificação do Evento "
    multiselect_event_spec_label_suffix = ":"
    multiselect_classification_oms_label = "Classificação OMS:* (selecionar ao menos um)"
    selectbox_default_requires_approval = "Selecione"
    selectbox_default_approver = "Selecione"
    selectbox_default_decisao_revisao = "Selecione"
    selectbox_default_acao_realizar = "Selecione"
    multiselect_assign_executors_label = "Atribuir Executores Responsáveis:*"
    selectbox_default_decisao_aprovacao = "Selecione"
    multiselect_all_option = "Todos"
    selectbox_sort_by_placeholder = "Ordenar por..."
    selectbox_sort_by_label = "Ordenar por:"
    selectbox_items_per_page_placeholder = "Itens por página..."
    selectbox_items_per_page_label = "Itens por página:"
    selectbox_default_admin_debug_notif = "Selecione uma notificação..."
    selectbox_never_event_na_text = "Não Aplicável (N/A)"
    multiselect_user_roles_label = "Funções do Usuário:*"
    deadline_status_ontrack = "No Prazo"
    deadline_status_duesoon = "Prazo Próximo"
    deadline_status_overdue = "Atrasada"
    deadline_days_nan = "Nenhum prazo definido"
    selectbox_default_department_select = "Selecione o Setor..."

    multiselect_filter_status_label = "Filtrar por Status:"
    multiselect_filter_nnc_label = "Filtrar por Classificação NNC:"
    multiselect_filter_priority_label = "Filtrar por Prioridade:"


class FORM_DATA:
    turnos = ["Diurno", "Noturno", "Não sei informar"]
    classificacao_nnc = ["Não conformidade", "Circunstância de Risco", "Near Miss", "Evento sem dano",
                         "Evento com dano"]
    niveis_dano = ["Dano leve", "Dano moderado", "Dano grave", "Óbito"]
    prioridades = ["Baixa", "Média", "Alta", "Crítica"]
    SETORES = [
        "Superintendência", "Agência Transfusional (AGT)", "Ala A", "Ala B",
        "Ala C", "Ala E", "Almoxarifado", "Assistência Social",
        "Ambulatório Bariátrica/Reparadora", "CCIH", "CDI", "Centro Cirúrgico",
        "Centro Obstétrico", "CME", "Comercial/Tesouraria", "Compras",
        "Comunicação", "Contabilidade", "CPD (TI)", "DPI",
        "Diretoria Assistencial", "Diretoria Clínica", "Diretoria Financeira",
        "Diretoria Técnica", "Departamento Pessoal (RH)", "Ambulatório Egresso (Especialidades)",
        "EMTN", "Farmácia Clínica", "Farmácia Central", "Farmácia Oncológica (Manipulação Quimioterapia)",
        "Farmácia UNACON", "Farmácia Satélite UTI",
        "Faturamento", "Fisioterapia", "Fonoaudiologia", "Gestão de Leitos",
        "Hemodiálise", "Higienização", "Internação/Autorização (Convênio)", "Iodoterapia",
        "Laboratório de Análises Clínicas", "Lavanderia", "Manutenção Equipamentos", "Manutenção Predial",
        "Maternidade", "Medicina do Trabalho", "NHE", "Odontologia", "Ouvidoria", "Pediatria",
        "Portaria/Gestão de Acessos", "Psicologia", "Qualidade", "Quimioterapia (Salão de Quimio)",
        "Recepção", "Recrutamento e Seleção", "Regulação", "SAME", "SESMT",
        "Serviço de Nutrição e Dietética", "SSB", "Urgência e Emergência/Pronto Socorro",
        "UNACON", "UTI Adulto", "UTI Neo e Pediátrica"
    ]
    never_events = [
        "Cirurgia no local errado do corpo, no paciente errado ou o procedimento errado",
        "Retenção de corpo estranho em paciente após a cirurgia",
        "Morte de paciente ou lesão grave associada ao uso de dispositivo médico",
        "Morte de paciente ou lesão grave associada à incompatibilidade de tipo sanguíneo",
        "Morte de paciente ou lesão grave associada a erro de medicação",
        "Morte de paciente ou lesão grave associada à trombose venosa profunda (TVP) ou embolia pulmonar (EP) após artroplastia total de quadril ou joelho",
        "Morte de paciente ou lesão grave associada a hipoglicemia",
        "Morte de paciente ou lesão grave associada à infecção hospitalar",
        "Morte de paciente ou lesão grave associada a úlcera por pressão (escaras) adquirida no hospital",
        "Morte de paciente ou lesão grave associada à contenção inadequada",
        "Morte ou lesão grave associada à falha ou uso incorreto de equipamentos de proteção individual (EPIs)",
        "Morte de paciente ou lesão grave associada à queda do paciente",
        "Morte de paciente ou lesão grave associada à violência física ou sexual no ambiente hospitalar",
        "Morte de paciente ou lesão grave associada ao desaparecimento de paciente"
    ]
    tipos_evento_principal = {
        "Clínico": [
            "Infecção Relacionada à Assistência à Saúde (IRAS)",
            "Administração de Antineoplásicos",
            "META 1 - Identificação Incorreta do Paciente",
            "META 2 - Falha na Comunicação entre Profissionais",
            "META 3 - Problema com Medicamento (Segurança Medicamentosa)",
            "META 4 - Procedimento Incorreto (Cirurgia/Parto)",
            "META 5 - Higiene das Mãos Inadequada",
            "META 6 - Queda de Paciente e Lesão por Pressão",
            "Transfusão Inadequada de Sangue ou Derivados",
            "Problema com Dispositivo/Equipamento Médico",
            "Evento Crítico ou Intercorrência Grave em Processo Seguro",
            "Problema Nutricional Relacionado à Assistência",
            "Não Conformidade com Protocolos Gerenciados",
            "Quebra de SLA (Atraso ou Falha na Assistência)",
            "Evento Relacionado ao Parto e Nascimento",
            "Crise Convulsiva em Ambiente Assistencial",
            "[Hemodiálise] Coagulação do Sistema Extracorpóreo",
            "[Hemodiálise] Desconexão Acidental da Agulha de Punção da Fístula Arteriovenosa",
            "[Hemodiálise] Desconexão Acidental do Cateter às Linhas de Hemodiálise",
            "[Hemodiálise] Embolia Pulmonar Relacionada à Hemodiálise",
            "[Hemodiálise] Exteriorização Acidental da Agulha de Punção da Fístula Arteriovenosa",
            "[Hemodiálise] Exteriorização Acidental do Cateter de Hemodiálise",
            "[Hemodiálise] Falha na Identificação do Dialisador ou das Linhas de Hemodiálise",
            "[Hemodiálise] Falha no Fluxo Sanguíneo do Cateter de Hemodiálise",
            "[Hemodiálise] Falha no Fluxo Sanguíneo da Fístula Arteriovenosa",
            "[Hemodiálise] Hematoma Durante a Passagem do Cateter de Hemodiálise",
            "Hemólise Relacionada à Hemodiálise",
            "[Hemodiálise] Infiltração, Edema ou Hematoma na Fístula Arteriovenosa",
            "[Hemodiálise] Pneumotórax Durante a Passagem do Cateter de Hemodiálise",
            "[Hemodiálise] Pseudoaneurisma na Fístula Arteriovenosa",
            "[Hemodiálise] Punção Arterial Acidental Durante Inserção do Cateter de Hemodiálise",
            "[Hemodiálise] Rotura da Fístula Arteriovenosa",
            "[Hemodiálise] Sangramento pelo Óstio do Cateter de Hemodiálise",
            "[Hemodiálise] Outras Falhas Relacionadas à Hemodiálise"
        ],
        "Não-clínico": [
            "Incidente de Segurança Patrimonial",
            "Problema Estrutural/Instalações",
            "Problema de Abastecimento/Logística",
            "Incidente de TI/Dados",
            "Erro Administrativo",
            "Outros Eventos Não-clínicos"
        ],
        "Ocupacional": [
            "Acidente com Material Biológico",
            "Acidente de Trabalho (geral)",
            "Doença Ocupacional",
            "Exposição a Agentes de Risco",
            "Outros Eventos Ocupacionais"
        ],
        "Queixa técnica": [],
        "Outros": []
    }
    classificacao_oms = [
        "Quedas", "Infecções", "Medicação", "Cirurgia", "Identificação do Paciente",
        "Procedimentos", "Dispositivos Médicos", "Urgência/Emergência",
        "Segurança do Ambiente", "Comunicação", "Recursos Humanos", "Outros"
    ]
DATA_DIR = "data"
ATTACHMENTS_DIR = os.path.join(DATA_DIR, "attachments")


def init_database():
    """Garante que os diretórios de dados e arquivos iniciais existam e cria tabelas no DB."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(ATTACHMENTS_DIR):
        os.makedirs(ATTACHMENTS_DIR)

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                roles TEXT[] NOT NULL DEFAULT '{}',
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
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- ADICIONADO: Coluna updated_at
                classification JSONB,
                rejection_classification JSONB,
                review_execution JSONB,
                approval JSONB,
                rejection_approval JSONB,
                rejection_execution_review JSONB,
                conclusion JSONB,

                executors INTEGER[] DEFAULT '{}',
                approver INTEGER REFERENCES users(id),
                search_vector TSVECTOR
            );
            -- CORREÇÃO: Adiciona a coluna updated_at se ela não existir
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='notifications' AND column_name='updated_at') THEN
                    ALTER TABLE notifications ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
                END IF;
            END
            $$;

            CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications (status);
            CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications (created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_notifications_approver ON notifications (approver);
            CREATE INDEX IF NOT EXISTS idx_notifications_classification_gin ON notifications USING GIN (classification);
            CREATE INDEX IF NOT EXISTS idx_notifications_executors_gin ON notifications USING GIN (executors);
            CREATE INDEX IF NOT EXISTS idx_notifications_search_vector ON notifications USING GIN (search_vector);

            CREATE OR REPLACE FUNCTION update_notification_search_vector() RETURNS TRIGGER AS $$
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
            $$ LANGUAGE plpgsql;
            DROP TRIGGER IF EXISTS trg_notifications_search_vector ON notifications;
            CREATE TRIGGER trg_notifications_search_vector
            BEFORE INSERT OR UPDATE ON notifications
            FOR EACH ROW EXECUTE FUNCTION update_notification_search_vector();

            CREATE TABLE IF NOT EXISTS notification_attachments (
                id SERIAL PRIMARY KEY,
                notification_id INTEGER NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
                unique_name VARCHAR(255) NOT NULL,
                original_name VARCHAR(255) NOT NULL,
                uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_attachments_notification_id ON notification_attachments (notification_id);
            CREATE TABLE IF NOT EXISTS notification_history (
                id SERIAL PRIMARY KEY,
                notification_id INTEGER NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
                action_type VARCHAR(255) NOT NULL,
                performed_by VARCHAR(255),
                action_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                details TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_history_notification_id ON notification_history (notification_id);
            CREATE INDEX IF NOT EXISTS idx_history_timestamp ON notification_history (action_timestamp);
            CREATE TABLE IF NOT EXISTS notification_actions (
                id SERIAL PRIMARY KEY,
                notification_id INTEGER NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
                executor_id INTEGER REFERENCES users(id),
                executor_name VARCHAR(255),
                description TEXT NOT NULL,
                action_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                final_action_by_executor BOOLEAN NOT NULL DEFAULT FALSE,
                evidence_description TEXT,
                evidence_attachments JSONB
            );
            CREATE INDEX IF NOT EXISTS idx_actions_notification_id ON notification_actions (notification_id);
            CREATE INDEX IF NOT EXISTS idx_actions_executor_id ON notification_actions (executor_id);
            CREATE INDEX IF NOT EXISTS idx_actions_timestamp ON notification_actions (action_timestamp);
        """)
        
        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0:
            admin_password_hash = hash_password("6105/*")
            cur.execute("""
                INSERT INTO users (username, password_hash, name, email, roles, active)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, ('admin', admin_password_hash, 'Administrador', 'admin@hospital.com',
                  ['admin', 'classificador', 'executor', 'aprovador'], True))
            conn.commit()
            st.toast("Usuário administrador padrão criado no banco de dados!")         

        conn.commit()
        cur.close()
    except psycopg2.Error as e:
        st.error(f"Erro ao inicializar o banco de dados: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def init_database_performance_objects():
    """
    Cria extensões e índices voltados a performance. Não altera a funcionalidade.
    Deve ser chamada uma única vez após init_database().
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        cur.execute("CREATE EXTENSION IF NOT EXISTS unaccent;")

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_status
              ON notifications (status);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_created_at
              ON notifications (created_at DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_approver
              ON notifications (approver);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_updated_at
              ON notifications (updated_at DESC);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_status_pendente_partial
              ON notifications (created_at DESC)
              WHERE status = 'pendente_classificacao';
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_classification_gin_path
              ON notifications USING GIN (classification jsonb_path_ops);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_search_vector
              ON notifications USING GIN (search_vector);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_title_trgm
              ON notifications USING GIN (title gin_trgm_ops);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_description_trgm
              ON notifications USING GIN (description gin_trgm_ops);
        """)

        cur.execute("""
            DO $$
            BEGIN
              IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'notifications'
                  AND column_name = 'notified_department'
              ) THEN
                CREATE INDEX IF NOT EXISTS idx_notifications_notified_department
                  ON notifications (notified_department);
              END IF;
            END$$;
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_attachments_notification_id
              ON notification_attachments (notification_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_history_notification_id
              ON notification_history (notification_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_actions_notification_id
              ON notification_actions (notification_id);
        """)

        conn.commit()
        cur.close()
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def load_users() -> List[Dict]:
    """Carrega dados de usuário do banco de dados."""
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
                "password": u[2],
                "name": u[3],
                "email": u[4],
                "roles": u[5],
                "active": u[6],
                "created_at": u[7].isoformat() if u[7] else None
            }
            for u in users_raw
        ]
    except psycopg2.Error as e:
        st.error(f"Erro ao carregar usuários: {e}")
        return []
    finally:
        if conn:
            conn.close()

def create_user(data: Dict) -> Optional[Dict]:
    """Cria um novo registro de usuário no banco de dados."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT id FROM users WHERE username = %s", (data.get('username', '').lower(),))
        if cur.fetchone():
            return None
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
            data.get('roles', []),
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
                "password": new_user_raw[2],
                "name": new_user_raw[3],
                "email": new_user_raw[4],
                "roles": new_user_raw[5],
                "active": new_user_raw[6],
                "created_at": new_user_raw[7].isoformat()
            }
        return None
    except psycopg2.Error as e:
        st.error(f"Erro ao criar usuário: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def update_user(user_id: int, updates: Dict) -> Optional[Dict]:
    """Atualiza um registro de usuário existente no banco de dados."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        set_clauses = []
        values = []
        for key, value in updates.items():
            if key == 'password' and value:
                set_clauses.append(sql.Identifier('password_hash') + sql.SQL(' = %s'))
                values.append(hash_password(value))
            elif key == 'roles':
                set_clauses.append(sql.Identifier(key) + sql.SQL(' = %s'))
                values.append(list(value))
            elif key not in ['id', 'username', 'created_at']:
                set_clauses.append(sql.Identifier(key) + sql.SQL(' = %s'))
                values.append(value)

        if not set_clauses:
            return None

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
                "password": updated_user_raw[2],
                "name": updated_user_raw[3],
                "email": updated_user_raw[4],
                "roles": updated_user_raw[5],
                "active": updated_user_raw[6],
                "created_at": updated_user_raw[7].isoformat()
            }
        return None
    except psycopg2.Error as e:
        st.error(f"Erro ao atualizar usuário: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

def load_notifications() -> List[Dict]:
    """
    Carrega dados de notificação do banco de dados, incluindo dados relacionados.
    """
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
                additional_notes, status, created_at, updated_at,
                classification, rejection_classification, review_execution, approval,
                rejection_approval, rejection_execution_review, conclusion,
                executors, approver
            FROM notifications
            ORDER BY created_at DESC
        """)
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        cur.close()

        notifications: List[Dict[str, Any]] = []
        ids: List[int] = []
        for row in rows:
            notification = dict(zip(column_names, row))
            if notification.get('occurrence_date'):
                notification['occurrence_date'] = notification['occurrence_date'].isoformat()
            if notification.get('occurrence_time'):
                notification['occurrence_time'] = notification['occurrence_time'].isoformat()
            if notification.get('created_at'):
                notification['created_at'] = notification['created_at'].isoformat()
            if notification.get('updated_at'):
                notification['updated_at'] = notification['updated_at'].isoformat()
            notifications.append(notification)
            ids.append(notification['id'])

        attachments_map = _get_attachments_map_by_ids(conn, ids)
        history_map = _get_history_map_by_ids(conn, ids)
        actions_map = _get_actions_map_by_ids(conn, ids)

        for n in notifications:
            nid = n['id']
            n['attachments'] = attachments_map.get(nid, [])
            n['history'] = history_map.get(nid, [])
            n['actions'] = actions_map.get(nid, [])

        return notifications

    except psycopg2.Error as e:
        st.error(f"Erro ao carregar notificações: {e}")
        return []
    finally:
        if conn:
            conn.close()

def create_notification(data: Dict, uploaded_files: Optional[List[Any]] = None):
    """
    Cria a notificação e retorna o registro criado (com anexos/histórico).
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO notifications (
                title, description, location, occurrence_date, occurrence_time,
                reporting_department, reporting_department_complement, notified_department,
                notified_department_complement, event_shift, immediate_actions_taken,
                immediate_action_description, patient_involved, patient_id, patient_outcome_obito,
                additional_notes, status, created_at, updated_at,
                classification, rejection_classification, review_execution, approval,
                rejection_approval, rejection_execution_review, conclusion,
                executors, approver
            ) VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,
                %s,%s, %s, %s,
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s
            )
            RETURNING id
        """, (
            data.get('title', '').strip(),
            data.get('description', '').strip(),
            data.get('location', '').strip(),
            data.get('occurrence_date'),
            data.get('occurrence_time'),
            data.get('reporting_department', '').strip(),
            data.get('reporting_department_complement', '').strip(),
            data.get('notified_department', '').strip(),
            data.get('notified_department_complement', '').strip(),
            data.get('event_shift'),
            data.get('immediate_actions_taken') == "Sim",
            (data.get('immediate_action_description', '').strip()
             if data.get('immediate_actions_taken') == "Sim" else None),
            data.get('patient_involved') == "Sim",
            (data.get('patient_id', '').strip() if data.get('patient_involved') == "Sim" else None),
            (True if data.get('patient_outcome_obito') == "Sim" else
             False if data.get('patient_outcome_obito') == "Não" else None)
              if data.get('patient_involved') == "Sim" else None,
            data.get('additional_notes', '').strip(),
            "pendente_classificacao",
            datetime.now(),
            datetime.now(),
            json.dumps(data.get('classification')) if data.get('classification') is not None else None,
            json.dumps(data.get('rejection_classification')) if data.get('rejection_classification') is not None else None,
            json.dumps(data.get('review_execution')) if data.get('review_execution') is not None else None,
            json.dumps(data.get('approval')) if data.get('approval') is not None else None,
            json.dumps(data.get('rejection_approval')) if data.get('rejection_approval') is not None else None,
            json.dumps(data.get('rejection_execution_review')) if data.get('rejection_execution_review') is not None else None,
            json.dumps(data.get('conclusion')) if data.get('conclusion') is not None else None,
            data.get('executors', []),
            data.get('approver')
        ))
        notification_id = cur.fetchone()[0]

        if uploaded_files:
            for file in uploaded_files:
                saved = save_uploaded_file_to_disk(file, notification_id)
                if saved:
                    cur.execute("""
                        INSERT INTO notification_attachments (notification_id, unique_name, original_name)
                        VALUES (%s, %s, %s)
                    """, (notification_id, saved['unique_name'], saved['original_name']))

        add_history_entry(
            notification_id,
            "Notificação criada",
            "Sistema (Formulário Público)",
            f"Notificação enviada para classificação. Título: {data.get('title', 'Sem título')[:100]}..." if len(data.get('title','')) > 100
              else f"Notificação enviada para classificação. Título: {data.get('title', 'Sem título')}",
            conn=conn
        )

        cur = conn.cursor()
        cur.execute("""
            SELECT
                id, title, description, location, occurrence_date, occurrence_time,
                reporting_department, reporting_department_complement, notified_department,
                notified_department_complement, event_shift, immediate_actions_taken,
                immediate_action_description, patient_involved, patient_id, patient_outcome_obito,
                additional_notes, status, created_at, updated_at,
                classification, rejection_classification, review_execution, approval,
                rejection_approval, rejection_execution_review, conclusion,
                executors, approver
            FROM notifications
            WHERE id = %s
        """, (notification_id,))
        row = cur.fetchone()
        cur.close()

        cols = [
            "id","title","description","location","occurrence_date","occurrence_time",
            "reporting_department","reporting_department_complement",
            "notified_department","notified_department_complement",
            "event_shift","immediate_actions_taken","immediate_action_description",
            "patient_involved","patient_id","patient_outcome_obito",
            "additional_notes","status","created_at","updated_at",
            "classification","rejection_classification","review_execution","approval",
            "rejection_approval","rejection_execution_review","conclusion",
            "executors","approver"
        ]
        created = dict(zip(cols, row))
        if created.get('occurrence_date'):
            created['occurrence_date'] = created['occurrence_date'].isoformat()
        if created.get('occurrence_time'):
            created['occurrence_time'] = created['occurrence_time'].isoformat()
        if created.get('created_at'):
            created['created_at'] = created['created_at'].isoformat()
        if created.get('updated_at'):
            created['updated_at'] = created['updated_at'].isoformat()

        attachments_map = _get_attachments_map_by_ids(conn, [notification_id])
        history_map = _get_history_map_by_ids(conn, [notification_id])
        actions_map = _get_actions_map_by_ids(conn, [notification_id])

        created['attachments'] = attachments_map.get(notification_id, [])
        created['history'] = history_map.get(notification_id, [])
        created['actions'] = actions_map.get(notification_id, [])

        conn.commit()
        return created

    except psycopg2.Error as e:
        st.error(f"Erro ao criar notificação: {e}")
        if conn:
            conn.rollback()
        return {}
    finally:
        if conn:
            conn.close()

def update_notification(notification_id: int, updates: Dict):
    """
    Atualiza um registro de notificação com novos dados no banco.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        column_mapping = {
            'immediate_actions_taken': lambda x: True if x == "Sim" else False if x == "Não" else None,
            'patient_involved': lambda x: True if x == "Sim" else False if x == "Não" else None,
            'patient_outcome_obito': lambda x: (True if x == "Sim" else False if x == "Não" else None),
            'occurrence_date': lambda x: x.isoformat() if isinstance(x, dt_date_class) else x,
            'occurrence_time': lambda x: x.isoformat() if isinstance(x, dt_time_class) else x,
            'classification': lambda x: json.dumps(x) if x is not None else None,
            'rejection_classification': lambda x: json.dumps(x) if x is not None else None,
            'review_execution': lambda x: json.dumps(x) if x is not None else None,
            'approval': lambda x: json.dumps(x) if x is not None else None,
            'rejection_approval': lambda x: json.dumps(x) if x is not None else None,
            'rejection_execution_review': lambda x: json.dumps(x) if x is not None else None,
            'conclusion': lambda x: json.dumps(x) if x is not None else None,
            'executors': lambda x: x
        }

        set_clauses = []
        values = []

        for key, value in updates.items():
            if key in ['id', 'created_at', 'attachments', 'actions', 'history', 'updated_at']:
                continue
            if key in column_mapping:
                set_clauses.append(sql.Identifier(key) + sql.SQL(' = %s'))
                values.append(column_mapping[key](value))
            else:
                set_clauses.append(sql.Identifier(key) + sql.SQL(' = %s'))
                values.append(value)
        
        set_clauses.append(sql.Identifier('updated_at') + sql.SQL(' = %s'))
        values.append(datetime.now())

        if not set_clauses:
            return None

        query = sql.SQL("""
            UPDATE notifications
               SET {}
             WHERE id = %s
         RETURNING
            id, title, description, location, occurrence_date, occurrence_time,
            reporting_department, reporting_department_complement, notified_department,
            notified_department_complement, event_shift, immediate_actions_taken,
            immediate_action_description, patient_involved, patient_id, patient_outcome_obito,
            additional_notes, status, created_at, updated_at,
            classification, rejection_classification, review_execution, approval,
            rejection_approval, rejection_execution_review, conclusion,
            executors, approver
        """).format(sql.SQL(', ').join(set_clauses))

        values.append(notification_id)
        cur.execute(query, values)
        updated_row = cur.fetchone()
        cur.close()

        if not updated_row:
            conn.rollback()
            return None

        cols = [
            "id","title","description","location","occurrence_date","occurrence_time",
            "reporting_department","reporting_department_complement",
            "notified_department","notified_department_complement",
            "event_shift","immediate_actions_taken","immediate_action_description",
            "patient_involved","patient_id","patient_outcome_obito",
            "additional_notes","status","created_at","updated_at",
            "classification","rejection_classification","review_execution","approval",
            "rejection_approval","rejection_execution_review","conclusion",
            "executors","approver"
        ]
        updated = dict(zip(cols, updated_row))
        if updated.get('occurrence_date'):
            updated['occurrence_date'] = updated['occurrence_date'].isoformat()
        if updated.get('occurrence_time'):
            updated['occurrence_time'] = updated['occurrence_time'].isoformat()
        if updated.get('created_at'):
            updated['created_at'] = updated['created_at'].isoformat()
        if updated.get('updated_at'):
            updated['updated_at'] = updated['updated_at'].isoformat()

        attachments_map = _get_attachments_map_by_ids(conn, [notification_id])
        history_map = _get_history_map_by_ids(conn, [notification_id])
        actions_map = _get_actions_map_by_ids(conn, [notification_id])

        updated['attachments'] = attachments_map.get(notification_id, [])
        updated['history'] = history_map.get(notification_id, [])
        updated['actions'] = actions_map.get(notification_id, [])

        conn.commit()
        return updated

    except psycopg2.Error as e:
        st.error(f"Erro ao atualizar notificação: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

def get_notification_attachments(notification_id: int, conn=None, cur=None) -> List[Dict]:
    """Busca anexos para uma notificação específica. Pode usar conexão e cursor existentes."""
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
        st.error(f"Erro ao carregar anexos da notificação {notification_id}: {e}")
        return []
    finally:
        if not (conn and cur) and local_cur: local_cur.close()
        if not (conn and cur) and local_conn: local_conn.close()


def get_notification_history(notification_id: int, conn=None, cur=None) -> List[Dict]:
    """Busca entradas de histórico para uma notificação. Pode usar conexão e cursor existentes."""
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
        st.error(f"Erro ao carregar histórico da notificação {notification_id}: {e}")
        return []
    finally:
        if not (conn and cur) and local_cur: local_cur.close()
        if not (conn and cur) and local_conn: local_conn.close()

def get_notification_actions(notification_id: int, conn=None, cur=None) -> List[Dict]:
    """Busca ações de executores para uma notificação. Pode usar conexão e cursor existentes."""
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
                "evidence_attachments": a[6]
            }
            for a in actions_raw
        ]
    except psycopg2.Error as e:
        st.error(f"Erro ao carregar ações da notificação {notification_id}: {e}")
        return []
    finally:
        if not (conn and cur) and local_cur: local_cur.close()
        if not (conn and cur) and local_conn: local_conn.close()

def add_history_entry(notification_id: int, action: str, user: str, details: str = "", conn=None, cursor=None):
    """
    Adiciona uma entrada ao histórico de uma notificação.
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
        if not (conn and cursor):
            local_conn.commit()
        return True
    except psycopg2.Error as e:
        st.error(f"Erro ao adicionar entrada de histórico para notificação {notification_id}: {e}")
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
    Adiciona uma ação de executor a uma notificação.
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
        st.error(f"Erro ao adicionar ação para notificação {notification_id}: {e}")
        if local_conn and not (conn and cur):
            local_conn.rollback()
        return False
    finally:
        if local_cur and not (conn and cur): local_cur.close()
        if local_conn and not (conn and cur): local_conn.close()

def save_uploaded_file_to_disk(uploaded_file: Any, notification_id: int) -> Optional[Dict]:
    """Salva um file enviado para o diretório de anexos no disco e retorna suas informações."""
    if uploaded_file is None:
        return None
    original_name = uploaded_file.name
    safe_original_name = "".join(c for c in original_name if c.isalnum() or c in ('.', '_', '-')).rstrip('.')
    unique_filename = f"{notification_id}_{uuid.uuid4().hex}_{safe_original_name}"
    file_path = os.path.join(ATTACHMENTS_DIR, unique_filename)
    try:
        os.makedirs(ATTACHMENTS_DIR, exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        return {"unique_name": unique_filename, "original_name": original_name}
    except Exception as e:
        st.error(f"Erro ao salvar o anexo {original_name} no disco: {e}")
        return None

def get_attachment_data(unique_filename: str) -> Optional[bytes]:
    """Lê o conteúdo de um file de anexo do disco."""
    file_path = os.path.join(ATTACHMENTS_DIR, unique_filename)
    try:
        with open(file_path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        st.warning(f"Anexo não encontrado no caminho: {unique_filename}")
        return None
    except Exception as e:
        st.error(f"Erro ao ler o anexo {unique_filename}: {e}")
        return None


def hash_password(password: str) -> str:
    """Faz o hash de uma senha usando SHA-256."""
    return hashlib.sha256(password.encode()).hexdigest()

def authenticate_user(username: str, password: str) -> Optional[Dict]:
    """Autentica um usuário com base no nome de usuário e senha."""
    users = load_users()
    hashed_password = hash_password(password)
    for user in users:
        if (user.get('username', '').lower() == username.lower() and
                user.get('password') == hashed_password and
                user.get('active', True)):
            return user
    return None

def logout_user():
    """Desloga o usuário atual."""
    st.session_state.authenticated = False
    st.session_state.user = None
    st.session_state.user_id = None
    st.session_state.user_username = None
    st.session_state.page = 'create_notification'
    _reset_form_state()
    if 'initial_classification_state' in st.session_state: st.session_state.pop('initial_classification_state')
    if 'review_classification_state' in st.session_state: st.session_state.pop('review_classification_state')
    if 'classification_active_notification_id' in st.session_state: st.session_state.pop('classification_active_notification_id')
    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')

def check_permission(required_role: str) -> bool:
    """Verifica se o usuário logado possui a função necessária ou é um admin."""
    if not st.session_state.authenticated or st.session_state.user is None:
        return False
    user_roles = st.session_state.user.get('roles', [])
    return required_role in user_roles or 'admin' in user_roles


def get_users_by_role(role: str) -> List[Dict]:
    """Retorna usuários ativos com uma função específica."""
    users = load_users()
    return [user for user in users if role in user.get('roles', []) and user.get('active', True)]


def get_deadline_status(deadline_date_str: Optional[str], completion_timestamp_str: Optional[str] = None) -> Dict:
    """
    Calcula o status do prazo com base no prazo final e, caso aplicável, também se a notificação foi concluída a tempo.
    Retorna um dicionário com 'text' (status) e 'class' (classe CSS para estilo).
    """
    if not deadline_date_str:
        return {"text": UI_TEXTS.deadline_days_nan, "class": ""}

    try:
        deadline_date = dt_date_class.fromisoformat(deadline_date_str)
        if completion_timestamp_str:
            completion_date = datetime.fromisoformat(completion_timestamp_str).date()
            if completion_date <= deadline_date:
                return {"text": UI_TEXTS.deadline_status_ontrack, "class": "deadline-ontrack"}
            else:
                return {"text": UI_TEXTS.deadline_status_overdue, "class": "deadline-overdue"}
        else:
            today = dt_date_class.today()
            days_diff = (deadline_date - today).days
            if days_diff < 0:
                return {"text": UI_TEXTS.deadline_status_overdue, "class": "deadline-overdue"}
            elif days_diff <= 7:
                return {"text": UI_TEXTS.deadline_status_duesoon, "class": "deadline-duesoon"}
            else:
                return {"text": UI_TEXTS.deadline_status_ontrack, "class": "deadline-ontrack"}
    except ValueError:
        return {"text": UI_TEXTS.text_na, "class": ""}


def format_date_time_summary(date_val: Any, time_val: Any) -> str:
    """Formata data e hora opcional para exibição."""
    date_part_formatted = UI_TEXTS.text_na
    time_part_formatted = ''
    if isinstance(date_val, dt_date_class):
        date_part_formatted = date_val.strftime('%d/%m/%Y')
    elif isinstance(date_val, str) and date_val:
        try:
            date_part_formatted = datetime.fromisoformat(date_val).date().strftime('%d/%m/%Y')
        except ValueError:
            date_part_formatted = 'Data inválida'
    elif date_val is None:
        date_part_formatted = 'Não informada'
    if isinstance(time_val, dt_time_class):
        time_part_formatted = f" às {time_val.strftime('%H:%M')}"
    elif isinstance(time_val, str) and time_val and time_val.lower() != 'none':
        try:
            time_str_part = time_val.split('.')[0]
            try:
                time_obj = datetime.strptime(time_str_part, '%H:%M:%S').time()
                if time_obj == datetime.strptime("00:00:00", '%H:%M:%S').time():
                    time_part_formatted = ''
                else:
                    time_part_formatted = f" às {time_obj.strftime('%H:%M')}"
            except ValueError:
                try:
                    time_obj = datetime.strptime(time_str_part, '%H:%M').time()
                    if time_obj == datetime.strptime("00:00", '%H:%M').time():
                        time_part_formatted = ''
                    else:
                        time_part_formatted = f" às {time_obj.strftime('%H:%M')}"
                except ValueError:
                    time_part_formatted = f" às {time_val}"
                    time_obj = None
        except ValueError:
            time_part_formatted = f" às {time_val}"
    elif time_val is None:
        time_part_formatted = ''

    return f"{date_part_formatted}{time_part_formatted}"


def _clear_execution_form_state(notification_id: int):
    """Limpa as chaves do session_state para o formulário de execução após o envio."""
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
    """Limpa as chaves do session_state para o formulário de aprovação."""
    key_notes = f"approval_notes_{notification_id}_refactored"
    key_decision = f"approval_decision_{notification_id}_refactored"

    if key_notes in st.session_state:
        del st.session_state[key_notes]
    if key_decision in st.session_state:
        del st.session_state[key_decision]

    if 'approval_form_state' in st.session_state and notification_id in st.session_state.approval_form_state:
        del st.session_state.approval_form_state[notification_id]

def _reset_form_state():
    """Reinicia as variáveis de estado para o formulário de criação de notificação e outros estados específicos da página."""
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
        'dashboard_filter_status', 'dashboard_filter_nnc', 'dashboard_filter_priority',
        'dashboard_filter_date_start', 'dashboard_filter_date_end', 'dashboard_search_query',
        'dashboard_sort_column', 'dashboard_sort_ascending', 'dashboard_current_page', 'dashboard_items_per_page',
        'dashboard_search_query_input'
    ]
    current_keys = list(st.session_state.keys())
    for key in keys_to_clear:
        if key in current_keys:
            st.session_state.pop(key, None)

    st.session_state.form_step = 1
    st.session_state.create_form_data = {
        'title': '', 'location': '', 'occurrence_date': datetime.now().date(),
        'occurrence_time': datetime.now().time(),
        'reporting_department': UI_TEXTS.selectbox_default_department_select,
        'reporting_department_complement': '', 'event_shift': UI_TEXTS.selectbox_default_event_shift,
        'description': '',
        'immediate_actions_taken': UI_TEXTS.selectbox_default_immediate_actions_taken,
        'immediate_action_description': '',
        'patient_involved': UI_TEXTS.selectbox_default_patient_involved,
        'patient_id': '',
        'patient_outcome_obito': UI_TEXTS.selectbox_default_patient_outcome_obito,
        'notified_department': UI_TEXTS.selectbox_default_department_select,
        'notified_department_complement': '', 'additional_notes': '', 'attachments': []
    }


def show_sidebar():
    """Renderiza a barra lateral com navegação e informações do usuário/login."""
    with st.sidebar:
        st.image("logo.png", use_container_width=True)
        st.markdown("<h2 class='sidebar-main-title'>Portal de Notificações</h2>", unsafe_allow_html=True)
        st.markdown("<h3 class='sidebar-subtitle'>Santa Casa de Poços de Caldas</h3>", unsafe_allow_html=True)
        st.markdown("---")
        if st.session_state.authenticated and st.session_state.user:
            st.markdown(f"""
            <div class="user-info">
                <strong>👤 {st.session_state.user.get('name', UI_TEXTS.text_na)}</strong><br>
                <small>{st.session_state.user.get('username', UI_TEXTS.text_na)}</small><br>
                <small>Funções: {', '.join(st.session_state.user.get('roles', [])) or 'Nenhuma'}</small>
            </div>
            """, unsafe_allow_html=True)
            st.markdown("### 📋 Menu Principal")

            user_roles = st.session_state.user.get('roles', [])

            if st.button("📝 Nova Notificação", key="nav_create_notif", use_container_width=True):
                st.session_state.page = 'create_notification'
                _reset_form_state()
                if 'initial_classification_state' in st.session_state: st.session_state.pop('initial_classification_state')
                if 'review_classification_state' in st.session_state: st.session_state.pop('review_classification_state')
                if 'classification_active_notification_id' in st.session_state: st.session_state.pop('classification_active_notification_id')
                if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                st.rerun()
            if 'classificador' in user_roles or 'admin' in user_roles:
                if st.button("📊 Dashboard de Notificações", key="nav_dashboard", use_container_width=True):
                    st.session_state.page = 'dashboard'
                    _reset_form_state()
                    if 'initial_classification_state' in st.session_state: st.session_state.pop('initial_classification_state')
                    if 'review_classification_state' in st.session_state: st.session_state.pop('review_classification_state')
                    if 'classification_active_notification_id' in st.session_state: st.session_state.pop('classification_active_notification_id')
                    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                    st.rerun()
            if 'classificador' in user_roles or 'admin' in user_roles:
                st.markdown("### 🔍 Classificação")
    
                if st.button("⏳ Classificação Inicial", key="nav_classif_inicial", use_container_width=True):
                   st.session_state.page = 'classificacao_inicial'
                   st.rerun()
    
                if st.button("🛠️ Revisão de Execução", key="nav_revisao_exec", use_container_width=True):
                    st.session_state.page = 'revisao_execucao'
                    st.rerun()
                
                if st.button("✅ Encerradas", key="nav_encerradas", use_container_width=True):
                    st.session_state.page = 'notificacoes_encerradas'
                    st.rerun()
                    
            if 'executor' in user_roles or 'admin' in user_roles:
                if st.button("⚡ Execução", key="nav_execution", use_container_width=True):
                    st.session_state.page = 'execution'
                    _reset_form_state()
                    if 'initial_classification_state' in st.session_state: st.session_state.pop('initial_classification_state')
                    if 'review_classification_state' in st.session_state: st.session_state.pop('review_classification_state')
                    if 'classification_active_notification_id' in st.session_state: st.session_state.pop('classification_active_notification_id')
                    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                    st.rerun()
            if 'aprovador' in user_roles or 'admin' in user_roles:
                if st.button("✅ Aprovação", key="nav_approval", use_container_width=True):
                    st.session_state.page = 'approval'
                    _reset_form_state()
                    if 'initial_classification_state' in st.session_state: st.session_state.pop('initial_classification_state')
                    if 'review_classification_state' in st.session_state: st.session_state.pop('review_classification_state')
                    if 'classification_active_notification_id' in st.session_state: st.session_state.pop('classification_active_notification_id')
                    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                    st.rerun()
            if 'admin' in user_roles:
                if st.button("⚙️ Administração", key="nav_admin", use_container_width=True):
                    st.session_state.page = 'admin'
                    _reset_form_state()
                    if 'initial_classification_state' in st.session_state: st.session_state.pop('initial_classification_state')
                    if 'review_classification_state' in st.session_state: st.session_state.pop('review_classification_state')
                    if 'classification_active_notification_id' in st.session_state: st.session_state.pop('classification_active_notification_id')
                    if 'approval_form_state' in st.session_state: st.session_state.pop('approval_form_state')
                    st.rerun()
            st.markdown("---")
            if st.button("🚪 Sair", key="nav_logout", use_container_width=True):
                logout_user()
                st.rerun()
        else:
            st.markdown("### 🔐 Login do Operador")
            with st.form("sidebar_login_form"):
                username = st.text_input("Usuário", key="sidebar_username_form")
                password = st.text_input("Senha", type="password", key="sidebar_password_form")
                if st.form_submit_button("🔑 Entrar", use_container_width=True):
                    user = authenticate_user(st.session_state.sidebar_username_form,
                                             st.session_state.sidebar_password_form)
                    if user:
                        st.session_state.authenticated = True
                        st.session_state.user = user
                        st.session_state.user_id = user['id']
                        st.session_state.user_username = user['username']
                        st.success(f"Login realizado com sucesso! Bem-vindo, {user.get('name', UI_TEXTS.text_na)}.")
                        st.session_state.pop('sidebar_username_form', None)
                        st.session_state.pop('sidebar_password_form', None)
                        if 'classificador' in user.get('roles', []) or 'admin' in user.get('roles', []):
                            st.session_state.page = 'classificacao_inicial'
                        else:
                            st.session_state.page = 'create_notification'
                        st.rerun()
                    else:
                        st.error("Usuário ou senha inválidos!")
            st.markdown("---")
        st.markdown("""
        <div class="sidebar-footer">
            NotificaSanta v2.0.5<br>
            &copy; 2025 Todos os direitos reservados
        </div>
        """, unsafe_allow_html=True)

def display_notification_full_details(notification: Dict, user_id_logged_in: Optional[int] = None,
                                      user_username_logged_in: Optional[str] = None):
    """
    Exibe os detalhes completos de uma notificação, incluindo anexos com preview e download.
    
    Args:
        notification: Dicionário com os dados da notificação
        user_id_logged_in: ID do usuário logado (opcional)
        user_username_logged_in: Username do usuário logado (opcional)
    """
    st.markdown("###    Detalhes da Notificação")
    col_det1, col_det2 = st.columns(2)
    with col_det1:
        st.markdown("**📝 Evento Reportado Original**")
        st.write(f"**Título:** {notification.get('title', UI_TEXTS.text_na)}")
        st.write(f"**Local:** {notification.get('location', UI_TEXTS.text_na)}")
        occurrence_datetime_summary = format_date_time_summary(notification.get('occurrence_date'),
 notification.get('occurrence_time'))
        st.write(f"**Data/Hora Ocorrência:** {occurrence_datetime_summary}")
        reporting_department = notification.get('reporting_department', UI_TEXTS.text_na)
        reporting_complement = notification.get('reporting_department_complement')
        reporting_dept_display = f"{reporting_department}{f' ({reporting_complement})' if reporting_complement else ''}"
        st.write(f"**Setor Notificante:** {reporting_dept_display}")

        if notification.get('immediate_actions_taken') and notification.get('immediate_action_description'):
            st.info(
                f"**Ações Imediatas Reportadas:** {notification.get('immediate_action_description', UI_TEXTS.text_na)[:300]}...")
    
    with col_det2:
        st.markdown("**⏱️ Informações de Gestão e Classificação**")
        classif = notification.get('classification') or {}
        if isinstance(classif, str):
            try:
                classif = json.loads(classif)
            except json.JSONDecodeError:
                classif = {}
        
        st.write(f"**Classificação NNC:** {classif.get('nnc', UI_TEXTS.text_na)}")
        if classif.get('nnc') == "Evento com dano" and classif.get('nivel_dano'):
            st.write(f"**Nível de Dano:** {classif.get('nivel_dano', UI_TEXTS.text_na)}")
        st.write(f"**Prioridade:** {classif.get('prioridade', UI_TEXTS.text_na)}")
        st.write(f"**Never Event:** {classif.get('never_event', UI_TEXTS.text_na)}")
        st.write(f"**Evento Sentinela:** {'Sim' if classif.get('is_sentinel_event') else 'Não'}")
        st.write(f"**Tipo Principal:** {classif.get('event_type_main', UI_TEXTS.text_na)}")
        sub_type_display_closed = ''
        if classif.get('event_type_sub'):
            if isinstance(classif['event_type_sub'], list):
                sub_type_display_closed = ', '.join(classif['event_type_sub'])
            else:
                sub_type_display_closed = str(classif['event_type_sub'])
        if sub_type_display_closed: st.write(f"**Especificação:** {sub_type_display_closed}")
        st.write(f"**Classificação OMS:** {', '.join(classif.get('oms', [UI_TEXTS.text_na]))}")
        st.write(f"**Classificado por:** {classif.get('classified_by', UI_TEXTS.text_na)}")
        
        # ADIÇÃO: Setor Notificado
        notified_department = notification.get('notified_department', UI_TEXTS.text_na)
        notified_complement = notification.get('notified_department_complement')
        notified_dept_display = f"{notified_department}{f' ({notified_complement})' if notified_complement else ''}"
        st.write(f"**🏢 Setor Notificado:** {notified_dept_display}")
        
        deadline_date_str = classif.get('deadline')
        if deadline_date_str:
            deadline_date_formatted = datetime.fromisoformat(deadline_date_str).strftime('%d/%m/%Y')
            completion_timestamp_str = (notification.get('conclusion') or {}).get('timestamp')
            deadline_status = get_deadline_status(deadline_date_str, completion_timestamp_str)
            st.markdown(
                f"**Prazo de Conclusão:** {deadline_date_formatted} (<span class='{deadline_status['class']}'>{deadline_status['text']}</span>)",
                unsafe_allow_html=True)
        else:
            st.write(f"**Prazo de Conclusão:** {UI_TEXTS.deadline_days_nan}")
    
    st.markdown("**📝 Descrição Completa do Evento**")
    st.info(notification.get('description', UI_TEXTS.text_na))
    
    if classif.get('classifier_observations'):
        st.markdown("**📋 Orientações / Observações do Classificador**")
        st.success(classif.get('classifier_observations', UI_TEXTS.text_na))

    if notification.get('patient_involved'):
        st.markdown("**🏥 Informações do Paciente Afetado**")
        st.write(f"**N° Atendimento/Prontuário:** {notification.get('patient_id', UI_TEXTS.text_na)}")
        outcome = notification.get('patient_outcome_obito')
        if outcome is not None:
            st.write(f"**Evoluiu com óbito?** {'Sim' if outcome is True else 'Não'}")
        else:
            st.write("**Evoluiu com óbito?** Não informado")

    if notification.get('additional_notes'):
        st.markdown("**ℹ️ Observações Adicionais do Notificante**")
        st.info(notification.get('additional_notes', UI_TEXTS.text_na))

    if notification.get('actions'):
        st.markdown("#### ⚡ Histórico de Ações")
        for action in sorted(notification['actions'], key=lambda x: x.get('timestamp', '')):
            action_type = "🏁 CONCLUSÃO (Executor)" if action.get('final_action_by_executor') else "📝 AÇÃO Registrada"
            action_timestamp = action.get('timestamp', UI_TEXTS.text_na)
            if action_timestamp != UI_TEXTS.text_na:
                try:
                    action_timestamp = datetime.fromisoformat(action_timestamp).strftime('%d/%m/%Y %H:%M:%S')
                except ValueError:
                    pass
            if user_id_logged_in and action.get('executor_id') == user_id_logged_in:
                st.markdown(f"""
                <div class='my-action-entry-card'>
                    <strong>{action_type}</strong> - por <strong>VOCÊ ({action.get('executor_name', UI_TEXTS.text_na)})</strong> em {action_timestamp}
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
            if action.get('final_action_by_executor'):
                evidence_desc = action.get('evidence_description', '').strip()
                evidence_atts = action.get('evidence_attachments', [])
                if evidence_desc or evidence_atts:
                    st.markdown(f"""<div class='evidence-section'>""", unsafe_allow_html=True)
                    st.markdown("<h6>Evidências da Conclusão:</h6>", unsafe_allow_html=True)
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
                                        label=f"Baixar Evidência: {original_name}",
                                        data=file_content,
                                        file_name=original_name,
                                        mime="application/octet-stream",
                                        key=f"download_action_evidence_{notification['id']}_{unique_name}"
                                    )
                                else:
                                    st.write(f"Anexo: {original_name} (arquivo não encontrado ou corrompido)")
                    st.markdown(f"""</div>""", unsafe_allow_html=True)
            st.markdown("---")

@st_fragment
def show_create_notification():
    """
    Renderiza a página para criar novas notificações como um formulário multi-etapa.
    """
    st.markdown("<h1 class='main-header'>📝 Nova Notificação (Formulário NNC)</h1>", unsafe_allow_html=True)
    if not st.session_state.authenticated:
        st.info("Para acompanhar o fluxo completo de uma notificação (classificação, execução, aprovação), faça login.")

    if 'form_step' not in st.session_state:
        _reset_form_state()

    current_data = st.session_state.create_form_data
    if st.session_state.form_step == 5:
        st.balloons()
        st.markdown(r"""
        <div style="text-align: center; margin-top: 100px;">
            <h1 style="color: #2E86AB; font-size: 3em;">
                ✅ Notificação Enviada com Sucesso! 😊
            </h1>
            <p style="font-size: 1.2em; color: #555;">
                Obrigado pela sua participação! Voltando para um novo formulário...
            </p>
        </div>
        """, unsafe_allow_html=True)
        time_module.sleep(2)
        _reset_form_state()
        st.session_state.form_step = 1
        st.rerun()
    st.markdown(f"### Etapa {st.session_state.form_step}")
    if st.session_state.form_step == 1:
        with st.container():
            st.markdown("""
            <div class="form-section">
                <h3>📋 Etapa 1: Detalhes da Ocorrência</h3>
                <p>Preencha as informações básicas sobre o evento ocorrido.</p>
            </div>
            """, unsafe_allow_html=True)
            current_data['title'] = st.text_input(
                "Título da Notificação*", value=current_data['title'], placeholder="Breve resumo da notificação",
                help="Descreva brevemente o evento ocorrido", key="create_title_state_refactored")
            current_data['location'] = st.text_input(
                "Local do Evento*", value=current_data['location'],
                placeholder="Ex: UTI - Leito 05, Centro Cirúrgico - Sala 3",
                help="Especifique o local exato onde ocorreu o evento", key="create_location_state_refactored")
            col1, col2 = st.columns(2)
            with col1:
                current_data['occurrence_date'] = st.date_input(
                    "Data da Ocorrência do Evento*", value=current_data['occurrence_date'],
                    help="Selecione a data em que o evento ocorreu", key="create_occurrence_date_state_refactored",
format="DD/MM/YYYY")
            with col2:
                current_data['occurrence_time'] = st.time_input(
                    "Hora Aproximada do Evento", value=current_data['occurrence_time'],
                    help="Hora aproximada em que o evento ocorreu.", key="create_event_time_state_refactored")
            
            reporting_dept_options = [UI_TEXTS.selectbox_default_department_select] + FORM_DATA.SETORES
            current_data['reporting_department'] = st.selectbox(
                "Setor Notificante*",
                options=reporting_dept_options,
                index=reporting_dept_options.index(current_data['reporting_department'])
                      if current_data['reporting_department'] in reporting_dept_options
                      else 0,
                help="Selecione o setor responsável por notificar o evento",
                key="create_reporting_dept_state_refactored"
            )
            current_data['reporting_department_complement'] = st.text_input(
                "Complemento do Setor Notificante", value=current_data['reporting_department_complement'],
                placeholder="Informações adicionais do setor (opcional)",
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
                "Descrição Detalhada do Evento*", value=current_data['description'],
                placeholder="Descreva:\n• O que aconteceu?\n• Quando aconteceu?\n• Onde aconteceu?\n• Quem esteve envolvido?\n• Como aconteceu?\n• Consequências observadas",
                height=150,
                key="create_description_state_refactored")
            st.markdown("<span class='required-field'>* Campos obrigatórios</span>", unsafe_allow_html=True)
            st.markdown("---")
    elif st.session_state.form_step == 2:
        with st.container():
            st.markdown("""
            <div class="form-section">
                <h3>⚡ Etapa 2: Ações Imediatas</h3>
                <p>Indique se alguma ação foi tomada imediatamente após o evento.</p>
            </div>
            """, unsafe_allow_html=True)
            immediate_actions_taken_options = [UI_TEXTS.selectbox_default_immediate_actions_taken, "Sim", "Não"]
            current_data['immediate_actions_taken'] = st.selectbox(
                "Foram tomadas ações imediatas?*", options=immediate_actions_taken_options,
                index=immediate_actions_taken_options.index(current_data[
                                                                'immediate_actions_taken']) if current_data[
                                                                                                    'immediate_actions_taken'] in immediate_actions_taken_options else 0,
                key="immediate_actions_taken_state_refactored", help="Indique se alguma ação foi tomada...")
            st.markdown("<span class='required-field'>* Campo obrigatório</span>", unsafe_allow_html=True)
            if current_data['immediate_actions_taken'] == 'Sim':
                st.markdown("""
                <div class="conditional-field">
                    <h4>📝 Detalhes das Ações Imediatas</h4>
                    <p>Descreva detalhadamente as ações que foram tomadas.</p>
                </div>
                """, unsafe_allow_html=True)
                current_data['immediate_action_description'] = st.text_area(
                    "Descrição detalhada da ação realizada*", value=current_data['immediate_action_description'],
placeholder="Descreva:\n• Quais ações foram tomadas?\n• Por quem foram executadas?\n• Quando foram realizadas?\n• Resultados obtidos",
                    height=150,
                    key="create_immediate_action_desc_state_refactored",
                    help="Forneça um relato completo...")
                st.markdown("<span class='required-field'>* Campo obrigatório</span>", unsafe_allow_html=True)
            else:
                current_data['immediate_action_description'] = ""
        st.markdown("---")
    elif st.session_state.form_step == 3:
        with st.container():
            st.markdown("""
            <div class="form-section">
                <h3>🏥 Etapa 3: Impacto no Paciente</h3>
                <p>Indique se o evento teve qualquer tipo de envolvimento ou impacto em um paciente.</p>
            </div>
            """, unsafe_allow_html=True)
            patient_involved_options = [UI_TEXTS.selectbox_default_patient_involved, "Sim", "Não"]
            current_data['patient_involved'] = st.selectbox(
                "O evento atingiu algum paciente?*", options=patient_involved_options,
                index=patient_involved_options.index(current_data[
                                                         'patient_involved']) if current_data[
                                                                                     'patient_involved'] in patient_involved_options else 0,
                key="patient_involved_state_refactored",
                help="Indique se o evento teve qualquer tipo de envolvimento...")
            st.markdown("<span class='required-field'>* Campo obrigatório</span>", unsafe_allow_html=True)
            if current_data['patient_involved'] == 'Sim':
                st.markdown("""
                <div class="conditional-field">
                    <h4>🏥 Informações do Paciente Afetado</h4>
                    <p>Preencha as informações do paciente envolvido no evento.</p>
                </div>
                """, unsafe_allow_html=True)
                col5, col6 = st.columns(2)
                with col5:
                    current_data['patient_id'] = st.text_input(
                        "Número do Atendimento/Prontuário*", value=current_data['patient_id'],
                        placeholder="Ex: 123456789", key="create_patient_id_refactored",
                        help="Número de identificação do paciente...")
                with col6:
                    patient_outcome_obito_options = [UI_TEXTS.selectbox_default_patient_outcome_obito, "Sim",
                                                     "Não"]
                    current_data['patient_outcome_obito'] = st.selectbox(
                        "O paciente evoluiu com óbito?*", options=patient_outcome_obito_options,
                        index=patient_outcome_obito_options.index(current_data['patient_outcome_obito']) if
                        current_data['patient_outcome_obito'] in patient_outcome_obito_options else 0,
                        key="create_patient_outcome_obito_refactored",
                        help="Indique se o evento resultou diretamente no óbito do paciente.")
                st.markdown("<span class='required-field'>* Campos obrigatórios</span>", unsafe_allow_html=True)
            else:
                current_data['patient_id'] = ""
                current_data['patient_outcome_obito'] = UI_TEXTS.selectbox_default_patient_outcome_obito
        st.markdown("---")
    elif st.session_state.form_step == 4:
        with st.container():
            st.markdown("""
            <div class="form-section">
                <h3>📄 Etapa 4: Informações Adicionais e Evidências</h3>
                <p>Complete as informações adicionais e anexe documentos, se aplicável.</p>
            </div>
            """, unsafe_allow_html=True)
            col7, col8 = st.columns(2)
            with col7:
                notified_dept_options = [UI_TEXTS.selectbox_default_department_select] + FORM_DATA.SETORES
                current_data['notified_department'] = st.selectbox(
                    "Setor Notificado*",
                    options=notified_dept_options,
                    index=notified_dept_options.index(current_data['notified_department'])
                      if current_data['notified_department'] in notified_dept_options
                      else 0,
                help="Selecione o setor que será notificado sobre o evento",
                    key="create_notified_dept_refactored"
                )
            with col8:
                current_data['notified_department_complement'] = st.text_input(
                    "Complemento do Setor Notificado", value=current_data['notified_department_complement'],
                    placeholder="Informações adicionais (opcional)",
                    help="Detalhes adicionais sobre o setor notificante (Ex: Equipe A, Sala 101)",
                    key="create_notified_dept_comp_refactored")
            st.markdown("<span class='required-field'>* Campo obrigatório (Setor Notificado)</span>",
unsafe_allow_html=True)
            current_data['additional_notes'] = st.text_area(
                "Observações Adicionais", value=current_data['additional_notes'],
                placeholder="Qualquer outra informação que considere relevante.",
                height=100, key="additional_notes_refactored",
                help="Adicione qualquer outra informação relevante...")
            st.markdown("---")
            st.markdown("###    Documentos e Evidências")
            uploaded_files_list_widget = st.file_uploader(
                "Anexar files relacionados ao evento (Opcional)", type=None, accept_multiple_files=True,
                help="Anexe fotos, documentos...", key="create_attachments_refactored")

            current_data['attachments'] = st.session_state.get('create_attachments_refactored', [])
            if current_data.get('attachments'):
                st.info(
                    f"   {len(current_data['attachments'])} file(s) selecionado(s): {', '.join([f.name for f in current_data['attachments']])}")

            st.markdown("---")

    col_prev, col_cancel_btn, col_next_submit = st.columns(3)
    with col_prev:
        if st.session_state.form_step > 1 and st.session_state.form_step < 5:
            if st.button("◀️ Voltar", key=f"step_back_btn_refactored_{st.session_state.form_step}",
                         use_container_width=True):
                st.session_state.form_step -= 1
                st.rerun()
    with col_cancel_btn:
        if st.session_state.form_step < 5:
            if st.button("🚫 Cancelar Notificação", key="step_cancel_btn_refactored",
                         use_container_width=True):
                _reset_form_state()
                st.rerun()
    with col_next_submit:
        if st.session_state.form_step < 4:
            if st.button(f"➡️ Próximo",
                         key=f"step_next_btn_refactored_{st.session_state.form_step}", use_container_width=True):
                validation_errors = []
                if st.session_state.form_step == 1:
                    if not current_data['title'].strip(): validation_errors.append(
                        'Etapa 1: Título da Notificação é obrigatório.')
                    if not current_data['description'].strip(): validation_errors.append(
                        'Etapa 1: Descrição Detalhada é obrigatória.')
                    if not current_data['location'].strip(): validation_errors.append(
                        'Etapa 1: Local do Evento é obrigatório.')
                    if current_data['occurrence_date'] is None or not isinstance(current_data['occurrence_date'],
                                                                                 dt_date_class): validation_errors.append(
                        'Etapa 1: Data da Ocorrência é obrigatória.')
                    if current_data['reporting_department'] == UI_TEXTS.selectbox_default_department_select:
                        validation_errors.append('Etapa 1: Setor Notificante é obrigatório.')
                    if current_data['event_shift'] == UI_TEXTS.selectbox_default_event_shift: validation_errors.append(
                        'Etapa 1: Turno do Evento é obrigatório.')
                elif st.session_state.form_step == 2:
                    if current_data[
                        'immediate_actions_taken'] == UI_TEXTS.selectbox_default_immediate_actions_taken: validation_errors.append(
                        'Etapa 2: É obrigatório indicar se foram tomadas Ações Imediatas (Sim/Não).')
                    if current_data['immediate_actions_taken'] == "Sim" and not current_data[
                        'immediate_action_description'].strip(): validation_errors.append(
                        "Etapa 2: Descrição das ações imediatas é obrigatória quando há ações imediatas.")
                elif st.session_state.form_step == 3:
                    if current_data[
                        'patient_involved'] == UI_TEXTS.selectbox_default_patient_involved: validation_errors.append(
                        'Etapa 3: É obrigatório indicar se o Paciente foi Afetado (Sim/Não).')
                    if current_data['patient_involved'] == "Sim":
                        if not current_data['patient_id'].strip(): validation_errors.append(
                            "Etapa 3: Número do Atendimento/Prontuário é obrigatório quando paciente é afetado.")
                        if current_data[
                            'patient_outcome_obito'] == UI_TEXTS.selectbox_default_patient_outcome_obito: validation_errors.append(
                            "Etapa 3: Evolução para óbito é obrigatório quando paciente é afetado.")
                if validation_errors:
                    st.error("⚠️ **Por favor, corrija os seguintes erros:**")
                    for error in validation_errors:
                        st.warning(error)
                else:
                    st.session_state.form_step += 1
                    st.rerun()
        elif st.session_state.form_step == 4:
            with st.form("submit_form_refactored_step4", clear_on_submit=False):
                submit_button = st.form_submit_button("📤 Enviar Notificação", use_container_width=True)
                if submit_button:
                    st.subheader("Validando e Enviando Notificação...")
                    validation_errors = []
                    if not current_data['title'].strip(): validation_errors.append(
                        'Etapa 1: Título da Notificação é obrigatório.')
                    if not current_data['description'].strip(): validation_errors.append(
                        'Etapa 1: Descrição Detalhada é obrigatória.')
                    if not current_data['location'].strip(): validation_errors.append(
                        'Etapa 1: Local do Evento é obrigatório.')
                    if current_data['occurrence_date'] is None or not isinstance(current_data['occurrence_date'],
                                                                                 dt_date_class): validation_errors.append(
                        'Etapa 1: Data da Ocorrência é obrigatória.')
                    if not current_data['reporting_department'] or \
                       current_data['reporting_department'] == UI_TEXTS.selectbox_default_department_select:
                        validation_errors.append("Etapa 1: Setor Notificante é obrigatório.")
                    if current_data['event_shift'] == UI_TEXTS.selectbox_default_event_shift: validation_errors.append(
                        'Etapa 1: Turno do Evento é obrigatório.')
                    if current_data[
                        'immediate_actions_taken'] == UI_TEXTS.selectbox_default_immediate_actions_taken: validation_errors.append(
                        'Etapa 2: É obrigatório indicar se foram tomadas Ações Imediatas (Sim/Não).')
                    if current_data['immediate_actions_taken'] == "Sim" and not current_data[
                        'immediate_action_description'].strip(): validation_errors.append(
                        "Etapa 2: Descrição das ações imediatas é obrigatória quando há ações imediatas.")
                    if current_data[
                        'patient_involved'] == UI_TEXTS.selectbox_default_patient_involved: validation_errors.append(
                        'Etapa 3: É obrigatório indicar se o Paciente foi Afetado (Sim/Não).')
                    if current_data['patient_involved'] == "Sim":
                        if not current_data['patient_id'].strip(): validation_errors.append(
                            "Etapa 3: Número do Atendimento/Prontuário é obrigatório quando paciente é afetado.")
                        if current_data[
                            'patient_outcome_obito'] == UI_TEXTS.selectbox_default_patient_outcome_obito: validation_errors.append(
                            "Etapa 3: Evolução para óbito é obrigatório quando paciente é afetado.")
                    if not current_data['notified_department'] or \
                       current_data['notified_department'] == UI_TEXTS.selectbox_default_department_select:
                        validation_errors.append("Etapa 4: Setor Notificado é obrigatório.")
                    if validation_errors:
                        st.error("⚠️ **Por favor, corrija os seguintes erros antes de enviar:**")
                        for error in validation_errors:
                            st.warning(error)
                        st.stop()
                    else:
                        notification_data_to_save = current_data.copy()
                        uploaded_files_list = notification_data_to_save.pop('attachments', [])
                        try:
                            notification = create_notification(notification_data_to_save, uploaded_files_list)
                            st.success(f"✅ **Notificação #{notification['id']} criada com sucesso!**")
                            st.info(
                                "📋 Sua notificação foi enviada para classificação e será processada pela equipe responsável.")
                            with st.expander("   Resumo da Notificação Enviada", expanded=False):
                                occurrence_datetime_summary = format_date_time_summary(
                                    notification_data_to_save.get('occurrence_date'),
                                    notification_data_to_save.get('occurrence_time')
                                )
                                st.write(f"**ID:** #{notification['id']}")
                                st.write(f"**Título:** {notification_data_to_save.get('title', UI_TEXTS.text_na)}")
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
                                    f"**Descrição:** {notification_data_to_save.get('description', UI_TEXTS.text_na)[:200]}..." if len(
                                        notification_data_to_save.get('description',
                                                                      '')) > 200 else notification_data_to_save.get(
                                        'description', UI_TEXTS.text_na))
                                st.write(
                                    f"**Ações Imediatas Tomadas:** {notification_data_to_save.get('immediate_actions_taken', UI_TEXTS.text_na)}")
                                if notification_data_to_save.get('immediate_actions_taken') == 'Sim':
                                    st.write(
                                        f"**Descrição Ações Imediatas:** {notification_data_to_save.get('immediate_action_description', UI_TEXTS.text_na)[:200]}..." if len(
                                            notification_data_to_save.get('immediate_action_description',
                                                                          '')) > 200 else notification_data_to_save.get(
                                            'immediate_action_description', UI_TEXTS.text_na))
                                st.write(
                                    f"**Paciente Envolvido:** {notification_data_to_save.get('patient_involved', UI_TEXTS.text_na)}")
                                if notification_data_to_save.get('patient_involved') == 'Sim':
                                    st.write(
                                        f"**N° Atendimento:** {notification_data_to_save.get('patient_id', UI_TEXTS.text_na)}")
                                    outcome_text = 'Sim' if notification_data_to_save.get(
                                        'patient_outcome_obito') is True else 'Não' if notification_data_to_save.get(
                                        'patient_outcome_obito') is False else 'Não informado'
                                    st.write(f"**Evoluiu para óbito:** {outcome_text}")
                                if notification_data_to_save.get('additional_notes'):
                                    st.write(
                                        f"**Observações Adicionais:** {notification_data_to_save.get('additional_notes', UI_TEXTS.text_na)[:200]}..." if len(
                                            notification_data_to_save.get('additional_notes',
                                                                          '')) > 200 else notification_data_to_save.get(
                                        'additional_notes', UI_TEXTS.text_na))
                                if uploaded_files_list:
                                    st.write(
                                        f"**Anexos:** {len(uploaded_files_list)} file(s) selecionado(s): {', '.join([f.name for f in uploaded_files_list])}")
                                else:
                                    st.write("**Anexos:** Nenhum file anexado.")
                            st.session_state.form_step = 5
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Ocorreu um erro ao finalizar a notificação: {e}")
                            st.warning("Por favor, revise as informações e tente enviar novamente.")

@st_fragment
def show_classificacao_inicial():
    """
    Tela dedicada para classificação inicial de notificações pendentes.
    """
    if not check_permission('classificador'):
        st.error("❌ Acesso negado! Você não tem permissão para acessar esta página.")
        return
    
    st.markdown("<h1 class='main-header'>⏳ Classificação Inicial de Notificações</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    pending_notifications = load_notifications_by_status("pendente_classificacao")
    
    if not pending_notifications:
        st.success("✅ Não há notificações pendentes de classificação inicial no momento.")
        st.info("💡 Todas as notificações foram classificadas ou estão em outras etapas do fluxo.")
        return
    
    st.info(f"📋 **{len(pending_notifications)} notificação(ões)** aguardando classificação inicial")
    
    # Seleção de notificação
    notification_options = []
    for n in pending_notifications:
        created_at_dt = n['created_at']
        if isinstance(created_at_dt, str):
            try:
                created_at_dt = datetime.fromisoformat(created_at_dt)
            except ValueError:
                created_at_dt = None
        
        created_at_formatted = created_at_dt.strftime('%d/%m/%Y %H:%M') if isinstance(created_at_dt, datetime) else str(created_at_dt)
        notification_options.append(f"ID {n['id']} - {n.get('title', 'Sem título')} ({created_at_formatted})")
    
    selected_index_key = "classif_inicial_select"
    if selected_index_key not in st.session_state or st.session_state[selected_index_key] >= len(notification_options) or st.session_state[selected_index_key] < 0:
        st.session_state[selected_index_key] = 0
    
    selected_index = st.selectbox(
        "🔍 Selecione a notificação para classificar:",
        range(len(notification_options)),
        format_func=lambda i: notification_options[i],
        key=selected_index_key
    )
    
    selected_notification = pending_notifications[selected_index]
    notif_id = selected_notification['id']
    
    st.markdown("---")
    
    # Exibir detalhes da notificação
    display_notification_full_details(
        selected_notification, 
        st.session_state.user_id, 
        st.session_state.user_username
    )
    
    st.markdown("---")
    st.markdown("### 📝 Formulário de Classificação")
    
    # ========== DECISÃO: CLASSIFICAR OU REJEITAR ==========
    decisao_key = f"decisao_classif_{notif_id}"
    if decisao_key not in st.session_state:
        st.session_state[decisao_key] = "Selecione"
    
    decisao_options = ["Selecione", "Classificar Notificação", "Rejeitar Notificação"]
    decisao = st.selectbox(
        "🎯 Decisão sobre a Notificação *",
        options=decisao_options,
        key=decisao_key,
        help="Escolha se deseja classificar a notificação ou rejeitá-la"
    )
    
    # ========== FLUXO DE REJEIÇÃO ==========
    if decisao == "Rejeitar Notificação":
        st.markdown("""
        <div class="conditional-field">
            <h4>🚫 Rejeição de Notificação</h4>
            <p>Informe o motivo da rejeição. A notificação será marcada como rejeitada.</p>
        </div>
        """, unsafe_allow_html=True)
        
        motivo_rejeicao_key = f"motivo_rejeicao_{notif_id}"
        motivo_rejeicao = st.text_area(
            "📝 Motivo da Rejeição *",
            key=motivo_rejeicao_key,
            height=200,
            placeholder="Explique detalhadamente o motivo da rejeição da notificação...",
            help="Este motivo será registrado no histórico e será visível para todos os usuários envolvidos",
            value=st.session_state.get(motivo_rejeicao_key, "")
        )
        
        st.markdown("<span class='required-field'>* Campo obrigatório</span>", unsafe_allow_html=True)
        st.markdown("---")
        
        col_reject1, col_reject2 = st.columns(2)
        
        with col_reject1:
            if st.button("🚫 Confirmar Rejeição", use_container_width=True, type="primary"):
                # Validação
                if not motivo_rejeicao or not motivo_rejeicao.strip():
                    st.error("⚠️ **Erro de Validação**")
                    st.warning("📝 O motivo da rejeição é obrigatório.")
                else:
                    # Preparar dados da rejeição
                    rejection_data = {
                        "rejected_by": st.session_state.user_username,
                        "rejected_by_id": st.session_state.user_id,
                        "rejection_reason": motivo_rejeicao.strip(),
                        "rejected_at": datetime.now().isoformat()
                    }
                    
                    # Atualizar notificação
                    updates = {
                        'status': 'rejeitada',
                        'rejection_classification': rejection_data
                    }
                    
                    updated_notif = update_notification(notif_id, updates)
                    
                    if updated_notif:
                        # Adicionar ao histórico
                        add_history_entry(
                            notif_id,
                            "Notificação rejeitada na classificação inicial",
                            st.session_state.user_username,
                            f"Motivo: {motivo_rejeicao.strip()}"
                        )
                        
                        st.success("✅ Notificação rejeitada com sucesso!")
                        st.info("📋 A notificação foi marcada como rejeitada e o histórico foi atualizado.")
                        
                        # Limpar session state
                        st.session_state.pop(decisao_key, None)
                        st.session_state.pop(motivo_rejeicao_key, None)
                        
                        time_module.sleep(1.5)
                        st.rerun()
                    else:
                        st.error("❌ Erro ao rejeitar a notificação. Tente novamente.")
        
        with col_reject2:
            if st.button("❌ Cancelar Rejeição", use_container_width=True):
                st.session_state[decisao_key] = "Selecione"
                st.rerun()
        
        return  # Interrompe execução para não exibir formulário de classificação
    
    # ========== FLUXO DE CLASSIFICAÇÃO ==========
    elif decisao == "Classificar Notificação":
        st.markdown("""
        <div class="form-section">
            <h4>📋 Dados de Classificação</h4>
            <p>Preencha todos os campos obrigatórios.</p>
        </div>
        """, unsafe_allow_html=True)
        
        # CAMPOS FORA DO FORMULÁRIO (DINÂMICOS)
        col_pre1, col_pre2 = st.columns(2)
        
        with col_pre1:
            # CORREÇÃO 3: FORM_DATA.classificacao_nnc (singular)
            classificacao_options = [UI_TEXTS.selectbox_default_classificacao_nnc] + FORM_DATA.classificacao_nnc
            classificacao_key = f"classificacao_{notif_id}"
            classificacao = st.selectbox(
                "📋 Classificação NNC *",
                options=classificacao_options,
                index=classificacao_options.index(st.session_state.get(classificacao_key, UI_TEXTS.selectbox_default_classificacao_nnc)) if st.session_state.get(classificacao_key) in classificacao_options else 0,
                key=classificacao_key,
                help="Selecione o tipo de classificação principal do evento"
            )
        
        with col_pre2:
            nivel_dano = None
            nivel_dano_key = f"nivel_dano_{notif_id}"
            if classificacao == "Evento com dano":
                nivel_dano_options = [UI_TEXTS.selectbox_default_nivel_dano] + FORM_DATA.niveis_dano
                nivel_dano = st.selectbox(
                    "⚠️ Nível de Dano *",
                    options=nivel_dano_options,
                    index=nivel_dano_options.index(st.session_state.get(nivel_dano_key, UI_TEXTS.selectbox_default_nivel_dano)) if st.session_state.get(nivel_dano_key) in nivel_dano_options else 0,
                    key=nivel_dano_key,
                    help="Selecione o nível de dano ao paciente"
                )
            else:
                if nivel_dano_key in st.session_state:
                    st.session_state[nivel_dano_key] = UI_TEXTS.selectbox_default_nivel_dano
        
        st.markdown("---")
        st.markdown("### 📊 Classificação do Tipo de Evento")
        
        # Tipo de evento principal
        tipo_evento_principal_options = [UI_TEXTS.selectbox_default_tipo_principal] + list(FORM_DATA.tipos_evento_principal.keys())
        tipo_evento_principal_key = f"tipo_evento_{notif_id}"
        
        tipo_evento_principal = st.selectbox(
            "Tipo Principal de Evento *",
            options=tipo_evento_principal_options,
            index=tipo_evento_principal_options.index(st.session_state.get(tipo_evento_principal_key, UI_TEXTS.selectbox_default_tipo_principal)) if st.session_state.get(tipo_evento_principal_key) in tipo_evento_principal_options else 0,
            key=tipo_evento_principal_key,
            help="Classificação do tipo principal de evento"
        )
        
        # Subtipo dinâmico
        tipo_evento_sub = []
        tipo_evento_sub_key = f"tipo_evento_sub_{notif_id}"
        
        if tipo_evento_principal and tipo_evento_principal != UI_TEXTS.selectbox_default_tipo_principal:
            sub_options = FORM_DATA.tipos_evento_principal.get(tipo_evento_principal, [])
            
            if sub_options:
                saved_sub_values = st.session_state.get(tipo_evento_sub_key, [])
                valid_defaults = [v for v in saved_sub_values if v in sub_options]
                
                tipo_evento_sub = st.multiselect(
                    f"Especifique o Evento {tipo_evento_principal}: *",
                    options=sub_options,
                    default=valid_defaults,
                    key=tipo_evento_sub_key,
                    help=f"Selecione uma ou mais sub-categorias de {tipo_evento_principal}"
                )
            else:
                st.info(f"ℹ️ Não há especificações adicionais para '{tipo_evento_principal}'")
                if tipo_evento_sub_key in st.session_state:
                    st.session_state[tipo_evento_sub_key] = []
        else:
            if tipo_evento_sub_key in st.session_state:
                st.session_state[tipo_evento_sub_key] = []
        
        st.markdown("---")
        
        # FORMULÁRIO COM OS CAMPOS RESTANTES
        # CORREÇÃO 1: Adicionado st.form() para encapsular os campos e o botão de submit
        with st.form(key=f"form_classif_inicial_{notif_id}"):
            col_form1, col_form2 = st.columns(2)
            
            with col_form1:
                prioridade_options = [UI_TEXTS.selectbox_default_prioridade_resolucao] + FORM_DATA.prioridades
                prioridade = st.selectbox(
                    "🎯 Prioridade *",
                    options=prioridade_options,
                    index=prioridade_options.index(st.session_state.get(f"prioridade_{notif_id}", UI_TEXTS.selectbox_default_prioridade_resolucao)) if st.session_state.get(f"prioridade_{notif_id}") in prioridade_options else 0,
                    key=f"prioridade_{notif_id}",
                    help="Defina a prioridade para investigação e resolução"
                )
                
                never_event_options = [UI_TEXTS.selectbox_never_event_na_text] + FORM_DATA.never_events
                never_event = st.selectbox(
                    "🚨 Never Event *",
                    options=never_event_options,
                    index=never_event_options.index(st.session_state.get(f"never_event_{notif_id}", UI_TEXTS.selectbox_never_event_na_text)) if st.session_state.get(f"never_event_{notif_id}") in never_event_options else 0,
                    key=f"never_event_{notif_id}",
                    help="Selecione se o evento se enquadra como Never Event ou N/A"
                )
            
            with col_form2:
                setor_notificante_options = [UI_TEXTS.selectbox_default_department_select] + FORM_DATA.SETORES
                setor_notificante_default = selected_notification.get('reporting_department', UI_TEXTS.selectbox_default_department_select)
                setor_notificante_index = 0
                if setor_notificante_default in setor_notificante_options:
                    setor_notificante_index = setor_notificante_options.index(setor_notificante_default)
                
                setor_notificante = st.selectbox(
                    "🏥 Setor Notificante *",
                    options=setor_notificante_options,
                    index=setor_notificante_index,
                    key=f"setor_notificante_{notif_id}",
                    help="Setor que notificou o evento"
                )
                
                setor_options = [UI_TEXTS.selectbox_default_department_select] + FORM_DATA.SETORES
                setor_notificado_default = selected_notification.get('notified_department', UI_TEXTS.selectbox_default_department_select)
                setor_notificado_index = 0
                if setor_notificado_default in setor_options:
                    setor_notificado_index = setor_options.index(setor_notificado_default)
                
                setor_responsavel = st.selectbox( # Variável 'setor_responsavel' armazena o valor do 'Setor Notificado'
                    "🏢 Setor Notificado *",
                    options=setor_options,
                    index=setor_notificado_index,
                    key=f"setor_{notif_id}",
                    help="Setor que foi notificado do evento"
                )
            
            evento_sentinela_options = [UI_TEXTS.selectbox_default_evento_sentinela, "Sim", "Não"]
            evento_sentinela = st.selectbox(
                "⚠️ Evento Sentinela? *",
                options=evento_sentinela_options,
                index=evento_sentinela_options.index(st.session_state.get(f"evento_sentinela_{notif_id}", UI_TEXTS.selectbox_default_evento_sentinela)) if st.session_state.get(f"evento_sentinela_{notif_id}") in evento_sentinela_options else 0,
                key=f"evento_sentinela_{notif_id}",
                help="Indique se é um Evento Sentinela"
            )
            
            # CORREÇÃO 4: CAMPO CLASSIFICAÇÃO OMS (ADICIONADO)
            classificacao_oms = st.multiselect(
                "🏥 Classificação OMS: * (selecionar ao menos um)",
                options=FORM_DATA.classificacao_oms,
                default=st.session_state.get(f"classificacao_oms_{notif_id}", []),
                key=f"classificacao_oms_{notif_id}",
                help="Selecione ao menos uma classificação OMS"
            )
            
            # CORREÇÃO 2: get_users_by_role()
            all_executors = get_users_by_role('executor')
            executor_options = [f"{e['name']} ({e['username']})" for e in all_executors]
            
            executores_selecionados = st.multiselect(
                "👥 Atribuir Executores Responsáveis: *",
                options=executor_options,
                default=st.session_state.get(f"executores_{notif_id}", []),
                key=f"executores_{notif_id}",
                help="Selecione os usuários que executarão as ações"
            )
            
            observacoes_classificador = st.text_area(
                "📝 Observações do Classificador (opcional)",
                key=f"obs_classif_{notif_id}",
                height=100,
                placeholder="Adicione observações relevantes sobre a classificação...",
                value=st.session_state.get(f"obs_classif_{notif_id}", "")
            )
            
            st.markdown("<span class='required-field'>* Campos obrigatórios</span>", unsafe_allow_html=True)
            st.markdown("---")
            
            # CORREÇÃO 1: BOTÃO DE SUBMIT DENTRO DO FORMULÁRIO
            submitted = st.form_submit_button("✅ Salvar Classificação", use_container_width=True, type="primary")
            
            if submitted:
                # Validação
                classificacao_final = st.session_state.get(classificacao_key, UI_TEXTS.selectbox_default_classificacao_nnc)
                nivel_dano_final = st.session_state.get(nivel_dano_key, UI_TEXTS.selectbox_default_nivel_dano) if classificacao_final == "Evento com dano" else None
                tipo_evento_principal_final = st.session_state.get(tipo_evento_principal_key, UI_TEXTS.selectbox_default_tipo_principal)
                tipo_evento_sub_final = st.session_state.get(tipo_evento_sub_key, [])
                
                campos_faltantes = []
                
                if decisao == "Selecione":
                    campos_faltantes.append("🎯 Decisão sobre a Notificação")
                
                if classificacao_final == UI_TEXTS.selectbox_default_classificacao_nnc:
                    campos_faltantes.append("📋 Classificação NNC")
                
                if classificacao_final == "Evento com dano" and (nivel_dano_final is None or nivel_dano_final == UI_TEXTS.selectbox_default_nivel_dano):
                    campos_faltantes.append("⚠️ Nível de Dano")
                
                if tipo_evento_principal_final == UI_TEXTS.selectbox_default_tipo_principal:
                    campos_faltantes.append("📊 Tipo Principal de Evento")
                
                if tipo_evento_principal_final != UI_TEXTS.selectbox_default_tipo_principal:
                    sub_options = FORM_DATA.tipos_evento_principal.get(tipo_evento_principal_final, [])
                    if sub_options and not tipo_evento_sub_final:
                        campos_faltantes.append(f"📊 Especifique o Evento {tipo_evento_principal_final}")
                
                if prioridade == UI_TEXTS.selectbox_default_prioridade_resolucao:
                    campos_faltantes.append("🎯 Prioridade")
                
                if setor_notificante == UI_TEXTS.selectbox_default_department_select:
                    campos_faltantes.append("🏥 Setor Notificante")
                
                if setor_responsavel == UI_TEXTS.selectbox_default_department_select:
                    campos_faltantes.append("🏢 Setor Notificado") # Usando o label do campo
                
                if evento_sentinela == UI_TEXTS.selectbox_default_evento_sentinela:
                    campos_faltantes.append("⚠️ Evento Sentinela")
                
                if not classificacao_oms: # Validação para o novo campo
                    campos_faltantes.append("🏥 Classificação OMS")
                
                if not executores_selecionados:
                    campos_faltantes.append("👥 Executores Responsáveis")
                
                if campos_faltantes:
                    st.error("❌ **Os seguintes campos obrigatórios não foram preenchidos:**")
                    for campo in campos_faltantes:
                        st.error(f"   • {campo}")
                    return
                
                # Converter executores
                executor_name_to_id = {
                    f"{e['name']} ({e['username']})": e['id']
                    for e in all_executors
                }
                executor_ids = [executor_name_to_id[name] for name in executores_selecionados if name in executor_name_to_id]
                
                # Calcular prazo
                deadline_days = 0
                if classificacao_final == "Evento com dano" and nivel_dano_final and nivel_dano_final != UI_TEXTS.selectbox_default_nivel_dano:
                    deadline_mapping = DEADLINE_DAYS_MAPPING.get("Evento com dano", {})
                    deadline_days = deadline_mapping.get(nivel_dano_final, 30)
                else:
                    deadline_days = DEADLINE_DAYS_MAPPING.get(classificacao_final, 30)
                
                prazo_conclusao = datetime.now() + timedelta(days=deadline_days)
                
                # Preparar dados
                classification_data = {
                    "nnc": classificacao_final,
                    "nivel_dano": nivel_dano_final if classificacao_final == "Evento com dano" and nivel_dano_final != UI_TEXTS.selectbox_default_nivel_dano else None,
                    "prioridade": prioridade,
                    "never_event": never_event,
                    "event_type_main": tipo_evento_principal_final,
                    "event_type_sub": tipo_evento_sub_final,
                    "oms": classificacao_oms, # Adicionado o campo OMS
                    "is_sentinel_event": (evento_sentinela == "Sim"),
                    "reporting_sector": setor_notificante,
                    "responsible_sector": setor_responsavel, # Armazena o valor do "Setor Notificado"
                    "classified_by": st.session_state.user_username,
                    "classified_at": datetime.now().isoformat(),
                    "observations": observacoes_classificador
                }
                
                updates = {
                    'status': 'classificada_aguardando_execucao',
                    'classification': classification_data,
                    'executors': executor_ids,
                    #'deadline_date': prazo_conclusao.isoformat()
                }
                
                updated_notif = update_notification(notif_id, updates)
                
                if updated_notif:
                    add_history_entry(
                        notif_id,
                        "Classificação inicial realizada",
                        st.session_state.user_username,
                        f"NNC: {classificacao_final}, Prioridade: {prioridade}"
                    )
                    
                    st.success("✅ Classificação salva com sucesso!")
                    st.info(f"📅 Prazo de conclusão: {prazo_conclusao.strftime('%d/%m/%Y')}")
                    
                    # Limpar session state
                    for key in list(st.session_state.keys()):
                        if f"_{notif_id}" in str(key):
                            st.session_state.pop(key, None)
                    st.session_state.pop(decisao_key, None)
                    
                    time_module.sleep(1.5)
                    st.rerun()
                else:
                    st.error("❌ Erro ao salvar a classificação.")


@st_fragment
def show_revisao_execucao():
    """
    Tela dedicada para revisão de execução concluída pelo classificador.
    """
    if not check_permission('classificador'):
        st.error("❌ Acesso negado! Você não tem permissão para acessar esta página.")
        return
    
    st.markdown("<h1 class='main-header'>🛠️ Revisão de Execução</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    review_notifications = load_notifications_by_status("revisao_classificador_execucao")
    
    if not review_notifications:
        st.success("✅ Não há notificações aguardando revisão de execução no momento.")
        st.info("💡 Todas as execuções foram revisadas ou estão em outras etapas do fluxo.")
        return
    
    st.info(f"📋 **{len(review_notifications)} notificação(ões)** aguardando revisão de execução")
    
    notification_options = []
    for n in review_notifications:
        classification = n.get('classification', {})
        if isinstance(classification, str):
            try:
                classification = json.loads(classification)
            except json.JSONDecodeError:
                classification = {}
        
        nnc = classification.get('nnc', 'Sem classificação')
        
        executor_names = "Sem executor"
        if n.get('executors'):
            conn = get_db_connection()
            try:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                # Using sql.Literal for the list of IDs
                query = sql.SQL("SELECT name FROM users WHERE id = ANY({executors_ids})").format(
                    executors_ids=sql.Literal(n['executors'])
                )
                cursor.execute(query)
                executors = cursor.fetchall()
                if executors:
                    executor_names = ", ".join([e['name'] for e in executors])
            except Exception as e:
                st.warning(f"Erro ao buscar executores para notificação {n['id']}: {e}")
            finally:
                conn.close()
        
        created_at_formatted = datetime.fromisoformat(n['created_at']).strftime('%d/%m/%Y %H:%M') if isinstance(n['created_at'], str) else n['created_at'].strftime('%d/%m/%Y %H:%M')

        notification_options.append(f"ID {n['id']} - {n.get('title', 'Sem título')} - {nnc} ({executor_names}) ({created_at_formatted})")
    
    selected_option_key = f"revisao_exec_select"
    if selected_option_key not in st.session_state or st.session_state[selected_option_key] >= len(notification_options) or st.session_state[selected_option_key] < 0:
        st.session_state[selected_option_key] = 0
    
    selected_index = st.selectbox(
        "🔍 Selecione a notificação para revisar:",
        range(len(notification_options)),
        format_func=lambda i: notification_options[i],
        key=selected_option_key
    )
    
    selected_notification = review_notifications[selected_index]
    notif_id = selected_notification['id']
    
    classification = selected_notification.get('classification', {})
    if isinstance(classification, str):
        try:
            classification = json.loads(classification)
        except json.JSONDecodeError:
            classification = {}
    
    st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown(f"### 📄 {selected_notification.get('title', 'Sem título')}")
        st.markdown(f"**Descrição:** {selected_notification.get('description', 'Sem descrição')}")
        st.markdown(f"**Classificação:** `{classification.get('nnc', 'N/A')}`")
        st.markdown(f"**Prioridade:** `{classification.get('prioridade', 'N/A')}`")
        responsible_sector_display = classification.get('responsible_sector', UI_TEXTS.text_na)
        st.markdown(f"**Setor Responsável:** {responsible_sector_display}")
    
    with col2:
        st.markdown("**📊 Informações**")
        st.markdown(f"**ID:** {notif_id}")
        st.markdown(f"**Status:** `{selected_notification['status']}`")
        
        deadline_str = classification.get('deadline')
        if deadline_str:
            try:
                prazo = datetime.fromisoformat(deadline_str)
                dias_restantes = (prazo.date() - dt_date_class.today()).days
                
                if dias_restantes < 0:
                    status_prazo = "🔴 Atrasada"
                elif dias_restantes <= 7:
                    status_prazo = "🟡 Prazo Próximo"
                else:
                    status_prazo = "🟢 No Prazo"
                
                st.markdown(f"**Prazo:** {prazo.strftime('%d/%m/%Y')}")
                st.markdown(f"**Situação:** {status_prazo}")
            except Exception as e:
                st.warning(f"Erro ao processar prazo: {e}")
    
    st.markdown("---")

    # NOVO: Expander com detalhes completos da notificação
    with st.expander("📋 Detalhes Completos da Notificação", expanded=False):
        st.session_state.user_id = st.session_state.get('user_id', 1) # Mock user_id for example
        st.session_state.user_username = st.session_state.get('user_username', 'classificador_teste') # Mock user_username for example
        display_notification_full_details(
            selected_notification, 
            st.session_state.user_id, 
            st.session_state.user_username
        )
    
    st.markdown("### 🔧 Ações Realizadas pelos Executores")
    
    actions = _get_actions_map_by_ids(get_db_connection(), [notif_id]).get(notif_id, [])

    if not actions:
        st.warning("⚠️ Nenhuma ação registrada pelos executores ainda.")
    else:
        for idx, action in enumerate(actions, 1):
            action_label = f"📌 Ação {idx} - {action.get('executor_name', 'Executor desconhecido')}"
            action_timestamp_display = ""
            if action.get('timestamp'):
                try:
                    action_timestamp_display = datetime.fromisoformat(action['timestamp']).strftime('%d/%m/%Y %H:%M')
                except ValueError:
                    action_timestamp_display = action['timestamp']
            
            action_label += f" em {action_timestamp_display}"
            
            with st.expander(action_label):
                st.markdown(f"**Descrição da Ação:**")
                st.markdown(action.get('description', 'Sem descrição'))
                
                if action.get('evidence_description'):
                    st.markdown(f"**Evidências:**")
                    st.markdown(action['evidence_description'])
                
                if action.get('evidence_attachments'):
                    st.markdown("**📎 Anexos de Evidência:**")
                    anexos = action['evidence_attachments']
                    if isinstance(anexos, list):
                        for att_idx, anexo in enumerate(anexos):
                            unique_name = anexo.get('unique_name')
                            original_name = anexo.get('original_name')
                            if unique_name and original_name:
                                file_content = get_attachment_data(unique_name)
                                if file_content:
                                    st.download_button(
                                        f"⬇️ {original_name}",
                                        file_content,
                                        file_name=original_name,
                                        key=f"download_evidencia_{action['executor_id']}_{unique_name}_{att_idx}"
                                    )
                                else:
                                    st.write(f"Anexo: {original_name} (arquivo não encontrado ou corrompido)")
                    else:
                        st.warning("Formato de evidências anexadas inválido.")
    
    st.markdown("---")
    st.markdown("## ✅ Revisão da Execução")
    
    with st.form(key=f"form_revisao_{notif_id}"):
        decisao_options = [UI_TEXTS.selectbox_default_decisao_revisao, "✅ Aprovar Execução", "🔄 Solicitar Correções"]
        decisao_key = f"decisao_revisao_{notif_id}"
        if decisao_key not in st.session_state:
            st.session_state[decisao_key] = UI_TEXTS.selectbox_default_decisao_revisao

        decisao = st.radio(
            "📋 Decisão da Revisão *",
            options=decisao_options,
            index=decisao_options.index(st.session_state[decisao_key]) if st.session_state[decisao_key] in decisao_options else 0,
            key=decisao_key,
            help="Aprovar envia para aprovação final. Solicitar correções retorna para execução."
        )
        
        observacoes_revisao = st.text_area(
            "📝 Observações da Revisão *",
            key=f"obs_revisao_{notif_id}",
            height=150,
            placeholder="Descreva sua análise da execução. Se solicitar correções, especifique o que precisa ser ajustado.",
            help="Campo obrigatório",
            value=st.session_state.get(f"obs_revisao_{notif_id}", "")
        )
        
        st.markdown("<span class='required-field'>* Campos obrigatórios</span>", unsafe_allow_html=True)
        st.markdown("---")
        submitted = st.form_submit_button("💾 Salvar Revisão", use_container_width=True, type="primary")
        
        if submitted:
            if decisao == UI_TEXTS.selectbox_default_decisao_revisao:
                st.error("❌ Por favor, selecione uma decisão para a revisão!")
                return

            if not observacoes_revisao.strip():
                st.error("❌ Por favor, preencha as observações da revisão!")
                return
            
            new_status = ""
            acao_historico = ""
            mensagem_sucesso = ""
            rejection_reason = None

            if decisao == "✅ Aprovar Execução":
                new_status = "aguardando_aprovacao"
                acao_historico = "Revisão Aprovada"
                mensagem_sucesso = "✅ Execução aprovada! Notificação enviada para aprovação final."
            else:
                new_status = "em_execucao"
                acao_historico = "Revisão Rejeitada (Correções Solicitadas)"
                mensagem_sucesso = "🔄 Correções solicitadas! Notificação retornou para execução."
                rejection_reason = observacoes_revisao
            
            review_data = {
                "decision": decisao,
                "observations": observacoes_revisao,
                "reviewed_at": datetime.now().isoformat(),
                "reviewed_by": st.session_state.user_id,
                "reviewed_by_name": st.session_state.user_username
            }
            if rejection_reason:
                review_data["rejection_reason"] = rejection_reason

            updates = {
                'status': new_status,
                'review_execution': review_data,
            }
            if new_status == 'em_execucao':
                 updates['rejection_execution_review'] = None
            elif new_status == 'aguardando_aprovacao':
                current_notification = next((n for n in load_notifications() if n['id'] == notif_id), None)
                if current_notification and current_notification.get('classification'):
                    classif_data = current_notification['classification']
                    if isinstance(classif_data, str):
                        try: classif_data = json.loads(classif_data)
                        except json.JSONDecodeError: pass
                    
                    if isinstance(classif_data, dict) and classif_data.get('approver_id'):
                        updates['approver'] = classif_data['approver_id']

            updated_notif = update_notification(notif_id, updates)

            if updated_notif:
                add_history_entry(
                    notif_id,
                    acao_historico,
                    st.session_state.user_username,
                    f"{observacoes_revisao}"
                )
                
                st.success(mensagem_sucesso)
                time_module.sleep(1.5)
                st.session_state.pop(f"decisao_revisao_{notif_id}", None)
                st.session_state.pop(f"obs_revisao_{notif_id}", None)
                st.rerun()
                
            else:
                st.error(f"❌ Erro ao salvar revisão.")

@st_fragment
def show_notificacoes_encerradas():
    """
    Tela dedicada para visualização de notificações encerradas.
    """
    if not check_permission('classificador'):
        st.error("❌ Acesso negado! Você não tem permissão para acessar esta página.")
        return
    
    st.markdown("<h1 class='main-header'>✅ Notificações Encerradas</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    closed_statuses = ['aprovada', 'rejeitada', 'reprovada', 'concluida']
    closed_notifications = load_notifications_by_statuses(closed_statuses)
    
    if not closed_notifications:
        st.info("📭 Não há notificações encerradas no momento.")
        return
    
    st.success(f"📊 **{len(closed_notifications)} notificação(ões)** encerradas")
    
    col_filter1, col_filter2, col_filter3 = st.columns(3)
    
    with col_filter1:
        filtro_status_options = [UI_TEXTS.multiselect_all_option] + closed_statuses
        filtro_status = st.multiselect(
            "🏷️ Filtrar por Status",
            options=filtro_status_options,
            default=UI_TEXTS.multiselect_all_option,
            key="filtro_status_encerradas"
        )
        if UI_TEXTS.multiselect_all_option in filtro_status:
            filtro_status = closed_statuses
    
    with col_filter2:
        classificacoes_disponiveis = []
        for n in closed_notifications:
            classification = n.get('classification')
            if classification:
                if isinstance(classification, str):
                    try:
                        classification = json.loads(classification)
                    except json.JSONDecodeError:
                        classification = None
                
                if classification:
                    nnc = classification.get('nnc')
                    if nnc and nnc not in classificacoes_disponiveis:
                        classificacoes_disponiveis.append(nnc)
        
        if not classificacoes_disponiveis:
            classificacoes_disponiveis_with_placeholder = [UI_TEXTS.multiselect_all_option, "Sem classificação"]
        else:
            classificacoes_disponiveis_with_placeholder = [UI_TEXTS.multiselect_all_option] + sorted(classificacoes_disponiveis)

        filtro_classificacao = st.multiselect(
            "📋 Filtrar por Classificação",
            options=classificacoes_disponiveis_with_placeholder,
            default=UI_TEXTS.multiselect_all_option,
            key="filtro_classif_encerradas"
        )
        if UI_TEXTS.multiselect_all_option in filtro_classificacao:
            filtro_classificacao = classificacoes_disponiveis
        elif "Sem classificação" in filtro_classificacao and "Sem classificação" not in classificacoes_disponiveis:
            filtro_classificacao = []
    
    with col_filter3:
        setores_disponiveis = []
        for n in closed_notifications:
            classification = n.get('classification')
            if classification:
                if isinstance(classification, str):
                    try:
                        classification = json.loads(classification)
                    except json.JSONDecodeError:
                        classification = None
                
                if classification:
                    setor = classification.get('responsible_sector')
                    if setor and setor not in setores_disponiveis:
                        setores_disponiveis.append(setor)
        
        if not setores_disponiveis:
            setores_disponiveis_with_placeholder = [UI_TEXTS.multiselect_all_option, "Sem setor"]
        else:
            setores_disponiveis_with_placeholder = [UI_TEXTS.multiselect_all_option] + sorted(setores_disponiveis)
        
        filtro_setor = st.multiselect(
            "🏢 Filtrar por Setor",
            options=setores_disponiveis_with_placeholder,
            default=UI_TEXTS.multiselect_all_option,
            key="filtro_setor_encerradas"
        )
        if UI_TEXTS.multiselect_all_option in filtro_setor:
            filtro_setor = setores_disponiveis
        elif "Sem setor" in filtro_setor and "Sem setor" not in setores_disponiveis:
            filtro_setor = []
    
    filtered_notifications = []
    for n in closed_notifications:
        match = True

        if n['status'] not in filtro_status:
            match = False
        
        if match:
            classification = n.get('classification')
            if isinstance(classification, str):
                try:
                    classification = json.loads(classification)
                except json.JSONDecodeError:
                    classification = {}
            elif classification is None:
                classification = {}

            nnc = classification.get('nnc')
            if filtro_classificacao:
                if nnc not in filtro_classificacao and not (nnc is None and "Sem classificação" in filtro_classificacao):
                    match = False
            
            if match:
                setor = classification.get('responsible_sector')
                if filtro_setor:
                    if setor not in filtro_setor and not (setor is None and "Sem setor" in filtro_setor):
                        match = False
        
        if match:
            filtered_notifications.append(n)
    
    st.markdown("---")
    st.info(f"🔍 Exibindo **{len(filtered_notifications)}** notificação(ões) após filtros")
    
    if not filtered_notifications:
        st.warning("⚠️ Nenhuma notificação encontrada com os filtros selecionados.")
        return
    
    # FUNÇÃO AUXILIAR PARA CONVERTER PARA DATETIME
    def to_datetime(value):
        """Converte string ou datetime para datetime object"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except (ValueError, TypeError):
                return None
        return None
    
    df_data = []
    for n in filtered_notifications:
        status_icons = {
            'aprovada': '✅',
            'concluida': '✅',
            'rejeitada': '❌',
            'reprovada': '🔴'
        }
        status_icon = status_icons.get(n['status'], '❓')
        
        # CORREÇÃO: Conversão segura de datas
        tempo_resolucao = "N/A"
        created_dt = to_datetime(n.get('created_at'))
        updated_dt = to_datetime(n.get('updated_at'))
        
        if created_dt and updated_dt:
            delta = updated_dt - created_dt
            dias = delta.days
            tempo_resolucao = f"{dias} dia(s)"
        
        classification = n.get('classification')
        if classification is None:
            classification = {}
        elif isinstance(classification, str):
            try:
                classification = json.loads(classification)
            except json.JSONDecodeError:
                classification = {}
        
        if not isinstance(classification, dict):
            classification = {}
        
        # Formatação segura de datas para exibição
        created_str = created_dt.strftime('%d/%m/%Y') if created_dt else 'N/A'
        updated_str = updated_dt.strftime('%d/%m/%Y') if updated_dt else 'N/A'
        
        df_data.append({
            'ID': n['id'],
            'Status': f"{status_icon} {n['status']}",
            'Título': n.get('title', 'Sem título'),
            'Classificação': classification.get('nnc', 'N/A'),
            'Prioridade': classification.get('prioridade', 'N/A'),
            'Setor': classification.get('responsible_sector', 'N/A'),
            'Criado em': created_str,
            'Encerrado em': updated_str,
            'Tempo': tempo_resolucao
        })
    
    df = pd.DataFrame(df_data)
    
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            'ID': st.column_config.NumberColumn('ID', width='small'),
            'Status': st.column_config.TextColumn('Status', width='medium'),
            'Título': st.column_config.TextColumn('Título', width='large'),
            'Classificação': st.column_config.TextColumn('Classificação', width='medium'),
            'Prioridade': st.column_config.TextColumn('Prioridade', width='small'),
            'Setor': st.column_config.TextColumn('Setor', width='medium'),
            'Criado em': st.column_config.TextColumn('Criado', width='small'),
            'Encerrado em': st.column_config.TextColumn('Encerrado', width='small'),
            'Tempo': st.column_config.TextColumn('Tempo', width='small')
        }
    )
    
    st.markdown("---")
    
    st.markdown("### 🔍 Visualizar Detalhes")
    
    notification_display_options = [UI_TEXTS.selectbox_default_notification_select] + [
        f"ID {n['id']} - {n.get('title', 'Sem título')} ({n['status']})"
        for n in filtered_notifications
    ]

    selected_detail_option_key = f"detalhes_encerrada_select"
    if selected_detail_option_key not in st.session_state or st.session_state[selected_detail_option_key] >= len(notification_display_options) or st.session_state[selected_detail_option_key] < 0:
        st.session_state[selected_detail_option_key] = 0
    
    selected_index_details = st.selectbox(
        "Selecione uma notificação para ver detalhes completos:",
        range(len(notification_display_options)),
        format_func=lambda i: notification_display_options[i],
        key=selected_detail_option_key
    )
    
    if notification_display_options[selected_index_details] != UI_TEXTS.selectbox_default_notification_select:
        selected_notification = filtered_notifications[selected_index_details - 1]
        
        with st.expander(f"📋 **Ver Detalhes Completos da Notificação {selected_notification['id']}**", expanded=True):
            display_notification_full_details(selected_notification, st.session_state.user_id, st.session_state.user_username)

@st_fragment
def show_execution():
    """Renderiza a página para executores visualizarem notificações atribuídas e registrarem ações."""
    if not check_permission('executor'):
        st.error("❌ Acesso negado! Você não tem permissão para executar notificações.")
        return
    st.markdown("<h1 class='main-header'>⚡ Execução de Notificações</h1>", unsafe_allow_html=True)
    st.info(
        "Nesta página, você pode visualizar as notificações atribuídas a você, registrar as ações executadas e marcar sua parte como concluída.")
    all_notifications = load_notifications()
    user_id_logged_in = st.session_state.user_id
    user_username_logged_in = st.session_state.user_username

    all_users = load_users()
    display_name_to_id_map = {
        f"{user.get('name', UI_TEXTS.text_na)} ({user.get('username', UI_TEXTS.text_na)})": user['id']
        for user in all_users
    }
    user_active_notifications = []
    active_execution_statuses = ['classificada', 'em_execucao']
    for notification in all_notifications:
        is_assigned_to_current_user = False
        assigned_executors_raw = notification.get('executors', [])
        for executor_entry in assigned_executors_raw:
            if isinstance(executor_entry, int) and executor_entry == user_id_logged_in:
                is_assigned_to_current_user = True
                break
        if is_assigned_to_current_user and notification.get('status') in active_execution_statuses:
            user_active_notifications.append(notification)
    closed_statuses = ['aprovada', 'rejeitada', 'reprovada', 'concluida']
    closed_my_exec_notifications = [
        n for n in all_notifications
        if n.get('status') in closed_statuses and user_id_logged_in in n.get('executors', [])
    ]

    if not user_active_notifications and not closed_my_exec_notifications:
        st.info("✅ Não há notificações ativas atribuídas a você no momento. Verifique com seu gestor ou classificador.")
        return
    st.success(f"Você tem {len(user_active_notifications)} notificação(ões) atribuída(s) aguardando ou em execução.")
    tab_active_notifications, tab_closed_my_exec_notifs = st.tabs(
        ["🔄 Notificações Atribuídas (Ativas)", f"✅ Minhas Ações Encerradas ({len(closed_my_exec_notifications)})"])
    with tab_active_notifications:
        st.markdown("### Notificações Aguardando ou Em Execução")
        priority_order = {p: i for i, p in enumerate(FORM_DATA.prioridades)}
        user_active_notifications.sort(key=lambda x: (
            priority_order.get(x.get('classification', {}).get('prioridade', UI_TEXTS.text_na), len(FORM_DATA.prioridades)),
            datetime.fromisoformat(x.get('created_at', '1900-01-01T00:00:00')).timestamp()
        ))
        for notification in user_active_notifications:
            status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
            classif_info = notification.get('classification') or {}
            if isinstance(classif_info, str):
                try:
                    classif_info = json.loads(classif_info)
                except json.JSONDecodeError:
                    classif_info = {}

            prioridade_display = classif_info.get('prioridade', UI_TEXTS.text_na)
            prioridade_display = prioridade_display if prioridade_display != UI_TEXTS.selectbox_default_prioridade_resolucao else f"{UI_TEXTS.text_na} (Não Classificado)"
            deadline_date_str = classif_info.get('deadline')
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
            with st.expander(
                    f"✨ Ver Detalhes Completos e Classificação - Notificação #{notification.get('id', UI_TEXTS.text_na)}"):
                display_notification_full_details(notification, user_id_logged_in, user_username_logged_in)
            if notification.get('actions'):
                st.markdown("#### ⚡ Histórico de Ações Realizadas")
                with st.expander(
                        f"Ver histórico de ações para Notificação #{notification.get('id', UI_TEXTS.text_na)}"):
                    sorted_actions = sorted(notification['actions'], key=lambda x: x.get('timestamp', '')) # CORREÇÃO: Removido o cólon aqui
                    for action in sorted_actions:
                        action_type = "🏁 CONCLUSÃO (Executor)" if action.get(
                            'final_action_by_executor') else "📝 AÇÃO Registrada"
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
                                <strong>{action_type}</strong> - por <strong>VOCÊ ({action.get('executor_name', UI_TEXTS.text_na)})</strong> em {action_timestamp}
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
                        if action.get('final_action_by_executor'):
                            evidence_desc = action.get('evidence_description', '').strip()
                            evidence_atts = action.get('evidence_attachments', [])
                            if evidence_desc or evidence_atts:
                                st.markdown(f"""<div class='evidence-section'>""", unsafe_allow_html=True)
                                st.markdown("<h6>Evidências da Conclusão:</h6>", unsafe_allow_html=True)
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
                                                    label=f"Baixar Evidência: {original_name}",
                                                    data=file_content,
                                                    file_name=original_name,
                                                    mime="application/octet-stream",
                                                    key=f"download_action_evidence_exec_{notification['id']}_{unique_name}"
                                                )
                                            else:
                                                st.write(
                                                    f"Anexo: {original_name} (arquivo não encontrado ou corrompido)")
                                st.markdown(f"""</div>""", unsafe_allow_html=True)
                        st.markdown("---")
            executor_has_already_concluded_their_part = False
            if user_id_logged_in:
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
                    f"✅ Sua parte na execução da Notificação #{notification.get('id')} já foi concluída. Você não pode adicionar mais ações para esta notificação.")
            else:
                st.markdown("### 📝 Registrar Ação Executada ou Concluir Sua Parte")
                action_type_choice_options = [UI_TEXTS.selectbox_default_acao_realizar, "Registrar Ação",
                                              "Concluir Minha Parte"]
                st.selectbox(
                    "Qual ação deseja realizar?*", options=action_type_choice_options,
                    key=action_choice_key,
                    index=action_type_choice_options.index(st.session_state[action_choice_key]),
                    help="Selecione 'Registrar Ação' para adicionar um passo ao histórico ou 'Concluir Minha Parte' para finalizar sua execução."
                )
                with st.form(f"action_form_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                             clear_on_submit=False):
                    action_description_state = st.text_area(
                        "Descrição detalhada da ação realizada*",
                        value=st.session_state.get(
                            f"exec_action_desc_{notification.get('id', UI_TEXTS.text_na)}_refactored", ""),
                        placeholder="Descreva:\\n• O QUÊ foi feito?\\n• POR QUÊ foi feito (qual o objetivo)?\\n• ONDE foi realizado?\\n• QUANDO foi realizado (data/hora)?\\n• QUEM executou (se aplicável)?\\n• COMO foi executado (passos, métodos)?\\n• QUANTO CUSTOU (recursos, tempo)?\\n• QUÃO FREQUENTE (se for uma ação contínua)?",
                        height=180,
                        key=f"exec_action_desc_{notification.get('id', UI_TEXTS.text_na)}_refactored"
                    ).strip()
                    evidence_description_state = ""
                    uploaded_evidence_files = []
                    if st.session_state[action_choice_key] == "Concluir Minha Parte":
                        st.markdown("""
                           <div class="conditional-field">
                               <h4>✅ Evidências da Tratativa</h4>
                               <p>Descreva e anexe as evidências da tratativa realizada para esta notificação.</p>
                           </div>
                           """, unsafe_allow_html=True)
                        evidence_description_state = st.text_area(
                            "Descrição da Evidência (Opcional)",
                            value=st.session_state.get(
                                f"exec_evidence_desc_{notification.get('id', UI_TEXTS.text_na)}_refactored", ""),
                            placeholder="Descreva o resultado da tratativa, evidências de conclusão, etc.",
                            height=100,
                            key=f"exec_evidence_desc_{notification.get('id', UI_TEXTS.text_na)}_refactored"
                        ).strip()
                        uploaded_evidence_files = st.file_uploader(
                            "Anexar files de Evidência (Opcional)", type=None, accept_multiple_files=True,
                            key=f"exec_evidence_attachments_{notification.get('id', UI_TEXTS.text_na)}_refactored"
                        )
                    st.markdown("<span class='required-field'>* Campo obrigatório (Descrição da Ação)</span>", unsafe_allow_html=True)
                    submit_button = st.form_submit_button("✔️ Confirmar Ação",
                                                          use_container_width=True)
                    st.markdown("---")
                    if submit_button:
                        validation_errors = []
                        if st.session_state[action_choice_key] == UI_TEXTS.selectbox_default_acao_realizar:
                            validation_errors.append("É obrigatório selecionar o tipo de ação (Registrar ou Concluir).")
                        if not action_description_state:
                            validation_errors.append("A descrição detalhada da ação é obrigatória.")
                        if validation_errors:
                            st.error("⚠️ **Por favor, corrija os seguintes erros:**")
                            for error in validation_errors: st.warning(error)
                        else:
                            current_notification_in_list = next(
                                (n for n in load_notifications() if n.get('id') == notification.get('id')), None)
                            if not current_notification_in_list:
                                st.error(
                                    "Erro interno: Notificação não encontrada na lista principal para atualização.")
                            else:
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
                                        "❌ Sua parte nesta notificação já foi marcada como concluída anteriormente. Operação abortada.")
                                    st.session_state[action_choice_key] = UI_TEXTS.selectbox_default_acao_realizar
                                    _clear_execution_form_state(notification['id'])
                                    st.rerun()
                                else:
                                    saved_evidence_attachments = []
                                    if st.session_state[
                                        action_choice_key] == "Concluir Minha Parte" and uploaded_evidence_files:
                                        for file in uploaded_evidence_files:
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
                                    add_notification_action(notification['id'], action_data_to_add)
                                    if st.session_state[action_choice_key] == "Registrar Ação":
                                        if current_notification_in_list.get('status') == 'classificada':
                                            update_notification(notification['id'], {'status': 'em_execucao'})
                                        add_history_entry(notification['id'],
                                                          "Ação registrada (Execução)",
                                                          user_username_logged_in,
                                                          f"Registrou ação: {action_description_state[:100]}..." if len(
                                                              action_description_state) > 100 else f"Registrou ação: {action_description_state}")
                                        st.toast("✅ Ação registrada com sucesso!", icon="🎉")
                                    elif st.session_state[action_choice_key] == "Concluir Minha Parte":
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
                                                "✅ Todos os executores concluíram suas partes. Notificação encaminhada para revisão!",
                                                icon="🏁")
                                        else:
                                            st.toast("✅ Sua execução foi concluída nesta notificação!", icon="✅")
                                        history_details = f"Executor {user_username_logged_in} concluiu sua parte das ações."
                                        add_history_entry(
                                            notification['id'],
                                            "Execução concluída (por executor)",
                                            user_username_logged_in,
                                            history_details
                                        )
                                        if updates_to_status:
                                            update_notification(notification['id'], updates_to_status)
                                        st.success(
                                            f"✅ Sua execução foi concluída nesta notificação! Status atual: '{current_notification_in_list['status'].replace('_', ' ').title()}'.")
                                        if not all_executors_concluded:
                                            users_list_exec = load_users()
                                            remaining_executors_ids = list(
                                                all_assigned_executors_ids - executors_who_concluded_ids)
                                            remaining_executors_names = [u.get('name', UI_TEXTS.text_na) for u in
                                                                         users_list_exec if
                                                                         u.get('id') in remaining_executors_ids]
                                            st.info(
                                                f"Aguardando conclusão dos seguintes executores: {', '.join(remaining_executors_names) or 'Nenhum'}.")
                                        elif all_executors_concluded:
                                            st.info(
                                                f"Todos os executores concluíram suas partes. A notificação foi enviada para revisão final pelo classificador.\\n\\nEvidência da tratativa:\\n{evidence_description_state}\\n\\nAnexos: {len(saved_evidence_attachments) if saved_evidence_attachments else 0}")
                                    _clear_execution_form_state(notification['id'])
                                    st.rerun()
                with st.expander("👥 Adicionar Executor Adicional"):
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
                            default_add_executor_selection = UI_TEXTS.multiselect_instruction_placeholder
                            new_executor_name_to_add_raw = st.selectbox(
                                "Selecionar executor para adicionar:*",
                                options=add_executor_display_options,
                                index=add_executor_display_options.index(default_add_executor_selection),
                                key=f"add_executor_select_exec_{notification.get('id', UI_TEXTS.text_na)}_form_refactored",
                                help="Selecione o usuário executor que será adicionado a esta notificação."
                            )
                            new_executor_name_to_add = (
                                new_executor_name_to_add_raw
                                if new_executor_name_to_add_raw != UI_TEXTS.multiselect_instruction_placeholder
                                else None
                            )
                            st.markdown("<span class='required-field'>* Campo obrigatório</span>",
                                        unsafe_allow_html=True)
                            submit_button = st.form_submit_button("➕ Adicionar Executor",
                                                                  use_container_width=True)
                            if submit_button:
                                if new_executor_name_to_add:
                                    new_executor_id = executor_options[new_executor_name_to_add]
                                    current_notification_in_list = next(
                                        (n for n in load_notifications() if n.get('id') == notification.get('id')), None)
                                    if current_notification_in_list:
                                        updated_executors = current_notification_in_list.get('executors', []) + [
                                            new_executor_id]
                                        update_notification(notification.get('id'), {'executors': updated_executors})
                                        add_history_entry(
                                            notification.get('id'), "Executor adicionado (durante execução)",
                                            user_username_logged_in,
                                            f"Adicionado o executor: {new_executor_name_to_add}"
                                        )
                                        st.success(
                                            f"✅ {new_executor_name_to_add} adicionado como executor para esta notificação.")
                                        st.rerun()
                                    else:
                                        st.error("Erro: Notificação não encontrada para adicionar executor.")
                                else:
                                    st.error("⚠️ Por favor, selecione um executor para adicionar.")
                        else:
                            st.info("Não há executores adicionais disponíveis para atribuição no momento.")
    with tab_closed_my_exec_notifs:
        st.markdown("### Minhas Ações Encerradas")
        if not closed_my_exec_notifications:
            st.info("✅ Não há notificações encerradas em que você estava envolvido como executor no momento.")
        else:
            st.info(
                f"Total de notificações encerradas em que você estava envolvido: {len(closed_my_exec_notifications)}.")
            search_query_exec_closed = st.text_input(
                "🔎 Buscar em Minhas Ações Encerradas (Título, Descrição, ID):",
                key="closed_exec_notif_search_input",
                placeholder="Ex: 'reparo', '987', 'instalação'"
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
                    "⚠️ Nenhuma notificação encontrada com os critérios de busca especificados em suas ações encerradas.")
            else:
                filtered_closed_my_exec_notifications.sort(key=lambda x: x.get('created_at', ''), reverse=True)
                st.markdown(f"**Notificações Encontradas ({len(filtered_closed_my_exec_notifications)})**:")
                for notification in filtered_closed_my_exec_notifications:
                    status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
                    created_at_str = datetime.fromisoformat(notification['created_at']).strftime('%d/%m/%Y %H:%M:%S') if isinstance(notification['created_at'], str) else notification['created_at'].strftime('%d/%m/%Y %H:%M:%S')

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
                    
                    classif_info = notification.get('classification', {})
                    if isinstance(classif_info, str):
                        try:
                            classif_info = json.loads(classif_info)
                        except json.JSONDecodeError:
                            classif_info = {}

                    deadline_info = classif_info.get('deadline')
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
                                <p><strong>Encerrada por:</strong> {concluded_by} | <strong>Data de Criação:</strong> {created_at_str}</p>
                                <p><strong>Prazo:</strong> {deadline_status['text']} (class={deadline_status['class']})</p>
                            </div>
                            """, unsafe_allow_html=True)
                    with st.expander(
                            f"👁️ Visualizar Detalhes - Notificação #{notification.get('id', UI_TEXTS.text_na)}"):
                        display_notification_full_details(notification, user_id_logged_in, user_username_logged_in)
@st_fragment
def show_approval():
    """Renderiza a página para aprovadores revisarem e aprovarem/rejeitarem notificações."""
    if not check_permission('aprovador'):
        st.error("❌ Acesso negado! Você não tem permissão para aprovar notificações.")
        return

    st.markdown("<h1 class='main-header'>✅ Aprovação de Notificações</h1>", unsafe_allow_html=True)
    st.info(
        "📋 Analise as notificações que foram concluídas pelos executores e revisadas/aceitas pelo classificador, e que requerem sua aprovação final.")
    all_notifications = load_notifications()
    user_id_logged_in = st.session_state.user_id
    user_username_logged_in = st.session_state.user_username
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
        st.info("✅ Não há notificações aguardando sua aprovação ou que foram encerradas por você no momento.")
        return

    st.success(f"⏳ Você tem {len(pending_approval)} notificação(ões) aguardando sua aprovação.")

    tab_pending_approval, tab_closed_my_approval_notifs = st.tabs(
        ["⏳ Aguardando Minha Aprovação", f"✅ Minhas Aprovações Encerradas ({len(closed_my_approval_notifications)})"])
    with tab_pending_approval:
        priority_order = {p: i for i, p in enumerate(FORM_DATA.prioridades)}
        pending_approval.sort(key=lambda x: (
            priority_order.get(x.get('classification', {}).get('prioridade', 'Baixa'), len(FORM_DATA.prioridades)),
            datetime.fromisoformat(
                x.get('classification', {}).get('classified_at',
                                                '1900-01-01T00:00:00')).timestamp() if x.get(
                'classification', {}).get('classified_at') else 0
        ))
        for notification in pending_approval:
            status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
            classif_info = notification.get('classification') or {}
            if isinstance(classif_info, str):
                try:
                    classif_info = json.loads(classif_info)
                except json.JSONDecodeError:
                    classif_info = {}

            prioridade_display = classif_info.get('prioridade', UI_TEXTS.text_na)
            prioridade_display = prioridade_display if prioridade_display != 'Selecionar' else f"{UI_TEXTS.text_na} (Não Classificado)"
            deadline_date_str = classif_info.get('deadline')

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
            with st.expander(
                    f"📋 Análise Completa e Decisão - Notificação #{notification.get('id', UI_TEXTS.text_na)}",
                    expanded=True):
                st.markdown("### 🧐 Detalhes para Análise de Aprovação")
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**📝 Evento Original Reportado**")
                    st.write(f"**Título:** {notification.get('title', UI_TEXTS.text_na)}")
                    st.write(f"**Local:** {notification.get('location', UI_TEXTS.text_na)}")
                    occurrence_datetime_summary = format_date_time_summary(notification.get('occurrence_date'),
                                                                           notification.get('occurrence_time'))
                    st.write(f"**Data/Hora Ocorrência:** {occurrence_datetime_summary}")
                    st.write(f"**Setor Notificante:** {notification.get('reporting_department', UI_TEXTS.text_na)}")
                    if notification.get('immediate_actions_taken') and notification.get('immediate_action_description'):
                        st.info(
                            f"**Ações Imediatas Reportadas:** {notification.get('immediate_action_description', UI_TEXTS.text_na)[:300]}...")
                with col2:
                    st.markdown("**⏱️ Informações de Gestão e Classificação**")
                    classif = notification.get('classification', {})
                    if isinstance(classif, str):
                        try:
                            classif = json.loads(classif)
                        except json.JSONDecodeError:
                            classif = {}

                    never_event_display = classif.get('never_event', UI_TEXTS.text_na)
                    st.write(f"**Never Event:** {never_event_display}")
                    sentinel_display = 'Sim' if classif.get('is_sentinel_event') else (
                        'Não' if classif.get('is_sentinel_event') is False else UI_TEXTS.text_na)
                    st.write(f"**Evento Sentinela:** {sentinel_display}")
                    st.write(f"**Classificação NNC:** {classif.get('nnc', UI_TEXTS.text_na)}")
                    if classif.get('nivel_dano'): st.write(
                        f"**Nível de Dano:** {classif.get('nivel_dano', UI_TEXTS.text_na)}")
                    event_type_main_display = classif.get('event_type_main', UI_TEXTS.text_na)
                    st.write(f"**Tipo Principal:** {event_type_main_display}")
                    event_type_sub_display = classif.get('event_type_sub')
                    if event_type_sub_display:
                        if isinstance(event_type_sub_display, list):
                            st.write(
                                f"**Especificação:** {', '.join(event_type_sub_display)[:100]}..." if len(', '.join(
                                    event_type_sub_display)) > 100 else f"**Especificação:** {', '.join(event_type_sub_display)}")
                        else:
                            st.write(f"**Especificação:** {str(event_type_sub_display)[:100]}..." if len(
                                str(event_type_sub_display)) > 100 else f"**Especificação:** {str(event_type_sub_display)}")
                    st.write(f"**Classificação OMS:** {', '.join(classif.get('oms', [UI_TEXTS.text_na]))}")
                    st.write(
                        f"**Requer Aprovação Superior (Classif. Inicial):** {'Sim' if classif.get('requires_approval') else 'Não'}")
                    st.write(f"**Classificado por:** {classif.get('classified_by', UI_TEXTS.text_na)}")
                    classified_at_str = classif.get('classified_at', UI_TEXTS.text_na)
                    if classified_at_str != UI_TEXTS.text_na:
                        try:
                            classified_at_str = datetime.fromisoformat(
                                classified_at_str).strftime(
                                '%d/%m/%Y %H:%M:%S')
                        except ValueError:
                            pass
                        st.write(f"**Classificado em:** {classified_at_str}")
                    if deadline_date_str:
                        deadline_date_formatted = datetime.fromisoformat(deadline_date_str).strftime('%d/%m/%Y')
                        st.markdown(
                            f"**Prazo de Conclusão:** {deadline_date_formatted} (<span class='{deadline_status['class']}'>{deadline_status['text']}</span>)",
                            unsafe_allow_html=True)
                    else:
                        st.write(f"**Prazo de Conclusão:** {UI_TEXTS.deadline_days_nan}")
                st.markdown("**📝 Descrição Completa do Evento**")
                st.info(notification.get('description', UI_TEXTS.text_na))
                if classif.get('classifier_observations'):
                    st.markdown("**📋 Orientações / Observações do Classificador (Classificação Inicial)**")
                    st.success(classif.get('classifier_observations', UI_TEXTS.text_na))

                if notification.get('patient_involved'):
                    st.markdown("**🏥 Informações do Paciente Afetado**")
                    st.write(f"**N° Atendimento/Prontuário:** {notification.get('patient_id', UI_TEXTS.text_na)}")
                    outcome = notification.get('patient_outcome_obito')
                    if outcome is not None:
                        st.write(f"**Evoluiu com óbito?** {'Sim' if outcome is True else 'Não'}")
                    else:
                        st.write("**Evoluiu com óbito?** Não informado")
                if notification.get('additional_notes'):
                    st.markdown("**ℹ️ Observações Adicionais do Notificante**")
                    st.info(notification.get('additional_notes', UI_TEXTS.text_na))
                st.markdown("---")
                st.markdown("#### ⚡ Ações Executadas pelos Responsáveis")
                if notification.get('actions'):
                    for action in sorted(notification['actions'], key=lambda x: x.get('timestamp', '')):
                        action_type = "🏁 CONCLUSÃO (Executor)" if action.get(
                            'final_action_by_executor') else "📝 AÇÃO Registrada"
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
                        if action.get('final_action_by_executor'):
                            evidence_desc = action.get('evidence_description', '').strip()
                            evidence_atts = action.get('evidence_attachments', [])
                            if evidence_desc or evidence_atts:
                                st.markdown(f"""<div class='evidence-section'>""", unsafe_allow_html=True)
                                st.markdown("<h6>Evidências da Conclusão:</h6>", unsafe_allow_html=True)
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
                                                    label=f"Baixar Evidência: {original_name}",
                                                    data=file_content,
                                                    file_name=original_name,
                                                    mime="application/octet-stream",
                                                    key=f"download_action_evidence_approval_{notification['id']}_{unique_name}"
                                                )
                                            else:
                                                st.write(f"Anexo: {original_name} (arquivo não encontrado ou corrompido)")
                                st.markdown(f"""</div>""", unsafe_allow_html=True)
                        st.markdown("---")
                else:
                    st.warning("⚠️ Nenhuma ação foi registrada pelos executores para esta notificação ainda.")
                users_exec = get_users_by_role('executor')
                executor_name_to_id_map_approval = {
                    f"{u.get('name', UI_TEXTS.text_na)} ({u.get('username', UI_TEXTS.text_na)})": u['id']
                    for u in users_exec
                }
                executor_names_approval = [
                    name for name, uid in executor_name_to_id_map_approval.items()
                    if uid in notification.get('executors', [])
                ]
                st.markdown(f"**👥 Executores Atribuídos:** {', '.join(executor_names_approval) or 'Nenhum'}")
                review_exec_info = notification.get('review_execution', {})
                if review_exec_info:
                    if isinstance(review_exec_info, str):
                        try:
                            review_exec_info = json.loads(review_exec_info)
                        except json.JSONDecodeError:
                            review_exec_info = {}

                    st.markdown("---")
                    st.markdown("#### 🛠️ Resultado da Revisão do Classificador")
                    review_decision_display = review_exec_info.get('decision', UI_TEXTS.text_na)
                    reviewed_by_display = review_exec_info.get('reviewed_by_name') or review_exec_info.get('reviewed_by', UI_TEXTS.text_na)
                    review_timestamp_str = review_exec_info.get('reviewed_at', UI_TEXTS.text_na)
                    if review_timestamp_str != UI_TEXTS.text_na:
                        try:
                            review_timestamp_str = datetime.fromisoformat(review_timestamp_str).strftime(
                                '%d/%m/%Y %H:%M:%S')
                        except ValueError:
                            pass
                        st.write(f"**Decisão da Revisão:** {review_decision_display}")
                    st.write(f"**Revisado por (Classificador):** {reviewed_by_display} em {review_timestamp_str}")
                    if review_exec_info.get('rejection_reason'):
                        st.write(
                            f"**Motivo da Rejeição:** {review_exec_info.get('rejection_reason', UI_TEXTS.text_na)}")
                    if review_exec_info.get('observations'):
                        st.write(
                            f"**Observações do Classificador:** {review_exec_info.get('observations', UI_TEXTS.text_na)}")
                if notification.get('attachments'):
                    st.markdown("---")
                    st.markdown("#### 📎 Anexos")
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
                                st.write(f"Anexo: {original_name_to_use} (arquivo não encontrado ou corrompido)")
                st.markdown("---")
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
                    st.markdown("### 🎯 Decisão de Aprovação Final")
                    approval_decision_options = [UI_TEXTS.selectbox_default_decisao_aprovacao, "Aprovar",
                                                 "Reprovar"]
                    current_approval_data['decision'] = st.selectbox(
                        "Decisão:*", options=approval_decision_options,
                        key=f"approval_decision_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                        index=approval_decision_options.index(
                            current_approval_data.get('decision', UI_TEXTS.selectbox_default_decisao_aprovacao)),
                        help="Selecione 'Aprovar' para finalizar a notificação ou 'Reprovar' para devolvê-la para revisão pelo classificador."
                    )
                    st.markdown("<span class='required-field'>* Campo obrigatório</span>", unsafe_allow_html=True)
                    approval_notes_input = st.text_area(
                        "Observações da Aprovação/Reprovação:*",
                        value=current_approval_data.get('notes', ''),
                        placeholder="• Avalie a completude e eficácia das ações executadas e a revisão do classificador...\\n• Indique se as ações foram satisfatórias para mitigar o risco ou resolver o evento.\\n• Forneça recomendações adicionais, se necessário.\\n• Em caso de reprovação, explique claramente o motivo e o que precisa ser revisado ou corrigido pelo classificador.",
                        height=120, key=f"approval_notes_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                        help="Forneça sua avaliação sobre as ações executadas, a revisão do classificador, e a decisão final.").strip()
                    current_approval_data['notes'] = approval_notes_input
                    submit_button = st.form_submit_button("✔️ Confirmar Decisão",
                                                          use_container_width=True)
                    st.markdown("---")
                    if submit_button:
                        validation_errors = []
                        if current_approval_data[
                            'decision'] == UI_TEXTS.selectbox_default_decisao_aprovacao: validation_errors.append(
                            "É obrigatório selecionar a decisão (Aprovar/Reprovar).")
                        if current_approval_data['decision'] == "Reprovar" and not current_approval_data[
                            'notes']: validation_errors.append(
                            "É obrigatório informar as observações para reprovar a notificação.")
                        if validation_errors:
                            st.error("⚠️ **Por favor, corrija os seguintes erros:**")
                            for error in validation_errors: st.warning(error)
                        else:
                            user_name = st.session_state.user.get('name', 'Usuário')
                            user_username = st.session_state.user_username
                            approval_notes = current_approval_data['notes']
                            
                            history_notes_part = ""
                            if approval_notes:
                                if len(approval_notes) > 150:
                                    history_notes_part = f" Obs: {approval_notes[:150]}..."
                                else:
                                    history_notes_part = f" Obs: {approval_notes}"

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
                                        'notes': approval_notes or "Notificação aprovada superiormente.",
                                        'timestamp': datetime.now().isoformat(),
                                        'status_final': 'aprovada'
                                    },
                                    'approver': None
                                }
                                update_notification(notification['id'], updates)
                                add_history_entry(notification['id'], "Notificação aprovada e finalizada",
                                                  user_name,
                                                  f"Aprovada superiormente.{history_notes_part}")
                                st.success(
                                    f"✅ Notificação #{notification['id']} aprovada e finalizada com sucesso! O ciclo de gestão do evento foi concluído.")
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
                                update_notification(notification['id'], updates)
                                add_history_entry(notification['id'], "Notificação reprovada (Aprovação)",
                                                  user_name,
                                                  f"Reprovada superiormente. Motivo: {approval_notes[:150]}...{history_notes_part}") 
                                st.warning(
                                    f"⚠️ Notificação #{notification['id']} reprovada! Devolvida para revisão pelo classificador.")
                                st.info(
                                    "A notificação foi movida para o status 'aguardando classificador' para que a equipe de classificação possa revisar e redefinir o fluxo.")
                            st.session_state.approval_form_state.pop(notification['id'], None)
                            _clear_approval_form_state(notification['id'])
                            st.rerun()
    with tab_closed_my_approval_notifs:
        st.markdown("### Minhas Aprovações Encerradas")
        if not closed_my_approval_notifications:
            st.info("✅ Não há notificações encerradas que você aprovou ou reprovou no momento.")
        else:
            st.info(
                f"Total de notificações encerradas por você: {len(closed_my_approval_notifications)}.")
            search_query_app_closed = st.text_input(
                "🔎 Buscar em Minhas Aprovações Encerradas (Título, Descrição, ID):",
                key="closed_app_notif_search_input",
                placeholder="Ex: 'aprovação', 'reprovado', '456'"
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
                    "⚠️ Nenhuma notificação encontrada com os critérios de busca especificados em suas aprovações encerradas.")
            else:
                filtered_closed_my_approval_notifications.sort(
                    key=lambda x: x.get('created_at', ''), reverse=True)
                st.markdown(
                    f"**Notificações Encontradas ({len(filtered_closed_my_approval_notifications)})**:")
                for notification in filtered_closed_my_approval_notifications:
                    status_class = f"status-{notification.get('status', UI_TEXTS.text_na).replace('_', '-')}"
                    created_at_str = datetime.fromisoformat(notification['created_at']).strftime('%d/%m/%Y %H:%M:%S') if isinstance(notification['created_at'], str) else notification['created_at'].strftime('%d/%m/%Y %H:%M:%S')

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

                    classif_info = notification.get('classification', {})
                    if isinstance(classif_info, str):
                        try:
                            classif_info = json.loads(classif_info)
                        except json.JSONDecodeError:
                            classif_info = {}

                    deadline_info = classif_info.get('deadline')
                    concluded_timestamp_str = (notification.get('conclusion') or {}).get('timestamp')
                    deadline_status = get_deadline_status(deadline_info, concluded_timestamp_str)
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
                                            <p><strong>Encerrada por:</strong> {concluded_by} | <strong>Data de Criação:</strong> {created_at_str}</p>
                                            <p><strong>Prazo:</strong> {deadline_status['text']} (class={deadline_status['class']})</p>
                                        </div>
                                        """, unsafe_allow_html=True)
                    with st.expander(
                            f"👁️ Visualizar Detalhes - Notificação #{notification.get('id', UI_TEXTS.text_na)}"):
                        display_notification_full_details(notification,
                                                          st.session_state.user_id if st.session_state.authenticated else None,
                                                          st.session_state.user_username if st.session_state.authenticated else None)

@st_fragment
def show_admin():
    """Renderiza a página de administração."""
    if not check_permission('admin'):
        st.error("❌ Acesso negado! Você não tem permissão de administrador.")
        return

    st.markdown("<h1 class='main-header'>⚙️ Administração do Sistema</h1>",
                unsafe_allow_html=True)
    st.info(
        "Esta área permite gerenciar usuários, configurar o sistema e acessar ferramentas de desenvolvimento.")

    tab1, tab2, tab3, tab4 = st.tabs(
        ["👥 Usuários", "💾 Configurações e Dados", "🛠️ Visualização de Desenvolvimento",
         "ℹ️ Sobre o Sistema"])

    with tab1:
        st.markdown("### 👥 Gerenciamento de Usuários")
        with st.expander("➕ Criar Novo Usuário", expanded=False):
            with st.form("create_user_form_refactored", clear_on_submit=True):
                st.markdown("**📝 Dados do Novo Usuário**")
                col1, col2 = st.columns(2)
                with col1:
                    new_username = st.text_input("Nome de Usuário*",
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
                    help="Selecione uma ou mais funções para o novo usuário.",
                    key="admin_new_roles_form_refactored"
                )

                st.markdown("<span class='required-field'>* Campos obrigatórios</span>",
                            unsafe_allow_html=True)
                submit_button = st.form_submit_button("➕ Criar Usuário",
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
                        "Nome de Usuário é obrigatório.")
                    if not password_state: validation_errors.append("Senha é obrigatória.")
                    if password_state != password_confirm_state: validation_errors.append(
                        "As senhas não coincidem.")
                    if not name_state: validation_errors.append("Nome Completo é obrigatório.")
                    if not email_state: validation_errors.append("Email é obrigatório.")
                    if not roles_to_save: validation_errors.append(
                        "Pelo menos uma Função é obrigatória.")
                    if validation_errors:
                        st.error("⚠️ **Por favor, corrija os seguintes erros:**")
                        for error in validation_errors: st.warning(error)
                    else:
                        user_data = {'username': username_state, 'password': password_state,
                                     'name': name_state,
                                     'email': email_state, 'roles': roles_to_save}
                        if create_user(user_data):
                            st.success(f"✅ Usuário '{name_state}' criado com sucesso!\\n\\n")
                            st.rerun()
                        else:
                            st.error("❌ Nome de usuário já existe. Por favor, escolha outro.")

        st.markdown("### 📋 Usuários Cadastrados no Sistema")
        users = load_users()
        if users:
            if 'editing_user_id' not in st.session_state:
                st.session_state.editing_user_id = None
            users_to_display = [u for u in users if u['id'] != st.session_state.user.get('id')]
            users_to_display.sort(key=lambda x: x.get('name', ''))

            for user in users_to_display:
                status_icon = "🟢" if user.get('active', True) else "🔴"

                expander_key = f"user_expander_{user.get('id', UI_TEXTS.text_na)}"
                with st.expander(
                        f"**{user.get('name', UI_TEXTS.text_na)}** ({user.get('username', UI_TEXTS.text_na)}) {status_icon}",
                        expanded=(st.session_state.editing_user_id == user['id'])):
                    col_display, col_actions = st.columns([0.7, 0.3])
                    with col_display:
                        st.write(f"**ID:** {user.get('id', UI_TEXTS.text_na)}")
                        st.write(f"**Email:** {user.get('email', UI_TEXTS.text_na)}")
                        st.write(
                            f"**Funções:** {', '.join(user.get('roles', [UI_TEXTS.text_na]))}")
                        st.write(
                            f"**Status:** {'✅ Ativo' if user.get('active', True) else '❌ Inativo'}")
                        created_at_str = user.get('created_at', UI_TEXTS.text_na)
                        if created_at_str != UI_TEXTS.text_na:
                            try:
                                created_at_str = datetime.fromisoformat(
                                    created_at_str).strftime('%d/%m/%Y %H:%M:%S')
                            except ValueError:
                                pass
                        st.write(f"**Criado em:** {created_at_str}")
                    with col_actions:
                        if user.get('id') != 1 and user.get('id') != st.session_state.user.get(
                                'id'):
                            if st.button("✏️ Editar",
                                         key=f"edit_user_{user.get('id', UI_TEXTS.text_na)}",
                                         use_container_width=True):
                                st.session_state.editing_user_id = user['id']
                                st.session_state[f"edit_name_{user['id']}"] = user.get('name',
                                                                                       '')
                                st.session_state[f"edit_email_{user['id']}"] = user.get('email',
                                                                                        '')
                                st.session_state[f"edit_roles_{user['id']}"] = user.get('roles',
                                                                                        [])
                                st.session_state[f"edit_active_{user['id']}"] = user.get(
                                    'active', True)
                                st.rerun()
                            action_text = "🔒 Desativar" if user.get('active',
                                                                    True) else "🔓 Ativar"
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
                                        f"✅ Usuário '{user.get('name', UI_TEXTS.text_na)}' {status_msg} com sucesso.")
                                    st.rerun() 
                                else:
                                    st.error("❌ Erro ao atualizar status do usuário.")
                        elif user.get('id') == 1:
                            st.info("👑 Admin inicial não editável.")
                        elif user.get('id') == st.session_state.user.get('id'):
                            st.info("👤 Você não pode editar sua própria conta.")
                            st.info(
                                "Para alterar sua senha ou dados, faça logout e use a opção de recuperação de senha ou peça a outro admin para editar.")
            if st.session_state.editing_user_id:
                edited_user = next(
                    (u for u in users if u['id'] == st.session_state.editing_user_id), None)
                if edited_user:
                    st.markdown(
                        f"### ✏️ Editando Usuário: {edited_user.get('name', UI_TEXTS.text_na)} ({edited_user.get('username', UI_TEXTS.text_na)})"
                    )
                    with st.form(key=f"edit_user_form_{edited_user['id']}",
                                 clear_on_submit=False):
                        st.text_input("Nome de Usuário", value=edited_user.get('username', ''),
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
                            "<span class='required-field'>* Campos obrigatórios (para nome, email e funções)</span>",
                            unsafe_allow_html=True)

                        col_edit_submit, col_edit_cancel = st.columns(2)
                        with col_edit_submit:
                            submit_edit_button = st.form_submit_button("💾 Salvar Alterações",
                                                                       use_container_width=True)
                        with col_edit_cancel:
                            cancel_edit_button = st.form_submit_button("❌ Cancelar Edição",
                                                                       use_container_width=True)
                        if submit_edit_button:
                            edit_validation_errors = []
                            if not edited_name: edit_validation_errors.append(
                                "Nome Completo é obrigatório.")
                            if not edited_email: edit_validation_errors.append(
                                "Email é obrigatório.")
                            if not edited_roles: edit_validation_errors.append(
                                "Pelo menos uma Função é obrigatória.")
                            if new_password:
                                if new_password != new_password_confirm:
                                    edit_validation_errors.append(
                                        "As novas senhas não coincidem.")
                                if len(new_password) < 6:
                                    edit_validation_errors.append(
                                        "A nova senha deve ter no mínimo 6 caracteres.")
                            if edit_validation_errors:
                                st.error("⚠️ **Por favor, corrija os seguintes erros:**")
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
                                        f"✅ Usuário '{updated_user_final.get('name', UI_TEXTS.text_na)}' atualizado com sucesso!")
                                    st.session_state.editing_user_id = None
                                    st.rerun()
                                else:
                                    st.error("❌ Erro ao salvar alterações do usuário.")

                        if cancel_edit_button:
                            st.session_state.editing_user_id = None
                            st.rerun()

        else:
            st.info("📋 Nenhum usuário cadastrado no sistema.")

    with tab2:
        st.markdown("### 💾 Configurações e Gerenciamento de Dados")
        st.warning(
            "⚠️ Esta seção é destinada a desenvolvedores para visualizar a estrutura completa dos dados. Não é para uso operacional normal.")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 💾 Backup dos Dados")
            st.info(
                "Gera um arquivo JSON contendo todos os dados de usuários e notificações cadastrados no sistema.")
            if st.button("📥 Gerar Backup (JSON)", use_container_width=True,
                         key="generate_backup_btn"):
                all_users_for_backup = load_users()
                all_notifications_for_backup = load_notifications()
                def prepare_for_json(data):
                    if isinstance(data, dict):
                        return {k: prepare_for_json(v) for k, v in data.items()}
                    elif isinstance(data, list):
                        return [prepare_for_json(elem) for elem in data]
                    elif isinstance(data, (datetime, dt_date_class, dt_time_class)):
                        return data.isoformat()
                    else:
                        try:
                            if isinstance(data, str) and (
                                    data.strip().startswith('{') or data.strip().startswith(
                                '[')):
                                return json.loads(data)
                        except json.JSONDecodeError:
                            pass
                        return data
                backup_data = {
                    'users': [prepare_for_json(u) for u in all_users_for_backup],
                    'notifications': [prepare_for_json(n) for n in
                                      all_notifications_for_backup],
                    'backup_date': datetime.now().isoformat(),
                    'version': '1.1-db-based'
                }
                backup_json = json.dumps(backup_data, indent=2, ensure_ascii=False)
                st.download_button(
                    label="⬇️ Baixar Backup Agora", data=backup_json,
                    file_name=f"hospital_notif_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json", use_container_width=True, key="download_backup_btn"
                )
        with col2:
            st.markdown("#### 📤 Restaurar Dados")
            st.info(
                "Carrega um arquivo JSON de backup para restaurar dados de usuários e notificações. **Isso sobrescreverá os dados existentes!**")
            uploaded_file = st.file_uploader("Selecione um arquivo de backup (formato JSON):",
                                             type=['json'],
                                             key="admin_restore_file_uploader")
            if uploaded_file:
                with st.form("restore_form", clear_on_submit=False):
                    submit_button = st.form_submit_button("🔄 Restaurar Dados",
                                                          use_container_width=True,
                                                          key="restore_data_btn")
                    if submit_button:
                        try:
                            uploaded_file_content = st.session_state.admin_restore_file_uploader.getvalue().decode(
                                'utf8')
                            backup_data = json.loads(uploaded_file_content)
                            if isinstance(backup_data,
                                          dict) and 'users' in backup_data and 'notifications' in backup_data:
                                conn = get_db_connection()
                                cur = conn.cursor()
                                try:
                                    cur.execute(
                                        "ALTER TABLE notifications DISABLE TRIGGER trg_notifications_search_vector;")
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
                                    for user_data in backup_data['users']:
                                        cur.execute("""
                                                    INSERT INTO users (id, username, password_hash, name, email, roles, active, created_at)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                                """, (
                                            user_data.get('id'),
                                            user_data.get('username'),
                                            user_data.get('password'),
                                            user_data.get('name'),
                                            user_data.get('email'),
                                            user_data.get('roles', []),
                                            user_data.get('active', True),
                                            datetime.fromisoformat(
                                                user_data['created_at']) if user_data.get(
                                                'created_at') else datetime.now()
                                        ))
                                    cur.execute(
                                        f"SELECT setval('users_id_seq', (SELECT MAX(id) FROM users));")
                                    for notif_data in backup_data['notifications']:
                                        occurrence_date = datetime.fromisoformat(notif_data[
                                                                                     'occurrence_date']).date() if notif_data.get(
                                            'occurrence_date') else None
                                        occurrence_time = datetime.fromisoformat(notif_data[
                                                                                     'occurrence_time']).time() if notif_data.get(
                                            'occurrence_time') else None
                                        created_at = datetime.fromisoformat(
                                            notif_data['created_at']) if notif_data.get(
                                            'created_at') else datetime.now()
                                        updated_at = datetime.fromisoformat(
                                            notif_data['updated_at']) if notif_data.get(
                                            'updated_at') else created_at
                                        cur.execute("""
                                                    INSERT INTO notifications (
                                                        id, title, description, location, occurrence_date, occurrence_time,
                                                        reporting_department, reporting_department_complement, notified_department,
                                                        notified_department_complement, event_shift, immediate_actions_taken,
                                                        immediate_action_description, patient_involved, patient_id, patient_outcome_obito,
                                                        additional_notes, status, created_at, updated_at,
                                                        classification, rejection_classification, review_execution, approval,
                                                        rejection_approval, rejection_execution_review, conclusion,
                                                        executors, approver
                                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
                                            notif_data.get('immediate_action_description'),
                                            notif_data.get('patient_involved'),
                                            notif_data.get('patient_id'),
                                            notif_data.get('patient_outcome_obito'),
                                            notif_data.get('additional_notes'),
                                            notif_data.get('status'),
                                            created_at,
                                            updated_at,
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
                                            notif_data.get('executors', []),
                                            notif_data.get('approver')
                                        ))
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
                                    cur.execute(
                                        f"SELECT setval('notifications_id_seq', (SELECT MAX(id) FROM notifications));")
                                    conn.commit()
                                    st.success(
                                        "✅ Dados restaurados com sucesso a partir do arquivo!\\n\\n")
                                    st.info(
                                        "A página será recarregada para refletir os dados restaurados.")
                                    st.session_state.pop('admin_restore_file_uploader', None)
                                    _reset_form_state()
                                    st.session_state.initial_classification_state = {}
                                    st.session_state.review_classification_state = {}
                                    st.session_state.classification_active_notification_id = None
                                    st.session_state.approval_form_state = {}
                                    st.rerun()
                                except psycopg2.Error as e:
                                    conn.rollback()
                                    st.error(
                                        f"❌ Erro ao restaurar dados no banco de dados: {e}")
                                finally:
                                    cur.execute(
                                        "ALTER TABLE notifications ENABLE TRIGGER trg_notifications_search_vector;")
                                    cur.close()
                                    conn.close()
                            else:
                                st.error(
                                    "❌ Arquivo de backup inválido. O arquivo JSON não contém a estrutura esperada (chaves 'users' e 'notifications').")
                        except json.JSONDecodeError:
                            st.error(
                                "❌ Erro ao ler o arquivo JSON. Certifique-se de que é um arquivo JSON válido.")
                        except Exception as e:
                            st.error(
                                f"❌ Ocorreu um erro inesperado ao restaurar os dados: {str(e)}")
    with tab3:
        st.markdown("### 🛠️ Visualização de Desenvolvimento e Debug")
        st.warning(
            "⚠️ Esta seção é destinada a desenvolvedores para visualizar a estrutura completa dos dados. Não é para uso operacional normal.")
        notifications = load_notifications()
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
                "Selecionar notificação para análise detalhada (JSON):",
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
                            st.markdown("#### Dados Completos da Notificação (JSON)")
                            st.json(notification)
                        else:
                            st.error("❌ Notificação não encontrada.")
                    else:
                        st.error("❌ Formato de seleção inválido.")
                except (IndexError, ValueError) as e:
                    st.error(f"❌ Erro ao processar seleção ou encontrar notificação: {e}")
        else:
            st.info("📋 Nenhuma notificação encontrada para análise de desenvolvimento.")
    with tab4:
        st.markdown("### ℹ️ Informações do Sistema")
        st.markdown("#### Detalhes do Portal")
        st.write(f"**Versão do Portal:** 2.0.5")
        st.write(f"**Data da Última Atualização:** 27/07/2025")
        st.write(f"**Desenvolvido por:** FIA Softworks")
        st.markdown("##### Suporte Técnico:")
        st.write(f"**Email:** borges@fiasoftworks.com.br")

@st_fragment
def show_dashboard():
    """
    Renderiza um dashboard abrangente para visualização de notificações,
    incluindo métricas chave, gráficos e uma lista detalhada, filtrável,
    pesquisável e paginada de notificações.
    """
    if not check_permission('admin') and not check_permission('classificador'):
        st.error("❌ Acesso negado! Você não tem permissão para visualizar o dashboard.")
        return
    
    st.markdown("## 📊 Dashboard de Notificações")
    st.info("Visualize métricas consolidadas, tendências e lista completa de notificações do sistema.")
    
    notifications = load_notifications()
    
    if not notifications:
        st.warning("⚠️ Nenhuma notificação cadastrada no sistema até o momento.")
        return
    
    df_notifications = pd.DataFrame(notifications)
    df_notifications['created_at_dt'] = pd.to_datetime(
        df_notifications['created_at'], errors='coerce'
    )
    
    # DEFINIÇÃO DO status_mapping - DEVE ESTAR NO INÍCIO
    status_mapping = {
        'pendente_classificacao': 'Pendente Classif.Inicial',
        'classificada_aguardando_execucao': 'Classificada (Aguardando Exec.)',
        'em_execucao': 'Em Execução',
        'revisao_classificador_execucao': 'Aguardando Revisão Exec.',
        'aguardando_classificador': 'Aguardando Classif.(Revisão)',
        'concluida_aguardando_aprovacao': 'Aguardando Aprovação',
        'aprovada': 'Concluída (Aprovada)',
        'rejeitada_classificacao': 'Rejeitada (Classif.Inicial)',
        'rejeitada_aprovacao': 'Rejeitada (Aprovação)',
        'encerrada': 'Encerrada'
    }
    
    # Processar campo de classificação JSON
    def parse_classification(classif_val):
        if pd.isna(classif_val) or classif_val is None:
            return {}
        if isinstance(classif_val, dict):
            return classif_val
        if isinstance(classif_val, str):
            try:
                return json.loads(classif_val)
            except json.JSONDecodeError:
                return {}
        return {}
    
    df_notifications['classification'] = df_notifications['classification'].apply(
        parse_classification
    )
    
    # Criar abas
    tab_list, tab_indicators = st.tabs(["📋 Lista de Notificações", "📈 Indicadores"])
    
    with tab_list:
        st.info("Explore a lista completa de notificações com filtros avançados e busca.")
        
        # Métricas rápidas
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("🟡 Total", len(notifications))
        with col2:
            pending_count = sum(
                1 for n in notifications if n['status'] in [
                    'pendente_classificacao', 'aguardando_classificador',
                    'revisao_classificador_execucao'
                ]
            )
            st.metric("⏳ Pendentes", pending_count)
        with col3:
            in_progress_count = sum(
                1 for n in notifications if n['status'] in [
                    'classificada_aguardando_execucao', 'em_execucao'
                ]
            )
            st.metric("🔄 Em Andamento", in_progress_count)
        with col4:
            completed_count = sum(
                1 for n in notifications if n['status'] in [
                    'concluida_aguardando_aprovacao', 'aprovada', 'encerrada'
                ]
            )
            st.metric("✅ Concluídas", completed_count)
        
        st.markdown("---")
        
        # Gráfico de tendência temporal
        st.markdown("### 📊 Tendência Temporal de Criação de Notificações")
        
        if not df_notifications.empty and 'created_at_dt' in df_notifications.columns:
            df_notifications_copy = df_notifications.dropna(subset=['created_at_dt']).copy()
            
            if not df_notifications_copy.empty:
                df_notifications_copy['month_year'] = df_notifications_copy['created_at_dt'].dt.to_period('M').astype(str)
                monthly_counts = df_notifications_copy.groupby('month_year').size().reset_index(name='count')
                monthly_counts['month_year'] = pd.to_datetime(monthly_counts['month_year'])
                monthly_counts = monthly_counts.sort_values('month_year')
                monthly_counts['month_year'] = monthly_counts['month_year'].dt.strftime('%Y-%m')
                
                st.line_chart(monthly_counts.set_index('month_year'))
            else:
                st.info("Nenhum dado para gerar o gráfico de tendência.")
        
        st.markdown("---")
        
        st.markdown("### Lista Detalhada de Notificações")
        
        col_filters1, col_filters2, col_filters3 = st.columns(3)
        
        all_option_text = UI_TEXTS.multiselect_all_option
        
        # CALCULAR PRIMEIRO DIA DO MÊS ATUAL
        today = dt_date_class.today()
        first_day_of_current_month = dt_date_class(today.year, today.month, 1)
        
        # Inicializar estados de sessão
        if 'dashboard_filter_status' not in st.session_state:
            st.session_state.dashboard_filter_status = [all_option_text]
        if 'dashboard_filter_nnc' not in st.session_state:
            st.session_state.dashboard_filter_nnc = [all_option_text]
        if 'dashboard_filter_priority' not in st.session_state:
            st.session_state.dashboard_filter_priority = [all_option_text]
        if 'dashboard_filter_date_start' not in st.session_state:
            st.session_state.dashboard_filter_date_start = first_day_of_current_month
        if 'dashboard_filter_date_end' not in st.session_state:
            st.session_state.dashboard_filter_date_end = today
        if 'dashboard_search_query_input' not in st.session_state:
            st.session_state.dashboard_search_query_input = ""
        if 'dashboard_sort_column' not in st.session_state:
            st.session_state.dashboard_sort_column = 'created_at'
        if 'dashboard_sort_ascending' not in st.session_state:
            st.session_state.dashboard_sort_ascending = False
        
        with col_filters1:
            # Filtro de Status
            all_status_options_keys = list(status_mapping.keys())
            display_status_options_with_all = [all_option_text] + all_status_options_keys
            
            current_status_selection_raw = st.session_state.get(
                "dashboard_filter_status_select", [all_option_text]
            )
            if all_option_text in current_status_selection_raw and len(current_status_selection_raw) > 1:
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
            
            if all_option_text in st.session_state.dashboard_filter_status and len(st.session_state.dashboard_filter_status) > 1:
                st.session_state.dashboard_filter_status = [all_option_text]
            elif not st.session_state.dashboard_filter_status:
                st.session_state.dashboard_filter_status = [all_option_text]
            
            applied_status_filters = [s for s in st.session_state.dashboard_filter_status if s != all_option_text]
            
            # Filtro de NNC
            all_nnc_options = FORM_DATA.classificacao_nnc
            display_nnc_options_with_all = [all_option_text] + all_nnc_options
            current_nnc_selection_raw = st.session_state.get("dashboard_filter_nnc_select", [all_option_text])
            
            if all_option_text in current_nnc_selection_raw and len(current_nnc_selection_raw) > 1:
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
            
            if all_option_text in st.session_state.dashboard_filter_nnc and len(st.session_state.dashboard_filter_nnc) > 1:
                st.session_state.dashboard_filter_nnc = [all_option_text]
            elif not st.session_state.dashboard_filter_nnc:
                st.session_state.dashboard_filter_nnc = [all_option_text]
            
            applied_nnc_filters = [n for n in st.session_state.dashboard_filter_nnc if n != all_option_text]
        
        with col_filters2:
            # Filtro de Prioridade
            all_priority_options = FORM_DATA.prioridades
            display_priority_options_with_all = [all_option_text] + all_priority_options
            current_priority_selection_raw = st.session_state.get("dashboard_filter_priority_select", [all_option_text])
            
            if all_option_text in current_priority_selection_raw and len(current_priority_selection_raw) > 1:
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
            
            if all_option_text in st.session_state.dashboard_filter_priority and len(st.session_state.dashboard_filter_priority) > 1:
                st.session_state.dashboard_filter_priority = [all_option_text]
            elif not st.session_state.dashboard_filter_priority:
                st.session_state.dashboard_filter_priority = [all_option_text]
            
            applied_priority_filters = [p for p in st.session_state.dashboard_filter_priority if p != all_option_text]
            
            # Filtros de Data - LIMITADO AO MÊS ATUAL POR PADRÃO
            date_start_default = st.session_state.dashboard_filter_date_start
            date_end_default = st.session_state.dashboard_filter_date_end
            
            dashboard_date_start = st.date_input(
                "Data Inicial (Criação):",
                value=date_start_default,
                key="dashboard_filter_date_start_input"
            )
            st.session_state.dashboard_filter_date_start = dashboard_date_start
            
            dashboard_date_end = st.date_input(
                "Data Final (Criação):",
                value=date_end_default,
                key="dashboard_filter_date_end_input"
            )
            st.session_state.dashboard_filter_date_end = dashboard_date_end
        
        with col_filters3:
            # Busca por texto
            st.text_input(
                "Buscar (Título, Descrição, ID):",
                value=st.session_state.dashboard_search_query_input,
                key="dashboard_search_query_input",
            )
            st.session_state.dashboard_search_query = st.session_state.dashboard_search_query_input.lower()
            
            # Ordenação
            sort_options_map = {
                'ID': 'id',
                'Data de Criação': 'created_at',
                'Título': 'title',
                'Local': 'location',
                'Prioridade': 'classification.prioridade',
            }
            sort_options_display = [UI_TEXTS.selectbox_sort_by_placeholder] + list(sort_options_map.keys())
            selected_sort_option_display = st.selectbox(
                UI_TEXTS.selectbox_sort_by_label,
                options=sort_options_display,
                index=0,
                key="dashboard_sort_column_select"
            )
            
            if selected_sort_option_display != UI_TEXTS.selectbox_sort_by_placeholder:
                st.session_state.dashboard_sort_column = sort_options_map[selected_sort_option_display]
            else:
                st.session_state.dashboard_sort_column = 'created_at'
            
            st.session_state.dashboard_sort_ascending = st.checkbox(
                "Ordem Crescente",
                value=st.session_state.dashboard_sort_ascending,
                key="dashboard_sort_ascending_checkbox"
            )
        
        # Aplicar filtros
        filtered_notifications = notifications.copy()
        
        if applied_status_filters:
            filtered_notifications = [n for n in filtered_notifications if n['status'] in applied_status_filters]
        
        if applied_nnc_filters:
            filtered_notifications = [
                n for n in filtered_notifications
                if parse_classification(n.get('classification', {})).get('nnc') in applied_nnc_filters
            ]
        
        if applied_priority_filters:
            filtered_notifications = [
                n for n in filtered_notifications
                if parse_classification(n.get('classification', {})).get('prioridade') in applied_priority_filters
            ]
        
        # Filtro de data - APLICADO AUTOMATICAMENTE COM O MÊS ATUAL
        if st.session_state.dashboard_filter_date_start and st.session_state.dashboard_filter_date_end:
            filtered_notifications = [
                n for n in filtered_notifications
                if st.session_state.dashboard_filter_date_start <= pd.to_datetime(n['created_at'], errors='coerce').date() <= st.session_state.dashboard_filter_date_end
            ]
        
        # Filtro de busca
        if st.session_state.dashboard_search_query:
            search_query = st.session_state.dashboard_search_query
            filtered_notifications = [
                n for n in filtered_notifications
                if search_query in str(n.get('id', '')).lower() or
                   search_query in n.get('title', '').lower() or
                   search_query in n.get('description', '').lower()
            ]
        
        st.info(f"🔍 Exibindo **{len(filtered_notifications)}** notificação(ões) após filtros (período: {st.session_state.dashboard_filter_date_start.strftime('%d/%m/%Y')} a {st.session_state.dashboard_filter_date_end.strftime('%d/%m/%Y')})")
        
        if not filtered_notifications:
            st.warning("⚠️ Nenhuma notificação encontrada com os filtros aplicados.")
            return
        
        # Ordenação
        def get_sort_key(notif):
            sort_col = st.session_state.dashboard_sort_column
            if sort_col == 'id':
                return notif.get('id', 0)
            elif sort_col == 'created_at':
                return notif.get('created_at', '')
            elif sort_col == 'title':
                return notif.get('title', '').lower()
            elif sort_col == 'location':
                return notif.get('location', '').lower()
            elif sort_col == 'classification.prioridade':
                return parse_classification(notif.get('classification', {})).get('prioridade', '')
            return ''
        
        filtered_notifications.sort(key=get_sort_key, reverse=not st.session_state.dashboard_sort_ascending)
        
        # Exibir notificações
        for notification in filtered_notifications:
            with st.expander(f"👁️ Visualizar Detalhes - Notificação #{notification.get('id', UI_TEXTS.text_na)}"):
                display_notification_full_details(
                    notification,
                    st.session_state.user_id if st.session_state.authenticated else None,
                    st.session_state.user_username if st.session_state.authenticated else None
                )
    
    with tab_indicators:
        st.info("Explore os indicadores e tendências das notificações, com filtros de período.")
        st.markdown("### Seleção de Período para Indicadores")
        
        # FILTRO INICIAL LIMITADO AO MÊS ATUAL
        today = dt_date_class.today()
        first_day_of_current_month = dt_date_class(today.year, today.month, 1)
        
        # Valores padrão para a aba de indicadores
        min_date = first_day_of_current_month
        max_date = today
        
        col_date1, col_date2 = st.columns(2)
        
        with col_date1:
            # CORREÇÃO: key diferente para evitar conflito
            start_date_indicators = st.date_input(
                "Data de Início",
                value=min_date,
                key="indicators_start_date_input"
            )
        
        with col_date2:
            # CORREÇÃO: key diferente para evitar conflito
            end_date_indicators = st.date_input(
                "Data de Fim",
                value=max_date,
                key="indicators_end_date_input"
            )
        
        # Usar os valores diretamente dos widgets (não precisa session_state)
        df_filtered_by_period = df_notifications[
            (df_notifications['created_at_dt'].dt.date >= start_date_indicators) &
            (df_notifications['created_at_dt'].dt.date <= end_date_indicators)
        ].copy()
        
        if df_filtered_by_period.empty:
            st.warning("⚠️ Não há dados para o período selecionado para gerar os indicadores.")
            return
        
        st.markdown("---")
        
        st.markdown("#### 📈 Quantidade de Notificações por Mês (Abertas, Concluídas, Rejeitadas)")
        
        completed_statuses = ['concluida_aguardando_aprovacao', 'aprovada', 'encerrada']
        rejected_statuses = ['rejeitada_classificacao', 'rejeitada_aprovacao']
        
        df_monthly = df_filtered_by_period.copy()
        df_monthly['month_year'] = df_monthly['created_at_dt'].dt.to_period('M').astype(str)
        df_monthly['status_category'] = 'Aberta'
        df_monthly.loc[df_monthly['status'].isin(completed_statuses), 'status_category'] = 'Concluída'
        df_monthly.loc[df_monthly['status'].isin(rejected_statuses), 'status_category'] = 'Rejeitada'
        
        monthly_counts = df_monthly.groupby(['month_year', 'status_category']).size().unstack(fill_value=0)
        
        all_months_in_range = pd.period_range(
            start=start_date_indicators,
            end=end_date_indicators,
            freq='M'
        ).astype(str)
        monthly_counts = monthly_counts.reindex(all_months_in_range, fill_value=0)
        
        if not monthly_counts.empty:
            st.line_chart(monthly_counts)
        else:
            st.info("Nenhuma notificação encontrada no período para este gráfico.")
        
        st.markdown("---")
        
        st.markdown("#### Pendência de Análises por Mês")
        pending_analysis_statuses = ['pendente_classificacao', 'aguardando_classificador', 'revisao_classificador_execucao']
        df_pending_analysis = df_filtered_by_period[df_filtered_by_period['status'].isin(pending_analysis_statuses)].copy()
        
        all_notified_departments_unique = sorted(df_notifications['notified_department'].unique().tolist())
        notified_departments_filter_options = [UI_TEXTS.multiselect_all_option] + all_notified_departments_unique
        selected_notified_dept = st.selectbox(
            "Filtrar por Setor Notificado:",
            notified_departments_filter_options,
            key="pending_dept_filter"
        )
        
        if selected_notified_dept != UI_TEXTS.multiselect_all_option:
            df_pending_analysis = df_pending_analysis[df_pending_analysis['notified_department'] == selected_notified_dept]
        
        if not df_pending_analysis.empty:
            df_pending_analysis['month_year'] = df_pending_analysis['created_at_dt'].dt.to_period('M').astype(str)
            monthly_pending_counts = df_pending_analysis.groupby('month_year').size().reset_index(name='count')
            monthly_pending_counts['month_year'] = pd.to_datetime(monthly_pending_counts['month_year'])
            monthly_pending_counts = monthly_pending_counts.sort_values('month_year')
            monthly_pending_counts['month_year'] = monthly_pending_counts['month_year'].dt.strftime('%Y-%m')
            
            st.line_chart(monthly_pending_counts.set_index('month_year'))
        else:
            st.info("Nenhuma notificação pendente de análise encontrada no período selecionado.")

def main():
    """Main function to run the Streamlit application."""
    init_database()
    init_database_performance_objects() 
    if 'authenticated' not in st.session_state: st.session_state.authenticated = False
    if 'user' not in st.session_state: st.session_state.user = None
    if 'user_id' not in st.session_state: st.session_state.user_id = None
    if 'user_username' not in st.session_state: st.session_state.user_username = None
    if 'page' not in st.session_state: st.session_state.page = 'create_notification'
    if 'initial_classification_state' not in st.session_state: st.session_state.initial_classification_state = {}
    if 'review_classification_state' not in st.session_state: st.session_state.review_classification_state = {}
    if 'classification_active_notification_id' not in st.session_state: st.session_state.classification_active_notification_id = None
    if 'approval_form_state' not in st.session_state: st.session_state.approval_form_state = {}

    show_sidebar()

    restricted_pages = ['dashboard', 'classificacao_inicial', 'revisao_execucao', 'notificacoes_encerradas', 'execution', 'approval', 'admin']
    if st.session_state.page in restricted_pages and not st.session_state.authenticated:
        st.warning("⚠️ Você precisa estar logado para acessar esta página.")
        st.session_state.page = 'create_notification'
        st.rerun()
    if st.session_state.page == 'create_notification':
        show_create_notification()
    elif st.session_state.page == 'dashboard':
        show_dashboard()
    elif st.session_state.page == 'classificacao_inicial':
        show_classificacao_inicial()
    elif st.session_state.page == 'revisao_execucao':
        show_revisao_execucao()
    elif st.session_state.page == 'notificacoes_encerradas':
        show_notificacoes_encerradas()
    elif st.session_state.page == 'execution':
        show_execution()
    elif st.session_state.page == 'approval':
        show_approval()
    elif st.session_state.page == 'admin':
        show_admin()
    else:
        st.error("Página solicitada inválida. Redirecionando para a página inicial.")
        st.session_state.page = 'create_notification'
        st.rerun()


if __name__ == "__main__":
    main()







