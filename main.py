
# --- git pull https://github.com/brgsfrmhll/NotiSanta
# --- sudo systemctl daemon-reload
# --- sudo systemctl restart streamlit-app2.service
# --- source /home/ubuntu/NotiSanta/venv/bin/activate

import streamlit as st
import json
import hashlib
import os
import io
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
# PDF (relatórios bonitos)
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors


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


def get_notification_actions(notification_id: int) -> List[Dict]:
    """Retorna lista de ações registradas pelos executores para uma notificação."""
    conn = None
    try:
        conn = get_db_connection()
        mp = _get_actions_map_by_ids(conn, [int(notification_id)])
        return mp.get(int(notification_id), []) or []
    except Exception as e:
        log_error(f"Erro ao buscar ações da notificação {notification_id}: {e}")
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def add_notification_action(notification_id: int, action_data: Dict) -> bool:
    """Insere uma ação de executor na tabela notification_actions.

    action_data esperado:
      - executor_id (int | None)
      - executor_name (str | None)
      - description (str) [obrigatório]
      - timestamp (str ISO | None)
      - final_action_by_executor (bool)
      - evidence_description (str | None)
      - evidence_attachments (list[dict] | None) -> [{unique_name, original_name, saved_at, size_bytes}, ...]
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        desc = (action_data.get("description") or "").strip()
        if not desc:
            return False

        ts = action_data.get("timestamp")
        # aceitar ISO string ou None (usa default do banco)
        action_ts = None
        if isinstance(ts, str) and ts.strip():
            try:
                action_ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                action_ts = None

        evidence_attachments = action_data.get("evidence_attachments")
        if evidence_attachments is None:
            evidence_attachments = action_data.get("attachments")
        if evidence_attachments is not None and not isinstance(evidence_attachments, (list, dict, str)):
            evidence_attachments = None

        cur.execute(
            """
            INSERT INTO notification_actions
                (notification_id, executor_id, executor_name, description, action_timestamp,
                 final_action_by_executor, evidence_description, evidence_attachments)
            VALUES
                (%s, %s, %s, %s, COALESCE(%s, CURRENT_TIMESTAMP),
                 %s, %s, %s)
            """,
            (
                int(notification_id),
                action_data.get("executor_id"),
                action_data.get("executor_name"),
                desc,
                action_ts,
                bool(action_data.get("final_action_by_executor", False)),
                (action_data.get("evidence_description") or None),
                json.dumps(evidence_attachments, ensure_ascii=False) if isinstance(evidence_attachments, (list, dict)) else evidence_attachments
            ),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        log_error(f"Erro ao adicionar ação para notificação {notification_id}: {e}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def add_history_entry(notification_id: int,
                      action_type: str,
                      performed_by: str,
                      details: str = "",
                      conn=None,
                      cursor=None) -> None:
    """Insere uma entrada na tabela notification_history.

    - `details` é opcional (evita TypeError em chamadas antigas).
    - Usa a mesma transação quando conn/cursor são fornecidos (evita FK violation antes do COMMIT).
    """
    if notification_id is None:
        return

    # details pode ser None/dict/list
    if details is None:
        details = ""
    try:
        if isinstance(details, (dict, list)):
            import json
            details = json.dumps(details, ensure_ascii=False)
    except Exception:
        details = str(details)

    own_conn = False
    try:
        cur = None
        if cursor is not None:
            cur = cursor
        else:
            if conn is None:
                conn = get_db_connection()
                own_conn = True
            cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO notification_history (notification_id, action_type, performed_by, details)
            VALUES (%s, %s, %s, %s)
            """,
            (int(notification_id), str(action_type or ""), str(performed_by or ""), str(details))
        )

        if own_conn:
            conn.commit()

    except Exception as e:
        # Não derruba o fluxo principal (UI), mas registra no console/log.
        try:
            import logging
            logging.getLogger(__name__).error(
                f"Erro ao adicionar entrada de histórico para notificação {notification_id}: {e}"
            )
        except Exception:
            pass
        if own_conn and conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if own_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


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


def safe_int(v, default=0):
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int,)):
            return int(v)
        # strings like '1'
        return int(str(v).strip())
    except Exception:
        return default

def truthy(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "yes", "y", "sim", "s", "ok", "aprovado")


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
            conn=conn,
            cursor=cur
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
    """Busca anexos para uma notificação específica.

    Observação:
    - Em alguns fluxos antigos, o mesmo arquivo pode acabar registrado mais de uma vez.
      Para evitar duplicação visual, deduplicamos por 'original_name' mantendo o mais recente.
    """
    created_conn = False
    created_cur = False
    local_conn = conn
    local_cur = cur
    try:
        if local_conn is None:
            local_conn = get_db_connection()
            created_conn = True
        if local_cur is None:
            local_cur = local_conn.cursor()
            created_cur = True

        local_cur.execute(
            """
            SELECT id, unique_name, original_name
            FROM notification_attachments
            WHERE notification_id = %s
            ORDER BY id ASC
            """,
            (notification_id,),
        )
        rows = local_cur.fetchall() or []

        # Dedup por original_name (mantém o último id)
        by_name = {}
        for rid, uniq, orig in rows:
            if orig is None:
                orig = uniq
            by_name[str(orig)] = {"id": rid, "unique_name": uniq, "original_name": orig}

        ordered = sorted(by_name.values(), key=lambda x: x["id"])
        return [{"unique_name": r["unique_name"], "original_name": r["original_name"]} for r in ordered]

    except psycopg2.Error:
        return []
    finally:
        try:
            if created_cur and local_cur:
                local_cur.close()
        finally:
            if created_conn and local_conn:
                local_conn.close()


def split_attachments_by_origin(notification_id: int) -> Dict[str, List[Dict]]:
    """
    Separa anexos em dois grupos:
      - 'notification': anexos cadastrados na tabela notification_attachments e NÃO referenciados por ações.
      - 'execution'   : anexos referenciados em notification_actions.evidence_attachments (mesmo que não estejam na tabela).

    Motivo: a tabela notification_attachments não possui coluna 'source'. Então distinguimos cruzando com as ações.
    """
    # Anexos da tabela (globais)
    all_atts = get_notification_attachments(int(notification_id)) if notification_id is not None else []

    # Anexos citados nas ações (execução)
    exec_by_unique = {}
    try:
        actions = get_notification_actions(int(notification_id))
        for a in actions or []:
            ev = a.get("evidence_attachments") or a.get("attachments") or []
            # Pode vir como string JSON
            if isinstance(ev, str):
                try:
                    import json
                    ev = json.loads(ev)
                except Exception:
                    ev = []
            if isinstance(ev, list):
                for item in ev:
                    if isinstance(item, dict):
                        u = item.get("unique_name") or item.get("filename")
                        o = item.get("original_name") or u
                        if u:
                            exec_by_unique[str(u)] = {"unique_name": str(u), "original_name": o or str(u)}
                    elif isinstance(item, str):
                        exec_by_unique[str(item)] = {"unique_name": str(item), "original_name": str(item)}
    except Exception:
        pass

    exec_unique = set(exec_by_unique.keys())

    # Grupo notificação = tudo que está na tabela e não está referenciado por ação
    notif_group = []
    for a in all_atts or []:
        if isinstance(a, dict):
            u = str(a.get("unique_name") or "")
            if u and u not in exec_unique:
                notif_group.append({"unique_name": u, "original_name": a.get("original_name") or u})
        elif isinstance(a, str):
            if a not in exec_unique:
                notif_group.append({"unique_name": a, "original_name": a})

    # Grupo execução = itens referenciados por ação (ordena por original_name para ficar estável)
    exec_group = list(exec_by_unique.values())
    exec_group.sort(key=lambda x: (str(x.get("original_name") or ""), str(x.get("unique_name") or "")))

    # Dedup simples no grupo notificação (por unique_name)
    seen = set()
    notif_unique = []
    for a in notif_group:
        u = a.get("unique_name")
        if u and u not in seen:
            seen.add(u)
            notif_unique.append(a)

    return {"notification": notif_unique, "execution": exec_group}


def render_attachments_download(title: str, attachments: List[Dict], key_prefix: str) -> None:
    """Renderiza anexos com download; funciona com arquivo vazio (b'')."""
    if not attachments:
        return
    st.markdown(f"#### {title}")
    for attach_info in attachments:
        if not isinstance(attach_info, dict):
            continue
        unique_name = str(attach_info.get("unique_name") or "")
        original_name = str(attach_info.get("original_name") or unique_name)
        if not unique_name:
            continue
        try:
            data = get_attachment_data(unique_name)
        except Exception as e:
            st.write(f"Anexo: {original_name} (erro ao ler: {e})")
            continue
        if data is None:
            st.write(f"Anexo: {original_name} (arquivo não encontrado)")
            continue
        st.download_button(
            label=f"⬇️ {original_name}",
            data=data,
            file_name=original_name,
            mime="application/octet-stream",
            key=f"{key_prefix}_{unique_name}",
        )

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


    # 📎 Anexos da notificação (tabela notification_attachments)
    try:
        notif_id_for_attachments = notification.get('id')
        if notif_id_for_attachments is not None:
            atts = get_notification_attachments(int(notif_id_for_attachments))
        else:
            atts = []
    except Exception:
        atts = []
    if atts:
        groups = split_attachments_by_origin(int(notif_id_for_attachments)) if notif_id_for_attachments is not None else {"notification": [], "execution": []}
        notif_atts = groups.get("notification", []) or []
        exec_atts = groups.get("execution", []) or []
        if notif_atts or exec_atts:
            st.markdown("#### 📎 Anexos")
            # Mostra separado para ficar claro a origem
            if notif_atts:
                render_attachments_download("📄 Anexos da Notificação", notif_atts, key_prefix=f"dl_notif_{notif_id_for_attachments}")
            if exec_atts:
                render_attachments_download("🛠️ Anexos da Execução", exec_atts, key_prefix=f"dl_exec_{notif_id_for_attachments}")
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
            # 📎 Anexos vinculados a esta ação (podem existir mesmo sem conclusão)
            action_attachments = action.get('attachments') or []
            if isinstance(action_attachments, list) and action_attachments:
                st.markdown("**📎 Anexos desta ação:**")
                for attach_info in action_attachments:
                    if not isinstance(attach_info, dict):
                        continue
                    unique_name = attach_info.get('unique_name')
                    original_name = attach_info.get('original_name')
                    if unique_name and original_name:
                        file_content = get_attachment_data(unique_name)
                        if file_content is not None:
                            st.download_button(
                                label=f"⬇️ {original_name}",
                                data=file_content,
                                file_name=original_name,
                                mime="application/octet-stream",
                                key=f"download_action_attachment_{notification.get('id')}_{unique_name}"
                            )
                        else:
                            st.write(f"Anexo: {original_name} (arquivo não encontrado ou corrompido)")

            # Evidências da conclusão (quando o executor finalizou)
            if action.get('final_action_by_executor'):
                evidence_desc = (action.get('evidence_description') or '').strip()
                evidence_atts = action.get('evidence_attachments') or []
                if evidence_desc or evidence_atts:
                    st.markdown("<div class='evidence-section'>", unsafe_allow_html=True)
                    st.markdown("<h6>Evidências da Conclusão:</h6>", unsafe_allow_html=True)
                    if evidence_desc:
                        st.info(evidence_desc)
                    if isinstance(evidence_atts, list) and evidence_atts:
                        for attach_info in evidence_atts:
                            if not isinstance(attach_info, dict):
                                continue
                            unique_name = attach_info.get('unique_name')
                            original_name = attach_info.get('original_name')
                            if unique_name and original_name:
                                file_content = get_attachment_data(unique_name)
                                if file_content is not None:
                                    st.download_button(
                                        label=f"Baixar Evidência: {original_name}",
                                        data=file_content,
                                        file_name=original_name,
                                        mime="application/octet-stream",
                                        key=f"download_action_evidence_{notification.get('id')}_{unique_name}"
                                    )
                                else:
                                    st.write(f"Anexo: {original_name} (arquivo não encontrado ou corrompido)")
                    st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("---")

def build_notification_report(notification_id: int) -> str:
    """Gera um relatório TXT completo (notificação + classificação + execução + revisões + aprovações + histórico).

    O objetivo é o usuário conseguir baixar e arquivar o ciclo completo.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id, title, description, location, occurrence_date, occurrence_time,
                reporting_department, reporting_department_complement, notified_department,
                notified_department_complement, event_shift,
                immediate_actions_taken, immediate_action_description,
                patient_involved, patient_id, patient_outcome_obito,
                additional_notes, status, created_at, updated_at,
                classification, rejection_classification, review_execution, approval,
                rejection_approval, rejection_execution_review, conclusion,
                executors, approver
            FROM notifications
            WHERE id = %s
            """,
            (notification_id,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return f"Relatório\n\nNotificação {notification_id} não encontrada."

        # mapeamento rápido
        keys = [
            "id","title","description","location","occurrence_date","occurrence_time",
            "reporting_department","reporting_department_complement","notified_department",
            "notified_department_complement","event_shift",
            "immediate_actions_taken","immediate_action_description",
            "patient_involved","patient_id","patient_outcome_obito",
            "additional_notes","status","created_at","updated_at",
            "classification","rejection_classification","review_execution","approval",
            "rejection_approval","rejection_execution_review","conclusion",
            "executors","approver"
        ]
        n = dict(zip(keys, row))

        def _json_to_obj(v):
            if v is None:
                return None
            if isinstance(v, (dict, list)):
                return v
            if isinstance(v, str):
                v = v.strip()
                if not v:
                    return None
                try:
                    return json.loads(v)
                except Exception:
                    return v
            return v

        for k in ["classification","rejection_classification","review_execution","approval","rejection_approval","rejection_execution_review","conclusion"]:
            n[k] = _json_to_obj(n.get(k))

        # histórico e ações
        hist_map = _get_history_map_by_ids(conn, [notification_id])
        act_map = _get_actions_map_by_ids(conn, [notification_id])
        atts = get_notification_attachments(notification_id, conn=conn)  # já dedup
        history = hist_map.get(notification_id, []) or []
        actions = act_map.get(notification_id, []) or []

        # formatação
        def fmt_dt(v):
            try:
                return v.isoformat(sep=" ") if hasattr(v, "isoformat") else str(v)
            except Exception:
                return str(v)

        lines = []
        lines.append("="*78)
        lines.append(f"RELATÓRIO COMPLETO — NOTIFICAÇÃO #{n['id']}")
        lines.append("="*78)
        lines.append("")
        lines.append("[1] NOTIFICAÇÃO")
        lines.append(f"Título: {n.get('title','')}")
        lines.append(f"Status atual: {n.get('status','')}")
        lines.append(f"Criada em: {fmt_dt(n.get('created_at'))}")
        lines.append(f"Atualizada em: {fmt_dt(n.get('updated_at'))}")
        lines.append(f"Local: {n.get('location','')}")
        lines.append(f"Data/Hora ocorrência: {n.get('occurrence_date','')} {n.get('occurrence_time','')}")
        lines.append(f"Depto notificante: {n.get('reporting_department','')} {n.get('reporting_department_complement','')}")
        lines.append(f"Depto notificado: {n.get('notified_department','')} {n.get('notified_department_complement','')}")
        lines.append(f"Turno: {n.get('event_shift','')}")
        lines.append("")
        lines.append("Descrição:")
        lines.append(n.get('description','') or "")
        lines.append("")
        lines.append("Ações imediatas tomadas: " + ("Sim" if n.get('immediate_actions_taken') else "Não"))
        if n.get('immediate_action_description'):
            lines.append("Descrição ação imediata: " + str(n.get('immediate_action_description')))
        lines.append("Paciente envolvido: " + ("Sim" if n.get('patient_involved') else "Não"))
        if n.get('patient_id'):
            lines.append("Paciente ID: " + str(n.get('patient_id')))
        if n.get('patient_outcome_obito') is not None:
            lines.append("Óbito: " + ("Sim" if n.get('patient_outcome_obito') else "Não"))
        if n.get('additional_notes'):
            lines.append("")
            lines.append("Observações adicionais:")
            lines.append(str(n.get('additional_notes')))
        lines.append("")
        lines.append("Anexos da notificação:")
        if atts:
            for a in atts:
                lines.append(f"- {a.get('original_name')} ({a.get('unique_name')})")
        else:
            lines.append("- (sem anexos)")
        lines.append("")
        lines.append("[2] CLASSIFICAÇÃO")
        if n.get('classification'):
            lines.append(json.dumps(n.get('classification'), ensure_ascii=False, indent=2))
        else:
            lines.append("(não classificada)")
        lines.append("")
        lines.append("[3] EXECUÇÃO — AÇÕES DOS EXECUTORES")
        if actions:
            for i, a in enumerate(actions, 1):
                lines.append("-"*78)
                lines.append(f"Ação {i}")
                lines.append(f"Executor: {a.get('executor_name','')} (id={a.get('executor_id')})")
                lines.append(f"Quando: {a.get('action_timestamp')}")
                lines.append(f"Descrição: {a.get('description','')}")
                lines.append(f"Marcou conclusão do executor: {bool(a.get('final_action_by_executor'))}")
                if a.get('evidence_description'):
                    lines.append(f"Evidência: {a.get('evidence_description')}")
                # anexos da ação (armazenados em evidence_attachments)
                eatt = a.get('evidence_attachments')
                if isinstance(eatt, str):
                    try:
                        eatt = json.loads(eatt)
                    except Exception:
                        eatt = None
                if eatt:
                    lines.append("Anexos da ação:")
                    for ea in eatt:
                        if isinstance(ea, dict):
                            lines.append(f"  * {ea.get('original_name')} ({ea.get('unique_name')})")
                        else:
                            lines.append(f"  * {ea}")
                else:
                    lines.append("Anexos da ação: (nenhum)")
            lines.append("-"*78)
        else:
            lines.append("(nenhuma ação registrada)")
        lines.append("")
        lines.append("[4] REVISÃO DA EXECUÇÃO")
        if n.get('review_execution'):
            lines.append(json.dumps(n.get('review_execution'), ensure_ascii=False, indent=2))
        else:
            lines.append("(sem revisão registrada)")
        lines.append("")
        lines.append("[5] APROVAÇÃO(ÕES)")
        if n.get('approval'):
            lines.append("Aprovação final:")
            lines.append(json.dumps(n.get('approval'), ensure_ascii=False, indent=2))
        else:
            lines.append("(sem aprovação final registrada)")
        lines.append("")
        lines.append("[6] CONCLUSÃO")
        if n.get('conclusion'):
            lines.append(json.dumps(n.get('conclusion'), ensure_ascii=False, indent=2))
        else:
            lines.append("(sem conclusão registrada)")
        lines.append("")
        lines.append("[7] HISTÓRICO (AUDITORIA)")
        if history:
            for h in history:
                lines.append(f"- {h.get('timestamp')} | {h.get('user')} | {h.get('action')} | {h.get('details','')}")
        else:
            lines.append("(sem histórico)")
        lines.append("")
        return "\n".join(lines)

    except Exception as e:
        return f"Relatório\n\nErro ao gerar relatório: {e}"
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass



def build_notification_report_pdf(notification_id: int) -> bytes:
    """Gera um PDF bem formatado com o ciclo completo da notificação (criação, classificação, execução, revisões/aprovações e auditoria).

    - Robusto a diferenças de schema (ex.: colunas do notification_history).
    - Retorna bytes do PDF para uso em st.download_button.
    """
    buf = io.BytesIO()
    styles = getSampleStyleSheet()

    def _p(text, style="Normal"):
        return Paragraph(text, styles[style])

    def _kv_table(rows):
        tbl = Table(rows, colWidths=[160, 360])
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (0,-1), colors.whitesmoke),
            ("BOX", (0,0), (-1,-1), 0.5, colors.grey),
            ("INNERGRID", (0,0), (-1,-1), 0.25, colors.lightgrey),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("LEFTPADDING", (0,0), (-1,-1), 6),
            ("RIGHTPADDING", (0,0), (-1,-1), 6),
            ("TOPPADDING", (0,0), (-1,-1), 4),
            ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ]))
        return tbl

    def _fmt_dt(v):
        if v is None:
            return UI_TEXTS.text_na if 'UI_TEXTS' in globals() else "N/A"
        try:
            if isinstance(v, datetime):
                return v.strftime("%d/%m/%Y %H:%M")
            return str(v)
        except Exception:
            return str(v)

    def _fmt_bool(v):
        if v is None:
            return UI_TEXTS.text_na if 'UI_TEXTS' in globals() else "N/A"
        return "Sim" if bool(v) else "Não"

    def _safe_json(v):
        if not v:
            return {}
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return {}
        return {}

    try:
        doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=42, bottomMargin=36)
        story = []

        conn = get_db_connection()
        cur = conn.cursor()

        # --- Notificação (row como dict via cursor factory? se não for, usamos zip com keys como no seu padrão)
        cur.execute(
            """
            SELECT
                id, title, description, location, occurrence_date, occurrence_time,
                reporting_department, reporting_department_complement, notified_department,
                notified_department_complement, event_shift,
                immediate_actions_taken, immediate_action_description,
                patient_involved, patient_id, patient_outcome_obito,
                additional_notes, status, created_at, updated_at,
                classification, rejection_classification, review_execution, approval,
                rejection_approval, rejection_execution_review, conclusion,
                executors, approver
            FROM notifications
            WHERE id = %s
            """,
            (notification_id,),
        )
        row = cur.fetchone()

        if not row:
            story.append(_p(f"Relatório — Notificação #{notification_id}", "Title"))
            story.append(Spacer(1, 12))
            story.append(_p("Notificação não encontrada.", "Normal"))
            doc.build(story)
            return buf.getvalue()

        keys = [
            "id","title","description","location","occurrence_date","occurrence_time",
            "reporting_department","reporting_department_complement","notified_department",
            "notified_department_complement","event_shift",
            "immediate_actions_taken","immediate_action_description",
            "patient_involved","patient_id","patient_outcome_obito",
            "additional_notes","status","created_at","updated_at",
            "classification","rejection_classification","review_execution","approval",
            "rejection_approval","rejection_execution_review","conclusion",
            "executors","approver"
        ]
        n = dict(zip(keys, row))

        # ---- Cabeçalho
        story.append(_p("RELATÓRIO DE NOTIFICAÇÃO DE EVENTO", "Title"))
        story.append(_p(f"Notificação nº <b>#{notification_id}</b>  —  Gerado em <b>{datetime.now().strftime('%d/%m/%Y %H:%M')}</b>", "Normal"))
        story.append(Spacer(1, 14))

        # ---- 1) Identificação
        story.append(_p("1) Identificação da Notificação", "Heading2"))
        story.append(Spacer(1, 6))

        ident_rows = [
            ["Título", n.get("title") or "—"],
            ["Status atual", n.get("status") or "—"],
            ["Criada em", _fmt_dt(n.get("created_at"))],
            ["Atualizada em", _fmt_dt(n.get("updated_at"))],
            ["Local", n.get("location") or "—"],
            ["Data/Hora da ocorrência", f"{n.get('occurrence_date') or ''} {n.get('occurrence_time') or ''}".strip() or "—"],
            ["Setor notificante", f"{n.get('reporting_department') or ''} {n.get('reporting_department_complement') or ''}".strip() or "—"],
            ["Setor notificado", f"{n.get('notified_department') or ''} {n.get('notified_department_complement') or ''}".strip() or "—"],
            ["Turno", n.get("event_shift") or "—"],
            ["Ações imediatas tomadas", _fmt_bool(n.get("immediate_actions_taken"))],
            ["Paciente envolvido", _fmt_bool(n.get("patient_involved"))],
        ]
        story.append(_kv_table(ident_rows))
        story.append(Spacer(1, 12))

        # ---- 2) Descrição
        story.append(_p("2) Descrição do Evento", "Heading2"))
        story.append(Spacer(1, 6))
        story.append(_p(n.get("description") or "—", "BodyText"))
        story.append(Spacer(1, 10))

        if n.get("immediate_actions_taken"):
            story.append(_p("<b>Ações imediatas descritas:</b>", "Normal"))
            story.append(_p(n.get("immediate_action_description") or "—", "BodyText"))
            story.append(Spacer(1, 8))

        if n.get("additional_notes"):
            story.append(_p("<b>Observações adicionais:</b>", "Normal"))
            story.append(_p(n.get("additional_notes") or "—", "BodyText"))
            story.append(Spacer(1, 12))

        # ---- Anexos (separando Notificação vs Execução)
        try:
            groups = split_attachments_by_origin(int(notification_id))
            notif_atts = groups.get("notification", []) or []
            exec_atts = groups.get("execution", []) or []
        except Exception:
            notif_atts, exec_atts = [], []

        if notif_atts or exec_atts:
            story.append(_p("Anexos", "Heading2"))
            story.append(Spacer(1, 6))

            if notif_atts:
                story.append(_p("Anexos da Notificação", "Heading3"))
                rows = [[f"• {a.get('original_name') or a.get('unique_name')}"] for a in notif_atts if isinstance(a, dict)]
                if rows:
                    t = Table(rows, colWidths=[520])
                    t.setStyle(TableStyle([("BOX",(0,0),(-1,-1),0.5,colors.lightgrey),
                                           ("INNERGRID",(0,0),(-1,-1),0.25,colors.lightgrey),
                                           ("VALIGN",(0,0),(-1,-1),"TOP")]))
                    story.append(t)
                story.append(Spacer(1, 10))

            if exec_atts:
                story.append(_p("Anexos da Execução", "Heading3"))
                rows = [[f"• {a.get('original_name') or a.get('unique_name')}"] for a in exec_atts if isinstance(a, dict)]
                if rows:
                    t = Table(rows, colWidths=[520])
                    t.setStyle(TableStyle([("BOX",(0,0),(-1,-1),0.5,colors.lightgrey),
                                           ("INNERGRID",(0,0),(-1,-1),0.25,colors.lightgrey),
                                           ("VALIGN",(0,0),(-1,-1),"TOP")]))
                    story.append(t)
                story.append(Spacer(1, 12))

        # ---- 3) Classificação
        story.append(_p("3) Classificação do Evento", "Heading2"))
        story.append(Spacer(1, 6))
        classif = _safe_json(n.get("classification"))
        if not classif:
            story.append(_p("Sem classificação registrada.", "Normal"))
            story.append(Spacer(1, 10))
        else:
            # principais primeiro (usando nomenclatura “humana”)
            nice = [
                ("Classificação NNC", classif.get("nnc")),
                ("Classificação OMS", ", ".join(classif.get("oms") or []) if isinstance(classif.get("oms"), list) else classif.get("oms")),
                ("Prioridade", classif.get("prioridade")),
                ("Nível de dano", classif.get("nivel_dano")),
                ("Never Event", classif.get("never_event")),
                ("Evento sentinela", "Sim" if classif.get("is_sentinel_event") else "Não" if classif.get("is_sentinel_event") is not None else None),
                ("Tipo principal", classif.get("event_type_main")),
                ("Especificações", ", ".join(classif.get("event_type_sub") or []) if isinstance(classif.get("event_type_sub"), list) else classif.get("event_type_sub")),
                ("Setor notificante", classif.get("reporting_sector")),
                ("Setor responsável", classif.get("responsible_sector")),
                ("Prazo calculado", classif.get("deadline_calculated")),
                ("Observações", classif.get("observations")),
                ("Classificado por", classif.get("classified_by")),
                ("Classificado em", classif.get("classified_at")),
            ]
            rows = [[k, (v if v not in [None, ""] else "—")] for k, v in nice if k]
            story.append(_kv_table(rows))
            story.append(Spacer(1, 12))

        # ---- 4) Execução
        story.append(_p("4) Execução — Ações dos Executores", "Heading2"))
        story.append(Spacer(1, 6))
        cur.execute(
            """
            SELECT executor_id, description, action_timestamp, final_action_by_executor, evidence_attachments
            FROM notification_actions
            WHERE notification_id = %s
            ORDER BY action_timestamp ASC
            """,
            (notification_id,),
        )
        action_rows = cur.fetchall() or []
        if not action_rows:
            story.append(_p("Nenhuma ação registrada pelos executores.", "Normal"))
            story.append(Spacer(1, 10))
        else:
            for idx, a in enumerate(action_rows, start=1):
                story.append(_p(f"<b>Ação {idx}</b>", "Heading3"))
                story.append(_kv_table([
                    ["Executor (id)", a[0]],
                    ["Descrição", a[1] or "—"],
                    ["Quando", _fmt_dt(a[2])],
                    ["Conclusão do executor", "Sim" if a[3] else "Não"],
                ]))
                story.append(Spacer(1, 6))
                # anexos da ação (evidence_attachments)
                atts = a[4]
                if isinstance(atts, str):
                    try:
                        atts = json.loads(atts)
                    except Exception:
                        atts = []
                if atts:
                    story.append(_p("Anexos da ação:", "Normal"))
                    rows = [[f"• {x.get('original_name') or x.get('unique_name')}"] for x in atts if isinstance(x, dict)]
                    t = Table(rows, colWidths=[520])
                    t.setStyle(TableStyle([("BOX",(0,0),(-1,-1),0.5,colors.lightgrey),("INNERGRID",(0,0),(-1,-1),0.25,colors.lightgrey)]))
                    story.append(t)
                    story.append(Spacer(1, 10))
                story.append(Spacer(1, 6))

        # ---- 5) Revisão da execução
        story.append(_p("5) Revisão da Execução", "Heading2"))
        story.append(Spacer(1, 6))
        rev = _safe_json(n.get("review_execution"))
        if not rev:
            story.append(_p("Sem revisão de execução registrada.", "Normal"))
        else:
            rows = [
                ["Decisão", rev.get("decision") or "—"],
                ["Observações", rev.get("observations") or "—"],
                ["Revisado por", rev.get("reviewed_by_username") or rev.get("reviewed_by_id") or "—"],
                ["Revisado em", rev.get("reviewed_at") or "—"],
            ]
            story.append(_kv_table(rows))
        story.append(Spacer(1, 12))

        # ---- 6) Aprovação final
        story.append(_p("6) Aprovação(ões)", "Heading2"))
        story.append(Spacer(1, 6))
        appr = _safe_json(n.get("approval"))
        if not appr:
            story.append(_p("Sem aprovação final registrada.", "Normal"))
        else:
            rows = [
                ["Decisão", appr.get("decision") or "—"],
                ["Observações", appr.get("observations") or "—"],
                ["Aprovado por", appr.get("approved_by_username") or appr.get("approved_by_id") or "—"],
                ["Aprovado em", appr.get("approved_at") or "—"],
            ]
            story.append(_kv_table(rows))
        story.append(Spacer(1, 12))

        # ---- 7) Histórico (auditoria) — robusto a schema
        story.append(_p("7) Histórico (Auditoria)", "Heading2"))
        story.append(Spacer(1, 6))

        # Descobre colunas disponíveis
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'notification_history'
            """
        )
        cols = {r[0] for r in (cur.fetchall() or [])}

        ts_col = "timestamp" if "timestamp" in cols else ("action_timestamp" if "action_timestamp" in cols else ("created_at" if "created_at" in cols else None))
        user_col = "username" if "username" in cols else ("user_name" if "user_name" in cols else ("user" if "user" in cols else None))
        action_col = "action" if "action" in cols else ("action_text" if "action_text" in cols else ("description" if "description" in cols else None))
        details_col = "details" if "details" in cols else None

        if not ts_col or not user_col or not action_col:
            story.append(_p("Histórico indisponível (schema inesperado em notification_history).", "Normal"))
        else:
            select_cols = [ts_col, user_col, action_col]
            if details_col:
                select_cols.append(details_col)

            cur.execute(
                f"SELECT {', '.join(select_cols)} FROM notification_history WHERE notification_id = %s ORDER BY {ts_col} ASC",
                (notification_id,),
            )
            hist = cur.fetchall() or []
            if not hist:
                story.append(_p("Sem eventos de histórico registrados.", "Normal"))
            else:
                # cada linha: ts, user, action, details?
                for h in hist:
                    ts_v = h[0]
                    user_v = h[1]
                    act_v = h[2]
                    det_v = h[3] if (details_col and len(h) > 3) else ""
                    line = f"<b>{_fmt_dt(ts_v)}</b> — {user_v or '—'}<br/>{act_v or '—'}"
                    if det_v:
                        line += f"<br/><i>{det_v}</i>"
                    story.append(_p(line, "BodyText"))
                    story.append(Spacer(1, 6))

        cur.close()
        conn.close()

        doc.build(story)
        return buf.getvalue()

    except Exception as e:
        # Nunca falha silenciosamente: devolve um PDF com erro
        err_doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=42, bottomMargin=36)
        story = [_p(f"Relatório — Notificação #{notification_id}", "Title"), Spacer(1, 12),
                 _p(f"Erro ao gerar PDF: {str(e)}", "Normal")]
        err_doc.build(story)
        return buf.getvalue()


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
    Tela dedicada para classificação inicial e gerenciamento de classificações realizadas.
    """
    if not check_permission('classificador'):
        st.error("❌ Acesso negado! Você não tem permissão para acessar esta página.")
        return
    
    st.markdown("<h1 class='main-header'>⏳ Central de Classificação</h1>", unsafe_allow_html=True)
    
    # Criação das abas
    tab_pendentes, tab_historico = st.tabs(["📥 Pendentes de Classificação", "🗂️ Minhas Classificações"])
    
    # ==============================================================================
    # ABA 1: PENDENTES (Fluxo Original)
    # ==============================================================================
    with tab_pendentes:
        st.markdown("### 📥 Notificações Aguardando Classificação")
        
        pending_notifications = load_notifications_by_status("pendente_classificacao")
        
        if not pending_notifications:
            st.success("✅ Não há notificações pendentes de classificação inicial no momento.")
            st.info("💡 Todas as notificações foram classificadas ou estão em outras etapas do fluxo.")
        else:
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
                        if not motivo_rejeicao or not motivo_rejeicao.strip():
                            st.error("⚠️ **Erro de Validação**")
                            st.warning("📝 O motivo da rejeição é obrigatório.")
                        else:
                            rejection_data = {
                                "rejected_by": st.session_state.user_username,
                                "rejected_by_id": st.session_state.user_id,
                                "rejection_reason": motivo_rejeicao.strip(),
                                "rejected_at": datetime.now().isoformat()
                            }
                            updates = {
                                'status': 'rejeitada',
                                'rejection_classification': rejection_data
                            }
                            updated_notif = update_notification(notif_id, updates)
                            if updated_notif:
                                add_history_entry(
                                    notif_id,
                                    "Notificação rejeitada na classificação inicial",
                                    st.session_state.user_username,
                                    f"Motivo: {motivo_rejeicao.strip()}"
                                )
                                st.success("✅ Notificação rejeitada com sucesso!")
                                st.session_state.pop(decisao_key, None)
                                st.session_state.pop(motivo_rejeicao_key, None)
                                time_module.sleep(1.5)
                                st.rerun()
                            else:
                                st.error("❌ Erro ao rejeitar a notificação.")
                
                with col_reject2:
                    if st.button("❌ Cancelar Rejeição", use_container_width=True):
                        st.session_state[decisao_key] = "Selecione"
                        st.rerun()
            
            # ========== FLUXO DE CLASSIFICAÇÃO ==========
            elif decisao == "Classificar Notificação":
                st.markdown("""
                <div class="form-section">
                    <h4>📋 Dados de Classificação</h4>
                    <p>Preencha todos os campos obrigatórios.</p>
                </div>
                """, unsafe_allow_html=True)
                
                col_pre1, col_pre2 = st.columns(2)
                with col_pre1:
                    classificacao_options = [UI_TEXTS.selectbox_default_classificacao_nnc] + FORM_DATA.classificacao_nnc
                    classificacao_key = f"classificacao_{notif_id}"
                    classificacao = st.selectbox(
                        "📋 Classificação NNC *",
                        options=classificacao_options,
                        index=classificacao_options.index(st.session_state.get(classificacao_key, UI_TEXTS.selectbox_default_classificacao_nnc)) if st.session_state.get(classificacao_key) in classificacao_options else 0,
                        key=classificacao_key
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
                            key=nivel_dano_key
                        )
                    else:
                        if nivel_dano_key in st.session_state:
                            st.session_state[nivel_dano_key] = UI_TEXTS.selectbox_default_nivel_dano
                
                st.markdown("---")
                
                tipo_evento_principal_options = [UI_TEXTS.selectbox_default_tipo_principal] + list(FORM_DATA.tipos_evento_principal.keys())
                tipo_evento_principal_key = f"tipo_evento_{notif_id}"
                tipo_evento_principal = st.selectbox(
                    "Tipo Principal de Evento *",
                    options=tipo_evento_principal_options,
                    index=tipo_evento_principal_options.index(st.session_state.get(tipo_evento_principal_key, UI_TEXTS.selectbox_default_tipo_principal)) if st.session_state.get(tipo_evento_principal_key) in tipo_evento_principal_options else 0,
                    key=tipo_evento_principal_key
                )
                
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
                            key=tipo_evento_sub_key
                        )
                
                st.markdown("---")
                
                with st.form(key=f"form_classif_inicial_{notif_id}"):
                    col_form1, col_form2 = st.columns(2)
                    with col_form1:
                        prioridade_options = [UI_TEXTS.selectbox_default_prioridade_resolucao] + FORM_DATA.prioridades
                        prioridade = st.selectbox("🎯 Prioridade *", options=prioridade_options, key=f"prioridade_{notif_id}")
                        never_event_options = [UI_TEXTS.selectbox_never_event_na_text] + FORM_DATA.never_events
                        never_event = st.selectbox("🚨 Never Event *", options=never_event_options, key=f"never_event_{notif_id}")
                    
                    with col_form2:
                        setor_notificante_options = [UI_TEXTS.selectbox_default_department_select] + FORM_DATA.SETORES
                        setor_notificante_default = selected_notification.get('reporting_department', UI_TEXTS.selectbox_default_department_select)
                        setor_notificante_index = setor_notificante_options.index(setor_notificante_default) if setor_notificante_default in setor_notificante_options else 0
                        setor_notificante = st.selectbox("🏥 Setor Notificante *", options=setor_notificante_options, index=setor_notificante_index, key=f"setor_notificante_{notif_id}")
                        
                        setor_options = [UI_TEXTS.selectbox_default_department_select] + FORM_DATA.SETORES
                        setor_notificado_default = selected_notification.get('notified_department', UI_TEXTS.selectbox_default_department_select)
                        setor_notificado_index = setor_options.index(setor_notificado_default) if setor_notificado_default in setor_options else 0
                        setor_responsavel = st.selectbox("🏢 Setor Notificado *", options=setor_options, index=setor_notificado_index, key=f"setor_{notif_id}")
                    
                    evento_sentinela_options = [UI_TEXTS.selectbox_default_evento_sentinela, "Sim", "Não"]
                    evento_sentinela = st.selectbox("⚠️ Evento Sentinela? *", options=evento_sentinela_options, key=f"evento_sentinela_{notif_id}")
                    
                    classificacao_oms = st.multiselect("🏥 Classificação OMS: *", options=FORM_DATA.classificacao_oms, key=f"classificacao_oms_{notif_id}")
                    
                    all_executors = get_users_by_role('executor')
                    executor_options = [f"{e['name']} ({e['username']})" for e in all_executors]
                    executores_selecionados = st.multiselect("👥 Atribuir Executores Responsáveis: *", options=executor_options, key=f"executores_{notif_id}")
                    
                    observacoes_classificador = st.text_area("📝 Observações (opcional)", key=f"obs_classif_{notif_id}", height=100)
                    
                    st.markdown("<span class='required-field'>* Campos obrigatórios</span>", unsafe_allow_html=True)
                    submitted = st.form_submit_button("✅ Salvar Classificação", use_container_width=True, type="primary")
                    
                    if submitted:
                        # Validação Simplificada
                        if classificacao == UI_TEXTS.selectbox_default_classificacao_nnc or not executores_selecionados:
                            st.error("❌ Preencha todos os campos obrigatórios (Classificação e Executores são essenciais).")
                        else:
                            executor_name_to_id = {f"{e['name']} ({e['username']})": e['id'] for e in all_executors}
                            executor_ids = [executor_name_to_id[name] for name in executores_selecionados if name in executor_name_to_id]
                            
                            deadline_days = 0
                            if classificacao == "Evento com dano" and nivel_dano and nivel_dano != UI_TEXTS.selectbox_default_nivel_dano:
                                deadline_mapping = DEADLINE_DAYS_MAPPING.get("Evento com dano", {})
                                deadline_days = deadline_mapping.get(nivel_dano, 30)
                            else:
                                deadline_days = DEADLINE_DAYS_MAPPING.get(classificacao, 30)
                            prazo_conclusao = datetime.now() + timedelta(days=deadline_days)
                            
                            # CORREÇÃO: Salvando o prazo DENTRO do JSON de classificação
                            classification_data = {
                                "nnc": classificacao,
                                "nivel_dano": nivel_dano if classificacao == "Evento com dano" else None,
                                "prioridade": prioridade,
                                "never_event": never_event,
                                "event_type_main": tipo_evento_principal,
                                "event_type_sub": tipo_evento_sub,
                                "oms": classificacao_oms,
                                "is_sentinel_event": (evento_sentinela == "Sim"),
                                "reporting_sector": setor_notificante,
                                "responsible_sector": setor_responsavel,
                                "classified_by": st.session_state.user_username,
                                "classified_at": datetime.now().isoformat(),
                                "observations": observacoes_classificador,
                                "deadline_calculated": prazo_conclusao.isoformat() # <--- SALVANDO AQUI
                            }
                            
                            updates = {
                                'status': 'classificada_aguardando_execucao',
                                'classification': classification_data,
                                'executors': executor_ids
                                # 'deadline': ... REMOVIDO PARA EVITAR ERRO DE COLUNA
                            }
                            
                            if update_notification(notif_id, updates):
                                add_history_entry(notif_id, "Classificação inicial realizada", st.session_state.user_username, f"NNC: {classificacao}")
                                st.success("✅ Classificação salva com sucesso!")
                                time_module.sleep(1.5)
                                st.rerun()

    # ==============================================================================
    # ABA 2: MINHAS CLASSIFICAÇÕES (Nova Funcionalidade)
    # ==============================================================================
    with tab_historico:
        st.markdown("### 🗂️ Gerenciamento de Classificações Realizadas")
        st.markdown("Acompanhe o status das notificações classificadas por você e realize ajustes se necessário.")
        
        all_notifications = load_notifications() 
        
        my_classifications = [
            n for n in all_notifications 
            if (n.get('classification') or {}).get('classified_by') == st.session_state.user_username
            and n.get('status') != 'rejeitada'
        ]
        
        if not my_classifications:
            st.info("ℹ️ Você ainda não classificou nenhuma notificação.")
        else:
            my_classifications.sort(key=lambda x: x['created_at'], reverse=True)
            
            editing_id_key = "editing_classification_id"
            if editing_id_key not in st.session_state:
                st.session_state[editing_id_key] = None
            
            # --- MODO EDIÇÃO ---
            if st.session_state[editing_id_key] is not None:
                edit_id = st.session_state[editing_id_key]
                notif_to_edit = next((n for n in my_classifications if n['id'] == edit_id), None)
                
                if not notif_to_edit:
                    st.session_state[editing_id_key] = None
                    st.rerun()
                
                st.markdown(f"#### ✏️ Reclassificando Notificação #{edit_id}")
                st.warning("⚠️ Atenção: Alterar a classificação pode redefinir prazos e executores.")
                
                with st.container(border=True):
                    curr_class = notif_to_edit.get('classification', {})
                    
                    new_nnc = st.selectbox("Classificação NNC", FORM_DATA.classificacao_nnc, index=FORM_DATA.classificacao_nnc.index(curr_class.get('nnc')) if curr_class.get('nnc') in FORM_DATA.classificacao_nnc else 0, key=f"edit_nnc_{edit_id}")
                    
                    new_dano = None
                    if new_nnc == "Evento com dano":
                        new_dano = st.selectbox("Nível de Dano", FORM_DATA.niveis_dano, index=FORM_DATA.niveis_dano.index(curr_class.get('nivel_dano')) if curr_class.get('nivel_dano') in FORM_DATA.niveis_dano else 0, key=f"edit_dano_{edit_id}")
                    
                    new_prioridade = st.selectbox("Prioridade", FORM_DATA.prioridades, index=FORM_DATA.prioridades.index(curr_class.get('prioridade')) if curr_class.get('prioridade') in FORM_DATA.prioridades else 0, key=f"edit_prio_{edit_id}")
                    
                    all_execs = get_users_by_role('executor')
                    exec_options = [f"{e['name']} ({e['username']})" for e in all_execs]
                    
                    current_exec_ids = notif_to_edit.get('executors', [])
                    default_execs = []
                    for e in all_execs:
                        if e['id'] in current_exec_ids:
                            default_execs.append(f"{e['name']} ({e['username']})")
                            
                    new_execs = st.multiselect("Executores", exec_options, default=default_execs, key=f"edit_execs_{edit_id}")
                    
                    col_b1, col_b2 = st.columns(2)
                    with col_b1:
                        if st.button("💾 Salvar Alterações", type="primary", use_container_width=True, key=f"save_edit_{edit_id}"):
                            executor_name_to_id = {f"{e['name']} ({e['username']})": e['id'] for e in all_execs}
                            new_exec_ids = [executor_name_to_id[name] for name in new_execs if name in executor_name_to_id]
                            
                            deadline_days = 0
                            if new_nnc == "Evento com dano" and new_dano:
                                deadline_days = DEADLINE_DAYS_MAPPING.get("Evento com dano", {}).get(new_dano, 30)
                            else:
                                deadline_days = DEADLINE_DAYS_MAPPING.get(new_nnc, 30)
                            new_prazo = datetime.now() + timedelta(days=deadline_days)
                            
                            curr_class.update({
                                "nnc": new_nnc,
                                "nivel_dano": new_dano,
                                "prioridade": new_prioridade,
                                "classified_at": datetime.now().isoformat(),
                                "deadline_calculated": new_prazo.isoformat() # <--- SALVANDO AQUI NA EDIÇÃO
                            })
                            
                            updates = {
                                'classification': curr_class,
                                'executors': new_exec_ids
                                # 'deadline': ... REMOVIDO
                            }
                            
                            if update_notification(edit_id, updates):
                                add_history_entry(edit_id, "Reclassificação realizada", st.session_state.user_username, f"Alterado para {new_nnc} - {new_prioridade}")
                                st.success("✅ Reclassificado com sucesso!")
                                st.session_state[editing_id_key] = None
                                time_module.sleep(1)
                                st.rerun()
                                
                    with col_b2:
                        if st.button("❌ Cancelar", use_container_width=True, key=f"cancel_edit_{edit_id}"):
                            st.session_state[editing_id_key] = None
                            st.rerun()

            # --- MODO LISTA (TABELA) ---
            else:
                st.markdown("""
                <div style="display: grid; grid-template-columns: 0.5fr 2fr 1fr 1fr 1fr 0.8fr; gap: 10px; font-weight: bold; padding: 10px; background-color: #f0f2f6; border-radius: 5px; margin-bottom: 10px;">
                    <div>ID</div>
                    <div>Título</div>
                    <div>Classificação</div>
                    <div>Status</div>
                    <div>Prazo</div>
                    <div>Ação</div>
                </div>
                """, unsafe_allow_html=True)
                
                for n in my_classifications:
                    n_id = n['id']
                    title = n.get('title', 'Sem título')
                    status = n.get('status', '').replace('_', ' ').capitalize()
                    classif_data = n.get('classification') or {}
                    nnc = classif_data.get('nnc', 'N/A')
                    
                    # Lógica de Atraso: Agora busca dentro do JSON 'classification'
                    deadline_str = classif_data.get('deadline_calculated')
                    
                    # Fallback para tentar achar em outros lugares se for antigo
                    if not deadline_str:
                         deadline_str = n.get('deadline') or n.get('deadline_date')

                    is_late = False
                    deadline_display = "Sem prazo"
                    
                    if deadline_str:
                        try:
                            deadline_dt = datetime.fromisoformat(deadline_str)
                            deadline_display = deadline_dt.strftime('%d/%m/%Y')
                            if status != 'Concluida' and status != 'Encerrada' and datetime.now() > deadline_dt:
                                is_late = True
                        except:
                            pass
                    
                    deadline_html = f"<span>{deadline_display}</span>"
                    if is_late:
                        deadline_html = f"<span style='color: #d9534f; font-weight: bold;'>⚠️ {deadline_display} (Atrasado)</span>"
                    elif status in ['Concluida', 'Encerrada']:
                         deadline_html = f"<span style='color: #28a745;'>✅ Concluído</span>"
                    
                    status_color = "#6c757d"
                    if "execucao" in status.lower(): status_color = "#ffc107"
                    if "aprovacao" in status.lower(): status_color = "#17a2b8"
                    if "concluida" in status.lower(): status_color = "#28a745"
                    
                    status_badge = f"<span style='background-color: {status_color}; color: white; padding: 2px 8px; border-radius: 10px; font-size: 0.8em;'>{status}</span>"

                    col1, col2, col3, col4, col5, col6 = st.columns([0.5, 2, 1, 1, 1, 0.8])
                    
                    with col1: st.write(f"**#{n_id}**")
                    with col2: st.write(title)
                    with col3: st.write(nnc)
                    with col4: st.markdown(status_badge, unsafe_allow_html=True)
                    with col5: st.markdown(deadline_html, unsafe_allow_html=True)
                    with col6:
                        if status not in ['Concluida', 'Encerrada', 'Rejeitada']:
                            if st.button("✏️ Editar", key=f"btn_reclass_{n_id}", help="Reclassificar esta notificação"):
                                st.session_state[editing_id_key] = n_id
                                st.rerun()
                        else:
                            st.write("🔒")
                    
                    st.markdown("<hr style='margin: 5px 0;'>", unsafe_allow_html=True)
@st_fragment
def show_revisao_execucao():
    """
    Tela dedicada para revisão de execução concluída pelo classificador.
    Agora com layout em cards (colapsados) e 2 guias:
      - Em execução
      - Aguardando revisão de execução
    Na aprovação da execução, o classificador pode (opcionalmente) encaminhar para aprovação superior (1 aprovador).
    """
    if not check_permission('classificador'):
        st.error("❌ Acesso negado! Você não tem permissão para acessar esta página.")
        return

    st.markdown("<h1 class='main-header'>🛠️ Execução & Revisão de Execução</h1>", unsafe_allow_html=True)
    st.markdown("---")

    em_execucao = load_notifications_by_status("em_execucao") or []
    aguardando_revisao = load_notifications_by_status("revisao_classificador_execucao") or []

    tab_exec, tab_rev = st.tabs([
        f"🚧 Em execução ({len(em_execucao)})",
        f"🛠️ Revisão de execução ({len(aguardando_revisao)})"
    ])

    def _safe_iso_to_dt(s):
        try:
            return datetime.fromisoformat(s) if isinstance(s, str) else s
        except Exception:
            return None

    def _header_label(n):
        notif_id = n.get('id', 'N/A')
        title = n.get('title') or "Sem título"
        created = _safe_iso_to_dt(n.get('created_at'))
        created_disp = created.strftime('%d/%m/%Y %H:%M') if created else "N/A"

        classification = n.get('classification') or {}
        if isinstance(classification, str):
            try:
                classification = json.loads(classification)
            except Exception:
                classification = {}
        deadline = classification.get('deadline')
        prazo_dt = _safe_iso_to_dt(deadline)
        prazo_disp = prazo_dt.strftime('%d/%m/%Y') if prazo_dt else "N/A"

        status = n.get('status', 'N/A')
        return f"#{notif_id} | {title} | Criada: {created_disp} | Prazo: {prazo_disp} | Status: {status}"
    def _render_actions_with_attachments(notif_id, notif_obj=None):
        """Renderiza ações dos executores + anexos.
        Compatível com dois formatos:
          - Novo: actions persistidas em tabela notification_actions
          - Legado/Streamlit: actions dentro do JSON da notificação (campo 'actions')
        """
        notif_id_int = int(notif_id)

        # 1) Tenta ler do banco (tabela notification_actions)
        actions = get_notification_actions(notif_id_int)

        # 2) Se não houver nada no banco, faz fallback para o JSON da própria notificação
        if not actions:
            if notif_obj is None:
                try:
                    all_n = load_notifications() or []
                    notif_obj = next((n for n in all_n if int(n.get('id', -1)) == notif_id_int), None)
                except Exception:
                    notif_obj = None
            actions = (notif_obj or {}).get('actions', []) or []

        if not actions:
            st.info("ℹ️ Nenhuma ação registrada ainda.")
            return

        # Normaliza para um formato único para renderização
        def _norm_action(a: dict) -> dict:
            if not isinstance(a, dict):
                return {}
            out = {
                'executor_name': a.get('executor_name') or a.get('executor') or a.get('executorUsername') or a.get('executor_id') or 'Executor',
                'description': a.get('description') or a.get('desc') or '',
                'timestamp': a.get('timestamp') or a.get('action_timestamp') or a.get('created_at'),
                'final_action_by_executor': bool(a.get('final_action_by_executor') or a.get('final')),
                'attachments': a.get('attachments') or [],
                'evidence_description': a.get('evidence_description') or '',
                'evidence_attachments': a.get('evidence_attachments') or [],
            }
            # evidence_attachments pode vir como string JSON do banco
            if isinstance(out['evidence_attachments'], str):
                try:
                    out['evidence_attachments'] = json.loads(out['evidence_attachments']) or []
                except Exception:
                    out['evidence_attachments'] = []
            # attachments pode vir como string JSON (defensivo)
            if isinstance(out['attachments'], str):
                try:
                    out['attachments'] = json.loads(out['attachments']) or []
                except Exception:
                    out['attachments'] = []
            # Se não houver attachments explícitos, reutiliza evidence_attachments como anexos (compatibilidade)
            if (not out['attachments']) and out.get('evidence_attachments'):
                out['attachments'] = out['evidence_attachments']
            return out

        norm_actions = [_norm_action(a) for a in actions if isinstance(a, dict)]

        for idx, action in enumerate(norm_actions, 1):
            executor_name = action.get('executor_name') or 'Executor'
            action_label = f"📌 Ação {idx} - {executor_name}"

            ts_disp = ""
            ts_val = action.get('timestamp')
            if ts_val:
                try:
                    ts_disp = datetime.fromisoformat(str(ts_val)).strftime('%d/%m/%Y %H:%M')
                except Exception:
                    ts_disp = str(ts_val)
            if ts_disp:
                action_label += f" em {ts_disp}"

            with st.expander(action_label, expanded=False):
                st.markdown("**Descrição da Ação:**")
                st.markdown(action.get('description') or 'Sem descrição')

                if action.get('final_action_by_executor'):
                    st.caption("🏁 Marcada como conclusão do executor")

                # Anexos da ação (execução)
                anexos = action.get('attachments') or []
                if anexos:
                    st.markdown("**📎 Anexos da Ação:**")
                    for at in anexos:
                        # Formatos aceitos:
                        #  - dict: {"unique_name": "...", "original_name": "..."}
                        #  - str: unique_name
                        if isinstance(at, dict):
                            unique_name = at.get('unique_name') or at.get('attachment_id') or at.get('id')
                            original_name = at.get('original_name') or at.get('filename') or at.get('name') or (unique_name or 'anexo')
                        else:
                            unique_name = at
                            original_name = str(at)

                        if not unique_name:
                            continue

                        att_bytes = get_attachment_data(str(unique_name))
                        if att_bytes is not None:
                            st.download_button(
                                label=f"⬇️ {original_name}",
                                data=att_bytes,
                                file_name=original_name,
                                mime="application/octet-stream",
                                key=f"dl_action_att_{notif_id_int}_{idx}_{hash(str(unique_name))}"
                            )

                # Evidências (texto + anexos)
                if action.get('evidence_description'):
                    st.markdown("**Evidências:**")
                    st.markdown(action.get('evidence_description'))

                anexos_ev = action.get('evidence_attachments') or []
                if anexos_ev:
                    st.markdown("**📎 Anexos de Evidência:**")
                    for at in anexos_ev:
                        if isinstance(at, dict):
                            unique_name = at.get('unique_name') or at.get('attachment_id') or at.get('id')
                            original_name = at.get('original_name') or at.get('filename') or at.get('name') or (unique_name or 'evidencia')
                        else:
                            unique_name = at
                            original_name = str(at)

                        if not unique_name:
                            continue

                        att_bytes = get_attachment_data(str(unique_name))
                        if att_bytes is not None:
                            st.download_button(
                                label=f"⬇️ {original_name}",
                                data=att_bytes,
                                file_name=original_name,
                                mime="application/octet-stream",
                                key=f"dl_ev_att_{notif_id_int}_{idx}_{hash(str(unique_name))}"
                            )
    def _approver_options_for_select(n):
        classification = n.get('classification') or {}
        if isinstance(classification, str):
            try:
                classification = json.loads(classification)
            except Exception:
                classification = {}
        approver_users = get_users_by_role('aprovador') or []
        label_to_id = {}
        labels = []
        for u in approver_users:
            lab = f"{u.get('name', UI_TEXTS.text_na)} ({u.get('username', UI_TEXTS.text_na)})"
            label_to_id[lab] = u.get('id')
            labels.append(lab)

        default_id = None
        if isinstance(classification, dict) and classification.get('approver_id'):
            default_id = classification.get('approver_id')
        elif n.get('approver'):
            default_id = n.get('approver')

        default_index = 0
        if default_id and labels:
            for i, lab in enumerate(labels):
                if label_to_id.get(lab) == default_id:
                    default_index = i
                    break

        return labels, label_to_id, default_index

    with tab_exec:
        if not em_execucao:
            st.success("✅ Não há notificações em execução no momento.")
        else:
            st.info(f"📋 **{len(em_execucao)} notificação(ões)** em execução")
            for n in em_execucao:
                notif_id = n.get('id')
                with st.expander(_header_label(n), expanded=False):
                    # Detalhes completos (read-only)
                    try:
                        display_notification_full_details(
                            n,
                            st.session_state.get('user_id', 1),
                            st.session_state.get('user_username', 'classificador')
                        )
                    except Exception as e:
                        st.warning(f"Não foi possível renderizar detalhes completos: {e}")

                    st.markdown("### 🔧 Ações dos Executores")
                    _render_actions_with_attachments(notif_id)

    with tab_rev:
        if not aguardando_revisao:
            st.success("✅ Não há notificações aguardando revisão de execução no momento.")
            st.info("💡 Todas as execuções foram revisadas ou estão em outras etapas do fluxo.")
            return

        st.info(f"📋 **{len(aguardando_revisao)} notificação(ões)** aguardando revisão de execução")

        for selected_notification in aguardando_revisao:
            notif_id = selected_notification.get('id')

            with st.expander(_header_label(selected_notification), expanded=False):
                # Detalhes completos
                with st.expander("📋 Detalhes Completos da Notificação", expanded=False):
                    try:
                        display_notification_full_details(
                            selected_notification,
                            st.session_state.get('user_id', 1),
                            st.session_state.get('user_username', 'classificador')
                        )
                    except Exception as e:
                        st.warning(f"Não foi possível renderizar detalhes completos: {e}")

                st.markdown("### 🔧 Ações Realizadas pelos Executores")
                _render_actions_with_attachments(notif_id)

                st.markdown("---")
                st.markdown("## ✅ Revisão da Execução")

                # Controle: encaminhar para aprovação superior só se aprovar
                forward_key = f"forward_superior_{notif_id}"
                if forward_key not in st.session_state:
                    st.session_state[forward_key] = False

                labels, label_to_id, default_index = _approver_options_for_select(selected_notification)

                # --- Revisão de Execução (UI reativa fora de st.form para permitir escolher aprovador ANTES de salvar) ---
                decisao_options = [UI_TEXTS.selectbox_default_decisao_revisao, "✅ Aprovar Execução", "🔄 Solicitar Correções"]
                decisao = st.radio(
                    "📋 Decisão da Revisão *",
                    options=decisao_options,
                    index=0,
                    key=f"decisao_revisao_{notif_id}",
                    help="Aprovar: encerra a revisão. Opcionalmente você pode encaminhar para aprovação superior. Solicitar correções retorna para execução."
                )
                
                observacoes_revisao = st.text_area(
                    "📝 Observações da Revisão *",
                    key=f"obs_revisao_{notif_id}",
                    height=140,
                    placeholder="Descreva sua análise da execução. Se solicitar correções, especifique o que precisa ser ajustado.",
                )
                
                encaminhar = False
                selected_approver_id = None
                
                if decisao == "✅ Aprovar Execução":
                    # Se a notificação exigir aprovação superior, torna obrigatório escolher um aprovador.
                    _classif = selected_notification.get("classification") or {}
                    if isinstance(_classif, str):
                        try:
                            _classif = json.loads(_classif)
                        except Exception:
                            _classif = {}
                    requires_sup = truthy(_classif.get('requires_approval')) or truthy(selected_notification.get('requires_approval'))
                
                    if requires_sup:
                        encaminhar = True
                        if labels:
                            selected_label = st.selectbox(
                                "👤 Aprovador superior (obrigatório)",
                                options=labels,
                                index=default_index,
                                key=f"selected_approver_label_{notif_id}",
                                help="Esta notificação exige aprovação superior após a revisão da execução."
                            )
                            selected_approver_id = label_to_id.get(selected_label)
                        else:
                            st.error("❌ Nenhum usuário com perfil 'aprovador' foi encontrado. Cadastre um aprovador para prosseguir.")
                            selected_approver_id = None
                    else:
                        encaminhar = st.checkbox(
                            "➡️ Encaminhar para aprovação superior (opcional)",
                            value=st.session_state.get(forward_key, False),
                            key=forward_key
                        )
                        if encaminhar:
                            if labels:
                                selected_label = st.selectbox(
                                    "👤 Aprovador superior",
                                    options=labels,
                                    index=default_index,
                                    key=f"selected_approver_label_{notif_id}",
                                    help="Será encaminhado para este aprovador após você aprovar a execução."
                                )
                                selected_approver_id = label_to_id.get(selected_label)
                            else:
                                st.warning("⚠️ Nenhum usuário com perfil 'aprovador' foi encontrado.")
                                selected_approver_id = None
                
                st.markdown("<span class='required-field'>* Campos obrigatórios</span>", unsafe_allow_html=True)
                submitted = st.button("💾 Salvar Revisão", use_container_width=True, type="primary", key=f"btn_salvar_revisao_{notif_id}")
                
                if submitted:
                    if decisao == UI_TEXTS.selectbox_default_decisao_revisao:
                        st.error("❌ Por favor, selecione uma decisão para a revisão!")
                        st.stop()
                
                    if not (observacoes_revisao or "").strip():
                        st.error("❌ Por favor, preencha as observações da revisão!")
                        st.stop()
                
                    # Se marcou encaminhamento (ou é obrigatório), precisa escolher um aprovador válido
                    if decisao == "✅ Aprovar Execução" and encaminhar and not selected_approver_id:
                        st.error("❌ Selecione um aprovador superior para encaminhar a aprovação.")
                        st.stop()
                
                    if decisao == "🔄 Solicitar Correções":
                        new_status = "em_execucao"
                    else:
                        # aprovado
                        if encaminhar and selected_approver_id:
                            new_status = "aguardando_aprovacao"
                        else:
                            new_status = "concluida"
                
                    review_data = {
                        "decision": decisao,
                        "observations": observacoes_revisao.strip(),
                        "reviewed_at": datetime.now().isoformat(),
                        "reviewed_by_id": st.session_state.get('user_id', None),
                        "reviewed_by_username": st.session_state.get('user_username', None),
                        "forwarded_to_approver_id": int(selected_approver_id) if selected_approver_id else None,
                        "forwarded_to_approver": bool(encaminhar and selected_approver_id)
                    }
                
                    updates = {
                        "status": new_status,
                        "review_execution": review_data,
                    }
                
                    if new_status == "em_execucao":
                        updates["rejection_execution_review"] = {
                            "reason": observacoes_revisao.strip(),
                            "rejected_at": datetime.now().isoformat(),
                            "rejected_by_id": st.session_state.get('user_id', None),
                            "rejected_by_username": st.session_state.get('user_username', None),
                        }
                
                    if new_status == "aguardando_aprovacao" and selected_approver_id:
                        updates["approver"] = selected_approver_id
                
                    updated_notif = update_notification(notif_id, updates)
                
                    if updated_notif:
                        add_history_entry(
                            notif_id,
                            f"🔎 Revisão de execução registrada: {decisao}",
                            st.session_state.get('user_username', UI_TEXTS.text_na)
                        )
                        st.success("✅ Revisão salva com sucesso!")
                        st.rerun()
                    else:
                        st.error("❌ Erro ao atualizar a notificação. Verifique o log do servidor.")

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
    st.markdown("### 🔍 Visualizar Detalhes e Baixar Relatório")

    notification_display_options = [UI_TEXTS.selectbox_default_notification_select] + [
        f"ID {n['id']} - {n.get('title', 'Sem título')} ({n.get('status', UI_TEXTS.text_na)})"
        for n in filtered_notifications
    ]

    selected_detail_option_key = "detalhes_encerrada_select"
    if selected_detail_option_key not in st.session_state or st.session_state[selected_detail_option_key] >= len(notification_display_options) or st.session_state[selected_detail_option_key] < 0:
        st.session_state[selected_detail_option_key] = 0

    selected_index_details = st.selectbox(
        "Selecione uma notificação para ver detalhes completos e baixar o relatório:",
        range(len(notification_display_options)),
        format_func=lambda i: notification_display_options[i],
        key=selected_detail_option_key
    )

    if notification_display_options[selected_index_details] != UI_TEXTS.selectbox_default_notification_select:
        selected_notification = filtered_notifications[selected_index_details - 1]

        with st.expander(f"📋 **Ver Detalhes Completos da Notificação {selected_notification['id']}**", expanded=True):
            display_notification_full_details(selected_notification, st.session_state.user_id, st.session_state.user_username)

            st.markdown("---")
            st.markdown("### 📦 Relatório completo (download)")

            notif_id_int = int(selected_notification['id'])
            pdf_bytes = build_notification_report_pdf(notif_id_int)
            if pdf_bytes:
                st.download_button(
                    label="⬇️ Baixar relatório (PDF)",
                    data=pdf_bytes,
                    file_name=f"notificacao_{notif_id_int}_relatorio.pdf",
                    mime="application/pdf",
                    key=f"dl_relatorio_pdf_{notif_id_int}"
                )
            else:
                st.warning("Não foi possível gerar o PDF desta notificação.")

            report_txt = build_notification_report(notif_id_int)
            st.download_button(
                label="⬇️ Baixar relatório (TXT)",
                data=report_txt.encode("utf-8"),
                file_name=f"notificacao_{notif_id_int}_relatorio.txt",
                mime="text/plain",
                key=f"dl_relatorio_txt_{notif_id_int}"
            )

            with st.expander("👀 Prévia rápida do relatório (TXT)", expanded=False):
                st.text_area("Relatório", report_txt, height=320, key=f"preview_rel_txt_{notif_id_int}")

@st_fragment
def show_execution():
    """Página do executor: 2 guias (Pendentes / Executadas) com cards colapsados."""
    if not check_permission('executor'):
        st.error("❌ Acesso negado! Você não tem permissão para executar notificações.")
        return

    st.markdown("<h1 class='main-header'>⚡ Execução de Notificações</h1>", unsafe_allow_html=True)
    st.info("Use as guias abaixo para ver o que ainda está pendente e o que você já executou.")

    all_notifications = load_notifications() or []
    user_id = st.session_state.get('user_id')
    user_username = st.session_state.get('user_username')

    def _assigned_to_me(n: dict) -> bool:
        for e in (n.get('executors', []) or []):
            if isinstance(e, int) and e == user_id:
                return True
            if isinstance(e, dict) and e.get('id') == user_id:
                return True
        return False

    def _has_my_final_action(n: dict) -> bool:
        # Fonte de verdade: tabela notification_actions
        try:
            actions = get_notification_actions(int(n.get('id', 0))) or []
        except Exception:
            actions = []
        for a in actions:
            if a.get('executor_id') == user_id and a.get('final_action_by_executor'):
                return True
        return False

    def _parse_classif(n: dict) -> dict:
        c = n.get('classification') or {}
        if isinstance(c, str):
            try:
                c = json.loads(c)
            except Exception:
                c = {}
        return c if isinstance(c, dict) else {}

    def _fmt_dt(s: str, with_time: bool = True) -> str:
        if not s:
            return UI_TEXTS.text_na
        try:
            dt = datetime.fromisoformat(s)
            return dt.strftime('%d/%m/%Y %H:%M') if with_time else dt.strftime('%d/%m/%Y')
        except Exception:
            return s

    def _header(n: dict) -> str:
        nid = n.get('id', UI_TEXTS.text_na)
        title = n.get('title', UI_TEXTS.text_na)
        created = _fmt_dt(n.get('created_at'), with_time=True)
        classif = _parse_classif(n)
        prazo_raw = classif.get('deadline')
        prazo = _fmt_dt(prazo_raw, with_time=False) if prazo_raw else UI_TEXTS.text_na
        status = n.get('status', UI_TEXTS.text_na).replace('_', ' ')
        return f"#{nid} | {title} | Criada: {created} | Prazo: {prazo} | Status: {status}"

    active_statuses = {'classificada_aguardando_execucao', 'classificada', 'em_execucao'}
    pending = []
    executed = []

    for n in all_notifications:
        if not _assigned_to_me(n):
            continue

        # "Executadas" = você já concluiu sua parte (final_action_by_executor),
        # independente do status global (pode estar em revisão/aprovação).
        if _has_my_final_action(n):
            executed.append(n)
            continue

        # "Pendentes" = atribuídas a você e ainda em execução/aguardando execução.
        if n.get('status') in active_statuses:
            pending.append(n)

    tab_pend, tab_exec = st.tabs([f"⏳ Pendentes de Execução ({len(pending)})", f"✅ Executadas ({len(executed)})"])

    # -------------------------
    # GUIA: PENDENTES
    # -------------------------
    with tab_pend:
        if not pending:
            st.success("✅ Nenhuma notificação pendente de execução para você no momento.")
        else:
            st.info(f"📋 Você tem **{len(pending)}** notificação(ões) pendente(s). Clique para abrir e executar.")
            # Ordenação simples: prazo mais próximo primeiro
            def _sort_key(n):
                c = _parse_classif(n)
                d = c.get('deadline')
                try:
                    return datetime.fromisoformat(d) if d else datetime.max
                except Exception:
                    return datetime.max
            pending.sort(key=_sort_key)

            for n in pending:
                notif_id = n.get('id')
                with st.expander(_header(n), expanded=False):
                    # Detalhes completos
                    try:
                        display_notification_full_details(n, user_id, user_username)
                    except Exception as e:
                        st.warning(f"Não foi possível renderizar detalhes completos: {e}")

                    st.markdown("---")

                    # ✅ Encaminhar / adicionar outro executor (se necessário)
                    with st.expander("👥 Encaminhar / Adicionar outro executor", expanded=False):
                        exec_users = get_users_by_role('executor') or []
                        opts = []
                        label_to_id = {}
                        for u in exec_users:
                            lab = f"{u.get('name', UI_TEXTS.text_na)} ({u.get('username', UI_TEXTS.text_na)})"
                            label_to_id[lab] = u.get('id')
                            opts.append(lab)

                        if not opts:
                            st.info("Nenhum usuário com perfil 'executor' encontrado.")
                        else:
                            selected = st.multiselect(
                                "Selecione executores para adicionar",
                                options=opts,
                                key=f"add_exec_ms_{notif_id}"
                            )
                            obs = st.text_input("Observação (opcional)", key=f"add_exec_obs_{notif_id}")
                            if st.button("➕ Adicionar executores", key=f"btn_add_exec_{notif_id}", use_container_width=True):
                                ids_to_add = [label_to_id[x] for x in selected if label_to_id.get(x)]
                                if not ids_to_add:
                                    st.warning("Selecione pelo menos um executor.")
                                else:
                                    cur = n.get('executors', []) or []
                                    cur_ids = set()
                                    for e in cur:
                                        if isinstance(e, int): cur_ids.add(e)
                                        elif isinstance(e, dict) and e.get('id') is not None: cur_ids.add(e.get('id'))
                                    new_ids = sorted(cur_ids.union(set(ids_to_add)))

                                    updated = update_notification(notif_id, {"executors": new_ids})
                                    if updated:
                                        add_history_entry(
                                            notif_id,
                                            f"👥 Executor(es) adicionados: {', '.join(map(str, ids_to_add))}" + (f" | Obs: {obs}" if obs else ""),
                                            st.session_state.get('user_username', UI_TEXTS.text_na)
                                        )
                                        st.success("✅ Executor(es) adicionados com sucesso!")
                                        st.rerun()
                                    else:
                                        st.error("❌ Não foi possível atualizar a notificação.")

                    st.markdown("---")

                    # Fluxo de execução (ação / conclusão)
                    st.markdown("## 🛠️ Registrar Execução")

                    with st.form(key=f"exec_form_{notif_id}"):
                        action_desc = st.text_area(
                            "📝 Descrição da ação executada *",
                            key=f"exec_action_desc_{notif_id}",
                            height=140,
                            placeholder="Descreva o que foi feito, responsáveis, evidências, etc."
                        )

                        st.markdown("### 📎 Anexar documentos da execução (opcional)")
                        up_files = st.file_uploader(
                            "Arquivos (PDF, imagens, etc.)",
                            accept_multiple_files=True,
                            key=f"exec_attach_{notif_id}"
                        )

                        conclui = st.checkbox("🏁 Concluir minha parte nesta notificação", key=f"exec_finish_{notif_id}")

                        st.markdown("<span class='required-field'>* Campo obrigatório</span>", unsafe_allow_html=True)
                        submitted = st.form_submit_button("💾 Salvar", use_container_width=True, type="primary")

                        if submitted:
                            if not (action_desc or "").strip():
                                st.error("❌ Descreva a ação executada.")
                                st.stop()

                            saved_attachments = []
                            for f in (up_files or []):
                                saved_info = save_uploaded_file_to_disk(f, notif_id)
                                if not saved_info:
                                    continue

                                saved_attachments.append(saved_info)

                                # também registra na tabela de anexos da notificação (para o classificador ver nos detalhes)
                                # Evita duplicação (mesmo nome original) caso o Streamlit reexecute o bloco.
                                try:
                                    conn_att = get_db_connection()
                                    cur_att = conn_att.cursor()
                                    orig_name = saved_info.get('original_name')
                                    cur_att.execute(
                                        "SELECT 1 FROM notification_attachments WHERE notification_id=%s AND original_name=%s LIMIT 1",
                                        (int(notif_id), orig_name)
                                    )
                                    exists = cur_att.fetchone()
                                    if not exists:
                                        cur_att.execute(
                                            "INSERT INTO notification_attachments (notification_id, unique_name, original_name) VALUES (%s, %s, %s)",
                                            (int(notif_id), saved_info.get('unique_name'), orig_name)
                                        )
                                        conn_att.commit()
                                    else:
                                        # já existe um anexo com o mesmo nome original para esta notificação
                                        conn_att.rollback()
                                except Exception as e:
                                    st.error(f"❌ Falha ao registrar anexo no banco: {e}")
                                    try:
                                        if 'conn_att' in locals():
                                            conn_att.rollback()
                                    except Exception:
                                        pass
                                finally:
                                    try:
                                        if 'cur_att' in locals() and cur_att:
                                            cur_att.close()
                                    except Exception:
                                        pass
                                    try:
                                        if 'conn_att' in locals() and conn_att:
                                            conn_att.close()
                                    except Exception:
                                        pass

                            # registra ação
                            action_entry = {
                                "executor_id": user_id,
                                "executor_name": st.session_state.get('user', {}).get('name', user_username),
                                "executor_username": user_username,
                                "description": action_desc.strip(),
                                "timestamp": datetime.now().isoformat(),
                                "final_action_by_executor": bool(conclui),
                                "evidence_attachments": saved_attachments,
                                "attachments": saved_attachments
                            }

                            # Persistir ação na tabela (fonte de verdade)
                            ok_action = add_notification_action(int(notif_id), action_entry)

                            updates = {}
                            # Se concluiu, empurra para revisão de execução
                            if conclui:
                                updates["status"] = "revisao_classificador_execucao"

                            updated = True
                            if updates:
                                updated = update_notification(notif_id, updates)

                            if ok_action and updated:
                                add_history_entry(
                                    notif_id,
                                    "🏁 Executor concluiu sua parte" if conclui else "📝 Ação registrada pelo executor",
                                    st.session_state.get('user_username', UI_TEXTS.text_na)
                                )
                                st.success("✅ Salvo com sucesso!")
                                st.rerun()
                            else:
                                st.error("❌ Erro ao salvar. Verifique logs do servidor.")

    # -------------------------
    # GUIA: EXECUTADAS
    # -------------------------
    with tab_exec:
        if not executed:
            st.success("✅ Você ainda não concluiu execuções.")
        else:
            st.info(f"📋 Você tem **{len(executed)}** notificação(ões) onde sua parte já foi concluída.")
            # Mais recentes primeiro (pela última ação sua)
            def _last_my_ts(n):
                ts = None
                try:
                    actions = get_notification_actions(int(n.get('id', 0))) or []
                except Exception:
                    actions = []
                for a in actions:
                    if a.get('executor_id') == user_id:
                        t = a.get('timestamp')
                        if t and (ts is None or t > ts):
                            ts = t
                return ts or ""
            executed.sort(key=_last_my_ts, reverse=True)

            for n in executed:
                notif_id = n.get('id')
                with st.expander(_header(n), expanded=False):
                    # Detalhes completos da notificação, sem histórico (vamos mostrar só "minhas ações")
                    try:
                        n_copy = dict(n)
                        n_copy['actions'] = []
                        display_notification_full_details(n_copy, user_id, user_username)
                    except Exception as e:
                        st.warning(f"Não foi possível renderizar detalhes completos: {e}")

                    st.markdown("---")
                    st.markdown("## ✅ Minhas ações nesta notificação")

                    my_actions = [a for a in (get_notification_actions(int(n.get('id',0))) or []) if a.get('executor_id') == user_id]
                    if not my_actions:
                        st.info("Nenhuma ação sua registrada (inconsistência).")
                    else:
                        my_actions = sorted(my_actions, key=lambda x: x.get('timestamp', ''))
                        for idx, a in enumerate(my_actions):
                            ts = a.get('timestamp', UI_TEXTS.text_na)
                            try:
                                ts_fmt = datetime.fromisoformat(ts).strftime('%d/%m/%Y %H:%M:%S')
                            except Exception:
                                ts_fmt = ts
                            tipo = "🏁 CONCLUSÃO" if a.get('final_action_by_executor') else "📝 AÇÃO"
                            st.markdown(f"**{tipo}** — {ts_fmt}")
                            st.write(a.get('description', UI_TEXTS.text_na))

                            # anexos da ação
                            atts = a.get('attachments') or []
                            if atts:
                                st.markdown("**📎 Anexos desta ação:**")
                                for att_idx, anexo in enumerate(atts):
                                    unique_name = (anexo or {}).get('unique_name')
                                    original_name = (anexo or {}).get('original_name')
                                    if unique_name and original_name:
                                        content = get_attachment_data(unique_name)
                                        if content is not None:
                                            st.download_button(
                                                f"⬇️ {original_name}",
                                                content,
                                                file_name=original_name,
                                                mime="application/octet-stream",
                                                key=f"dl_my_exec_{notif_id}_{idx}_{att_idx}_{unique_name}"
                                            )
                                        else:
                                            st.write(f"{original_name} (arquivo não encontrado)")
                            st.markdown("---")

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
    pending_approval = [n for n in all_notifications if n.get('status') == 'aguardando_aprovacao' and safe_int(n.get('approver')) == safe_int(user_id_logged_in)]
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
                                            if file_content is not None:
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
                groups = split_attachments_by_origin(int(notification.get('id'))) if notification.get('id') is not None else {"notification": [], "execution": []}
                notif_atts = groups.get("notification", []) or []
                exec_atts = groups.get("execution", []) or []
                if notif_atts or exec_atts:
                    st.markdown("---")
                    st.markdown("#### 📎 Anexos")
                    if notif_atts:
                        render_attachments_download("📄 Anexos da Notificação", notif_atts, key_prefix=f"dl_notif_{notification.get('id')}_appr")
                    if exec_atts:
                        render_attachments_download("🛠️ Anexos da Execução", exec_atts, key_prefix=f"dl_exec_{notification.get('id')}_appr")
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










