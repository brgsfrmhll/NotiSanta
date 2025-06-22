# ui_texts.py

UI_TEXTS = {
    # Textos genéricos
    "text_na": "N/A", # Usado para "Não Aplicável" ou "Não Informado"

    # Default texts para Selectboxes (Formulário de Notificação)
    "selectbox_default_event_shift": "Selecionar Turno",
    "selectbox_default_immediate_actions_taken": "Selecionar",
    "selectbox_default_patient_involved": "Selecionar",
    "selectbox_default_initial_event_type": "Selecionar Tipo",
    "selectbox_default_initial_severity": "Selecionar Gravidade",
    "selectbox_default_patient_outcome_obito": "Selecionar",

    # Default texts para Selectboxes (Classificação)
    "selectbox_default_notification_select": "--- Selecionar Notificação ---",
    "selectbox_default_procede_classification": "Selecionar",
    "selectbox_default_classificacao_nnc": "Selecionar Classificação",
    "selectbox_default_nivel_dano": "Selecionar Nível de Dano",
    "selectbox_default_prioridade_resolucao": "Selecionar Prioridade",
    "selectbox_default_never_event": "Selecionar Never Event",
    "selectbox_default_evento_sentinela": "Selecionar", # Sim/Não
    "selectbox_default_tipo_principal": "Selecionar Tipo Principal",
    "selectbox_default_requires_approval": "Selecionar", # Sim/Não
    "selectbox_default_approver": "Selecionar Aprovador",
    "selectbox_default_decisao_revisao": "Selecionar Decisão",
    "selectbox_default_acao_realizar": "Selecionar Ação",
    "selectbox_default_executor_add": "--- Selecionar Executor ---",

    # Default texts para Selectboxes (Aprovação)
    "selectbox_default_decisao_aprovacao": "Selecionar Decisão",

    # Default texts para Selectboxes (Dashboard)
    "selectbox_sort_by_placeholder": "Ordenar por:", # Placeholder para ordenar
    "selectbox_sort_by_label": "Ordenar por:", # Rótulo para ordenar
    "selectbox_items_per_page_placeholder": "Itens por página:", # Placeholder para itens por página
    "selectbox_items_per_page_label": "Itens por página:", # Rótulo para itens por página

    # Dashboard & Admin Multiselect Labels/Placeholders (Refined and Centralized)
    "multiselect_filter_status_label": "Filtrar por Status:",
    "multiselect_filter_nnc_label": "Filtrar por Classificação NNC:",
    "multiselect_filter_priority_label": "Filtrar por Prioridade:",
    "multiselect_assign_executors_label": "Atribuir Executores Responsáveis:*",
    "multiselect_classification_oms_label": "Classificação OMS (tipos de incidente):*",
    "multiselect_user_roles_label": "Funções*", # Label para multiselect de funções em Admin
    "multiselect_event_spec_label_prefix": "Especificação de ", # Prefixo para labels dinâmicas
    "multiselect_event_spec_label_suffix": " (pode selecionar mais de um):", # Sufixo para labels dinâmicas

    # Admin Debug Selectbox Default
    "selectbox_default_admin_debug_notif": "--- Selecionar Notificação para Debug ---",

    # NOVAS CHAVES PARA TEXTOS INSTRUTIVOS/DEFAULT DE MULTISELECT
    "multiselect_instruction_placeholder": "Selecione uma ou mais opções...",
    "multiselect_all_option": "Todos",
    "selectbox_never_event_na_text": "Selecione uma opção...", # NOVA LINHA AQUI
}