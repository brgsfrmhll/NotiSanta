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
                # Mapeia nomes de exibição para IDs de usuário para executores
                executor_name_to_id_map_approval = {
                    f"{u.get('name', UI_TEXTS.text_na)} ({u.get('username', UI_TEXTS.text_na)})": u['id']
                    for u in users_exec
                }
                # Pega os nomes de exibição dos executores atribuídos
                executor_names_approval = [
                    name for name, uid in executor_name_to_id_map_approval.items()
                    if uid in notification.get('executors', [])
                ]
                st.markdown(f"**👥 Executores Atribuídos:** {', '.join(executor_names_approval) or 'Nenhum'}")
                review_exec_info = notification.get('review_execution', {})
                if review_exec_info:
                    st.markdown("---")
                    st.markdown("#### 🛠️ Resultado da Revisão do Classificador")
                    review_decision_display = review_exec_info.get('decision', UI_TEXTS.text_na)
                    reviewed_by_display = review_exec_info.get('reviewed_by', UI_TEXTS.text_na)
                    review_timestamp_str = review_exec_info.get('timestamp', UI_TEXTS.text_na)
                    if review_timestamp_str != UI_TEXTS.text_na:
                        try:
                            review_timestamp_str = datetime.fromisoformat(review_timestamp_str).strftime(
                                '%d/%m/%Y %H:%M:%S')
                        except ValueError:
                            pass
                        st.write(f"**Decisão da Revisão:** {review_decision_display}")
                    st.write(f"**Revisado por (Classificador):** {reviewed_by_display} em {review_timestamp_str}")
                    if review_decision_display == 'Rejeitada' and review_exec_info.get('rejection_reason'):
                        st.write(
                            f"**Motivo da Rejeição:** {review_exec_info.get('rejection_reason', UI_TEXTS.text_na)}")
                    if review_exec_info.get('notes'):
                        st.write(
                            f"**Observações do Classificador:** {review_exec_info.get('notes', UI_TEXTS.text_na)}")
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
                # NOVO: Inicializa ou recupera o estado do formulário de aprovação para esta notificação específica
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
                    # Capture o valor do text_area e atribua-o ao `current_approval_data['notes']`
                    approval_notes_input = st.text_area(
                        "Observações da Aprovação/Reprovação:*",
                        value=current_approval_data.get('notes', ''),
                        placeholder="• Avalie a completude e eficácia das ações executadas e a revisão do classificador...\\n• Indique se as ações foram satisfatórias para mitigar o risco ou resolver o evento.\\n• Forneça recomendações adicionais, se necessário.\\n• Em caso de reprovação, explique claramente o motivo e o que precisa ser revisado ou corrigido pelo classificador.",
                        height=120, key=f"approval_notes_{notification.get('id', UI_TEXTS.text_na)}_refactored",
                        help="Forneça sua avaliação sobre as ações executadas, a revisão do classificador, e a decisão final.").strip()
                    current_approval_data['notes'] = approval_notes_input  # Atualiza o estado com o valor do text_area
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
                                        'notes': approval_notes or "Notificação aprovada superiormente.",
                                        'timestamp': datetime.now().isoformat(),
                                        'status_final': 'aprovada'
                                    },
                                    'approver': None
                                }
                                update_notification(notification['id'], updates)  # Atualiza no DB
                                add_history_entry(notification['id'], "Notificação aprovada e finalizada",
                                                  user_name,
                                                  f"Aprovada superiormente." + (
                                                      f" Obs: {approval_notes[:150]}..." if approval_notes and len(
                                                          approval_notes) > 150 else (
                                                          f" Obs: {approval_notes}" if approval_notes else "")))
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
                                update_notification(notification['id'], updates)  # Atualiza no DB
                                add_history_entry(notification['id'], "Notificação reprovada (Aprovação)",
                                                  user_name,
                                                  f"Reprovada superiormente. Motivo: {approval_notes[:150]}..." if len(
                                                      approval_notes) > 150 else f"Reprovada superiormente. Motivo: {approval_notes}")
                                st.warning(
                                    f"⚠️ Notificação #{notification['id']} reprovada! Devolvida para revisão pelo classificador.")
                                st.info(
                                    "A notificação foi movida para o status 'aguardando classificador' para que a equipe de classificação possa revisar e redefinir o fluxo.")
                            # For both approve and reject paths, perform cleanup and rerun
                            # (update_notification is already called inside if/elif blocks)
                            st.session_state.approval_form_state.pop(notification['id'], None)
                            _clear_approval_form_state(notification['id'])
                            st.rerun() # CORREÇÃO: Força o re-render
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
                    # Determinar o status do prazo para notificações encerradas
                    classif_info = notification.get('classification', {})
                    deadline_date_str = classif_info.get('deadline_date')
                    # Acessa 'timestamp' de 'conclusion' de forma segura
                    concluded_timestamp_str = (notification.get('conclusion') or {}).get(
                        'timestamp')
                    # Verificar se a conclusão foi dentro ou fora do prazo
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
                                            <p><strong>Encerrada por:</strong> {concluded_by} | <strong>Data de Criação:</strong> {created_at_str}</p>
                                            <p><strong>Prazo:</strong> {deadline_status['text']}</p>
                                        </div>
                                        """, unsafe_allow_html=True)
                    with st.expander(
                            f"👁️ Visualizar Detalhes - Notificação #{notification.get('id', UI_TEXTS.text_na)}"):
                        display_notification_full_details(notification,
                                                          st.session_state.user.get(
                                                              'id') if st.session_state.authenticated else None,
                                                          st.session_state.user.get(
                                                              'username') if st.session_state.authenticated else None)

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
            # Filtra o próprio usuário logado da lista para edição
            users_to_display = [u for u in users if u['id'] != st.session_state.editing_user_id]
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
                        # Regras para botões de ação:
                        # 1. Admin inicial (ID 1) não pode ser editado/desativado por segurança
                        # 2. O usuário logado não pode editar/desativar a própria conta
                        if user.get('id') != 1 and user.get('id') != st.session_state.user.get(
                                'id'):
                            if st.button("✏️ Editar",
                                         key=f"edit_user_{user.get('id', UI_TEXTS.text_na)}",
                                         use_container_width=True):
                                st.session_state.editing_user_id = user['id']
                                # Pre-popula os campos de edição
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
            # Formulário de edição flutuante para o usuário selecionado
            if st.session_state.editing_user_id:
                edited_user = next(
                    (u for u in users if u['id'] == st.session_state.editing_user_id), None)
                if edited_user:
                    st.markdown(
                        f"### ✏️ Editando Usuário: {edited_user.get('name', UI_TEXTS.text_na)} ({edited_user.get('username', UI_TEXTS.text_na)})")
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
                # Carrega todos os dados do banco para o backup
                all_users_for_backup = load_users()
                all_notifications_for_backup = load_notifications()
                # Garante que os dados do JSONB sejam dicionários e não strings JSON
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
                        # Para JSONB que já vem como dict/list
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
                                # Restauração: Apaga tudo e reinsere (simples, mas destrutivo)
                                conn = get_db_connection()
                                cur = conn.cursor()
                                try:
                                    # Desabilita triggers de TSVECTOR para restauração massiva
                                    cur.execute(
                                        "ALTER TABLE notifications DISABLE TRIGGER trg_notifications_search_vector;")
                                    # Limpa tabelas em ordem inversa de dependência
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
                                    # Restaura usuários
                                    for user_data in backup_data['users']:
                                        cur.execute("""
                                                    INSERT INTO users (id, username, password_hash, name, email, roles, active, created_at)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                                """, (
                                            user_data.get('id'),
                                            user_data.get('username'),
                                            user_data.get('password'),  # Já é o hash
                                            user_data.get('name'),
                                            user_data.get('email'),
                                            user_data.get('roles', []),
                                            user_data.get('active', True),
                                            datetime.fromisoformat(
                                                user_data['created_at']) if user_data.get(
                                                'created_at') else datetime.now()
                                        ))
                                    # Ajusta a sequência SERIAL para o próximo ID disponível
                                    cur.execute(
                                        f"SELECT setval('users_id_seq', (SELECT MAX(id) FROM users));")
                                    # Restaura notificações e sub-dados (attachments, actions, history)
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
                                        # Insere notificação principal
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
                                            # Já é boolean
                                            notif_data.get('immediate_action_description'),
                                            notif_data.get('patient_involved'),  # Já é boolean
                                            notif_data.get('patient_id'),
                                            notif_data.get('patient_outcome_obito'),
                                            # Já é boolean
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
                                    # Restaura histórico
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
                                    # Restaura ações
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
                                    # Ajusta a sequência SERIAL para o próximo ID disponível
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
                                    st.session_state.classification_active_notification_id = None # Limpa a seleção ativa
                                    st.session_state.approval_form_state = {}
                                    st.rerun()
                                except psycopg2.Error as e:
                                    conn.rollback()
                                    st.error(
                                        f"❌ Erro ao restaurar dados no banco de dados: {e}")
                                finally:
                                    # Habilita triggers de TSVECTOR novamente
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
                            # Para exibir JSON puro e bonito no Streamlit, garantimos que todas as datas/tempos e JSONB
                            # já estejam formatados como strings ISO e dicionários/listas Python, respectivamente.
                            # load_notifications já faz grande parte disso.
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
        st.write(f"**Versão do Portal:** 2.0.1")
        st.write(f"**Data da Última Atualização:** 19/07/2025")
        st.write(f"**Desenvolvido por:** FIA Softworks")
        st.markdown("#### Contato")
        st.markdown("##### Suporte Técnico:")
        st.write(f"**Email:** borges@fiasoftworks.com.br")
        

@st_fragment
def show_dashboard():
    # A função force_rerun_dashboard_search não é mais necessária aqui
    # def force_rerun_dashboard_search():
    #     st.rerun()
    if not check_permission('admin') and not check_permission('classificador'):
        st.error("❌ Acesso negado! Você não tem permissão para visualizar o dashboard.")
        return
    """
    Renderiza um dashboard abrangente para visualização de notificações,
    incluindo métricas chave, gráficos e uma lista detalhada, filtrável,
    pesquisável e paginada de notificações.
    """
    st.markdown("<h1 class='main-header'>   Dashboard de Notificações</h1>",
                unsafe_allow_html=True)

    all_notifications = load_notifications()  # Carrega do DB
    if not all_notifications:
        st.warning(
            "⚠️ Nenhuma notificação encontrada para exibir no dashboard. Comece registrando uma nova notificação.")
    # Converte a lista de notificações em um DataFrame pandas para facilitar a manipulação
    df_notifications = pd.DataFrame(all_notifications)
    df_notifications['created_at_dt'] = pd.to_datetime(df_notifications['created_at'])
    df_notifications['occurrence_date_dt'] = pd.to_datetime(df_notifications['occurrence_date'])

    # Define categorias de status para gráficos
    completed_statuses = ['aprovada', 'concluida']
    rejected_statuses = ['rejeitada', 'reprovada']

    # Aba para Visão Geral e Lista Detalhada (conteúdo existente)
    # Aba para Indicadores e Gráficos (novo conteúdo)
    tab_overview_list, tab_indicators = st.tabs(
        ["📊 Visão Geral e Lista", "📈 Indicadores e Gráficos"])

    with tab_overview_list:
        st.info("Visão geral e detalhada de todas as notificações registradas no sistema.")
        st.markdown("### Visão Geral e Métricas Chave")
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
            st.markdown(f"<div class='metric-card'><h4>Concluídas</h4><p>{completed}</p></div>",
                        unsafe_allow_html=True)
        with col_m5:
            st.markdown(f"<div class='metric-card'><h4>Rejeitadas</h4><p>{rejected}</p></div>",
                        unsafe_allow_html=True)

        st.markdown("---")

        st.markdown("### Gráficos de Tendência e Distribuição")
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            st.markdown("#### Distribuição de Notificações por Status")
            status_mapping = {
                'pendente_classificacao': 'Pendente Classif. Inicial',
                'classificada': 'Classificada (Aguardando Exec.)',
                'em_execucao': 'Em Execução',
                'revisao_classificador_execucao': 'Aguardando Revisão Exec.',
                'aguardando_classificador': 'Aguardando Classif. (Revisão)',
                'aguardando_aprovacao': 'Aguardando Aprovação',
                'aprovada': 'Concluída (Aprovada)',
                'rejeitada': 'Rejeitada (Classif. Inicial)',
                'reprovada': 'Reprovada (Aprovação)'
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
                st.info("Nenhum dado de status para gerar o gráfico.")
        with col_chart2:
            st.markdown("#### Notificações Criadas ao Longo do Tempo")
            if not df_notifications.empty:
                df_notifications_copy = df_notifications.copy()  # Cria uma cópia para evitar SettingWithCopyWarning
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
                st.info("Nenhum dado para gerar o gráfico de tendência.")
        st.markdown("---")

        st.markdown("### Lista Detalhada de Notificações")

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
            
        # Não é mais necessário inicializar dashboard_search_query aqui, pois será derivado de dashboard_search_query_input
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
                "Data Inicial (Criação):", value=date_start_default,
                key="dashboard_filter_date_start_input"
            )
            st.session_state.dashboard_filter_date_end = st.date_input(
                "Data Final (Criação):", value=date_end_default,
                key="dashboard_filter_date_date_end_input"
            )
        with col_filters3:
            # O st.text_input armazena seu valor diretamente em st.session_state.dashboard_search_query_input
            # O parâmetro 'value' serve para definir o valor inicial, que será o que está no session_state (ou vazio)
            st.text_input(
                "Buscar (Título, Descrição, ID):",
                value=st.session_state.dashboard_search_query_input, # Usa o valor que está no session_state para persistência
                key="dashboard_search_query_input", # A chave onde o valor atual do widget é armazenado
                # on_change=force_rerun_dashboard_search # Removido para evitar o problema de "voltar"
            )
            # A variável usada para a lógica de filtragem é atualizada APÓS o text_input ter seu valor persistido.
            # Essa linha será executada em cada rerun (seja por Enter, blur ou outro widget).
            st.session_state.dashboard_search_query = st.session_state.dashboard_search_query_input.lower()
            sort_options_map = {
                'ID': 'id',
                'Data de Criação': 'created_at',
                'Título': 'title',
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
            # Usa st.session_state.dashboard_search_query, que é sempre o valor atualizado e em minúsculas
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
                priority_order_val = {'Crítica': 4, 'Alta': 3, 'Média': 2, 'Baixa': 1,
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

        st.write(f"**Notificações Encontradas: {len(filtered_notifications)}**")

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
            "Página:", min_value=1, max_value=total_pages,
            value=st.session_state.dashboard_current_page,
            key="dashboard_current_page_input"
        )

        start_idx = (
                            st.session_state.dashboard_current_page - 1) * st.session_state.dashboard_items_per_page
        end_idx = start_idx + st.session_state.dashboard_items_per_page
        paginated_notifications = filtered_notifications[start_idx:end_idx]
        if not paginated_notifications:
            st.info("Nenhuma notificação encontrada com os filtros e busca aplicados.")
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
                        f"👁️ Visualizar Detalhes - Notificação #{notification.get('id', UI_TEXTS.text_na)}"):
                    display_notification_full_details(notification,
                                                      st.session_state.user.get(
                                                          'id') if st.session_state.authenticated else None,
                                                      st.session_state.user.get(
                                                          'username') if st.session_state.authenticated else None)

    with tab_indicators:
        st.info("Explore os indicadores e tendências das notificações, com filtros de período.")
        st.markdown("### Seleção de Período para Indicadores")
        # Define as datas padrão para o filtro de período, usando a data mais antiga e mais recente
        min_date = df_notifications[
            'created_at_dt'].min().date() if not df_notifications.empty else dt_date_class.today() - timedelta(
            days=365)
        max_date = df_notifications[
            'created_at_dt'].max().date() if not df_notifications.empty else dt_date_class.today()
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            st.date_input("Data de Início", value=min_date,
                                                  key="start_date_indicators")
        with col_date2:
            st.date_input("Data de Fim", value=max_date,
                                                key="end_date_indicators")
        # Filtra o DataFrame pelo período selecionado
        df_filtered_by_period = df_notifications[
            (df_notifications['created_at_dt'].dt.date >= st.session_state.start_date_indicators) &
            (df_notifications[
                 'created_at_dt'].dt.date <= st.session_state.end_date_indicators)].copy()

        if df_filtered_by_period.empty:
            st.warning("⚠️ Não há dados para o período selecionado para gerar os indicadores.")
            return

        st.markdown("---")

        st.markdown(
            "#### 📈 Quantidade de Notificações por Mês (Abertas, Concluídas, Rejeitadas)")

        df_monthly = df_filtered_by_period.copy()
        df_monthly['month_year'] = df_monthly['created_at_dt'].dt.to_period('M').astype(str)
        # Categoriza o status da notificação
        df_monthly['status_category'] = 'Aberta'
        df_monthly.loc[
            df_monthly['status'].isin(completed_statuses), 'status_category'] = 'Concluída'
        df_monthly.loc[
            df_monthly['status'].isin(rejected_statuses), 'status_category'] = 'Rejeitada'

        monthly_counts = df_monthly.groupby(['month_year', 'status_category']).size().unstack(
            fill_value=0)

        # Garante que todos os meses no período estejam presentes, mesmo que sem dados
        all_months_in_range = pd.period_range(start=st.session_state.start_date_indicators,
                                              end=st.session_state.end_date_indicators, freq='M').astype(
            str)
        monthly_counts = monthly_counts.reindex(all_months_in_range, fill_value=0)
        if not monthly_counts.empty:
            st.line_chart(monthly_counts)
        else:
            st.info("Nenhuma notificação encontrada no período para este gráfico.")
        st.markdown("---")

        st.markdown("####    Pendência de Análises por Mês")
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
            all_months_in_range_pending = pd.period_range(start=st.session_state.start_date_indicators,
                                                          end=st.session_state.end_date_indicators,
                                                          freq='M').astype(str)
            monthly_pending_counts = monthly_pending_counts.set_index('month_year').reindex(
                all_months_in_range_pending,
                fill_value=0).reset_index()
            monthly_pending_counts.columns = ['month_year', 'Quantidade']

            st.bar_chart(monthly_pending_counts.set_index('month_year'))
        else:
            st.info("Nenhuma pendência de análise encontrada no período e filtro selecionados.")

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
                    st.info("Nenhum dado de setor notificado para o período.")
            else:
                st.info("Nenhum dado de setor notificado para o período.")
        with col_top2:
            st.markdown("##### Top 10 Setores Notificantes")
            if not df_filtered_by_period.empty:
                top_reporting = df_filtered_by_period[
                    'reporting_department'].value_counts().nlargest(10)
                if not top_reporting.empty:
                    st.bar_chart(top_reporting)
                else:
                    st.info("Nenhum dado de setor notificante para o período.")
            else:
                st.info("Nenhum dado de setor notificante para o período.")

        st.markdown("---")

        st.markdown("#### 📊 Classificação das Notificações (NNC e Tipo Principal)")

        # DataFrame para notificações concluídas no período
        df_completed_period = df_filtered_by_period[
            df_filtered_by_period['status'].isin(completed_statuses)].copy()
        # DataFrame para notificações abertas no período (não concluídas e não rejeitadas)
        df_open_period = df_filtered_by_period[
            ~df_filtered_by_period['status'].isin(
                completed_statuses + rejected_statuses)].copy()

        col_classif1, col_classif2 = st.columns(2)

        with col_classif1:
            st.markdown("##### NNC - Concluídas")
            if not df_completed_period.empty:
                # Usa .apply para acessar o dicionário 'classification' e pegar 'nnc'
                completed_nnc = df_completed_period['classification'].apply(
                    lambda x: x.get('nnc') if x else None).value_counts().dropna()
                if not completed_nnc.empty:
                    st.bar_chart(completed_nnc)
                else:
                    st.info(
                        "Nenhuma classificação NNC para notificações concluídas no período.")
            else:
                st.info("Nenhuma notificação concluída no período.")
        with col_classif2:
            st.markdown("##### NNC - Abertas")
            if not df_open_period.empty:
                open_nnc = df_open_period['classification'].apply(
                    lambda x: x.get('nnc') if x else None).value_counts().dropna()
                if not open_nnc.empty:
                    st.bar_chart(open_nnc)
                else:
                    st.info("Nenhuma classificação NNC para notificações abertas no período.")
            else:
                st.info("Nenhuma notificação aberta no período.")
        col_classif3, col_classif4 = st.columns(2)
        with col_classif3:
            st.markdown("##### Tipo Principal - Concluídas")
            if not df_completed_period.empty:
                completed_main_type = df_completed_period['classification'].apply(
                    lambda x: x.get('event_type_main') if x else None).value_counts().dropna()
                if not completed_main_type.empty:
                    st.bar_chart(completed_main_type)
                else:
                    st.info("Nenhum tipo principal para notificações concluídas no período.")
            else:
                st.info("Nenhuma notificação concluída no período.")
        with col_classif4:
            st.markdown("##### Tipo Principal - Abertas")
            if not df_open_period.empty:
                open_main_type = df_open_period['classification'].apply(
                    lambda x: x.get('event_type_main') if x else None).value_counts().dropna()
                if not open_main_type.empty:
                    st.bar_chart(open_main_type)
                else:
                    st.info("Nenhuma tipo principal para notificações abertas no período.")
            else:
                st.info("Nenhuma notificação aberta no período.")


def main():
    """Main function to run the Streamlit application."""
    init_database()  # Garante que o DB e tabelas estão inicializadas

    if 'authenticated' not in st.session_state: st.session_state.authenticated = False
    if 'user' not in st.session_state: st.session_state.user = None
    if 'page' not in st.session_state: st.session_state.page = 'create_notification'
    if 'initial_classification_state' not in st.session_state: st.session_state.initial_classification_state = {}
    if 'review_classification_state' not in st.session_state: st.session_state.review_classification_state = {}
    # NOVO: Variável única para notificação ativa na tela de classificação/revisão
    if 'classification_active_notification_id' not in st.session_state: st.session_state.classification_active_notification_id = None
    # NOVO: Adiciona o estado para o formulário de aprovação
    if 'approval_form_state' not in st.session_state: st.session_state.approval_form_state = {}

    show_sidebar()

    restricted_pages = ['dashboard', 'classification', 'execution', 'approval', 'admin']
    if st.session_state.page in restricted_pages and not st.session_state.authenticated:
        st.warning("⚠️ Você precisa estar logado para acessar esta página.")
        st.session_state.page = 'create_notification'
        st.rerun()  # Permanece, pois é navegação global
    if st.session_state.page == 'create_notification':
        show_create_notification()  # Chama a versão fragmentada
    elif st.session_state.page == 'dashboard':
        show_dashboard()  # Chama a versão fragmentada
    elif st.session_state.page == 'classification':
        show_classification()  # Chama a versão fragmentada
    elif st.session_state.page == 'execution':
        show_execution()  # Chama a versão fragmentada
    elif st.session_state.page == 'approval':
        show_approval()  # Chama a versão fragmentada
    elif st.session_state.page == 'admin':
        show_admin()  # Chama a versão fragmentada
    else:
        st.error("Página solicitada inválida. Redirecionando para a página inicial.")
        st.session_state.page = 'create_notification'
        st.rerun()  # Permanece, pois é navegação global


if __name__ == "__main__":
    main()
