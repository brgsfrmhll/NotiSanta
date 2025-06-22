# form_configs.py

FORM_DATA = {
    "turnos": ["Manhã", "Tarde", "Noite", "Integral", "Outro"],
    "classificacao_nnc": [
        "Não conformidade", "Circunstância de Risco", "Near Miss",
        "Evento sem dano", "Evento com dano"
    ],
    "niveis_dano": ["Dano leve", "Dano moderado", "Dano grave", "Óbito"],
    "never_events": [
        "Alta ou liberação de paciente de qualquer idade que seja incapaz de tomar decisões, para outra pessoa não autorizada.",
        "Contaminação na administração de O2 ou gases medicinais.",
        "Gás errado na administração de O2 ou gases medicinais",
        "Lesão por pressão estágio 3 (perda total da espessura da pele).",
        "Lesão por pressão estágio 4 (perda total da espessura da pele e perda tissular).",
        "Lesão por pressão não classificável (perda da pele em sua espessura total e perda tissular).",
        "Óbito intra-operatório ou imediatamente pós-operatório/pós-procedimento em paciente ASA Classe 1.",
        "Óbito ou lesão grave de paciente associado a choque elétrico durante a assistência dentro do serviço de saúde.",
        "Óbito ou lesão grave de paciente associado à fuga do paciente.",
        "Óbito ou lesão grave de paciente associado à queimadura decorrente de qualquer fonte durante a assistência dentro do serviço de saúde",
        "Óbito ou lesão grave de paciente associado ao uso de contenção física ou grades da cama durante a assistência dentro do serviço de saúde.",
        "Óbito ou lesão grave de paciente ou colaborador associado à introdução de objeto metálico em área de Ressonância Magnética",
        "Óbito ou lesão grave resultante de falha no acompanhamento ou na comunicação dos resultados de exames laboratoriais ou de patologia clínica.",
        "Óbito ou lesão grave de paciente resultante de falha no acompanhamento ou na comunicação dos resultados de exames radiológicos/de radiodiagnóstico.",
        "Óbito ou lesão grave de paciente resultante de perda irrecuperável de amostra biológica insubstituível.",
        "Óbito ou lesão grave de recém-nascido associado ao trabalho de parto, ou ao parto, em gestação de baixo risco.",
        "Óbito ou lesão grave materna associado ao trabalho de parto ou parto em gestação de baixo risco.",
        "Procedimento cirúrgico realizado em local errado.",
        "Procedimento cirúrgico realizado no lado errado do corpo.",
        "Procedimento cirúrgico realizado no paciente errado.",
        "Realização de cirurgia errada em um paciente.",
        "Retenção não intencional de corpo estranho em um paciente após a cirurgia.",
        "Suicídio de paciente, tentativa de suicídio ou dano autoinfligido que resulte em lesão grave durante a assistência dentro do serviço de saúde.",
        "Lesão grave associado à caída do paciente durante prestação de cuidados/atendimento.",
        "Óbito associado à caída do paciente durante prestação de cuidados/atendimento."
    ],
    "tipos_evento_principal": {
        "Clínico": [
            "Infecção associada aos cuidados de saúde", "Infusão Antineoplásica",
            "META 1 - IDENTIFICAÇÃO DO PACIENTE", "META 2 - COMUNICAÇÃO EFETIVA",
            "META 3 - MEDICAMENTO SEGURO", "META 4 - CIRURGIA / PARTO SEGURO",
            "META 5 - HIGIENE DAS MÃOS", "META 6 - CAÍDA E LESÃO DE PELE",
            "Sangue e Derivados", "Dispositivos Invasivos", "Carro de Urgência",
            "Intercorrências graves no atendimento aos processos seguros", "Nutrição",
            "Protocolos gerenciados", "Quebra de SLA (Clínico)",
            "Parto e nascimento", "Crise Convulsiva"
        ],
        "Não-clínico": [
            "Segurança Patrimonial", "Hotelaria", "Infraestrutura (Não-clínico)",
            "Equipamentos médicos hospitalares (Não-clínico)", "Quebra de SLA (Não-clínico)",
            "Falha no preenchimento ou completude do prontuário", "Gestão de Acesso"
        ],
        "Ocupacional": [
            "Acidente com material químico", "Acidente com material biológico",
            "Acidente com material perfurocortante", "Acidente com material (Geral)",
            "Ausência de EPIs a ser disponibilizado pelo colaborador",
            "Não utilização de EPIs pelo colaborador",
            "Ausência de insumos para o descarte inadequado de resíduos",
            "Descarte inadequado de perfurocortante", "Descarte inadequado de resíduo infectante",
            "Descarte inadequado de resíduo comum", "Descarte inadequado de resíduo químico",
            "Derramamento de quimioterápico", "Derramamento de inflamáveis",
            "Derramamento de radioisótopos", "Adornos (Uso de)", "Calçados Abertos (Uso de)"
        ],
        "Queixa técnica": [],
        "Outros": []
    },
    "classificacao_oms": [
        "Administração clínica", "Processo/Procedimento clínico", "Documentação",
        "Infecções associadas ao cuidado à saúde", "Medicamentos ou Fluidos",
        "Produtos de sangue/sangue", "Nutrição", "Oxigênio/Gás/Vapor",
        "Aparelhos/Equipamentos médicos", "Comportamento (Pessoal/Paciente)",
        "Acidentes do paciente (Queda, etc.)",
        "Infraestrutura/Construção/Instalação",
        "Gerenciamento de Recursos/Organização"
    ],
    "prioridades": ["Baixa", "Média", "Alta", "Crítica"]
}