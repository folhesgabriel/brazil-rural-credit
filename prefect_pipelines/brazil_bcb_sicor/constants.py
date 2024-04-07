from enum import Enum


class Constants(Enum):
    """
    Constants for brazil_bcb_sicor.
    """
    
    INPUT_FOLDER = 'tmp/raw_data'
    OUTPUT_FOLDER = 'tmp/preprocessed_data'
    
    
    URLS = {
        'microdados_operacao' : [f'https://www.bcb.gov.br/htms/sicor/DadosBrutos/SICOR_OPERACAO_BASICA_ESTADO_{year}.gz' for year in range(2013, 2015)],
        'recurso_publico_complemento_operacao' : ['https://www.bcb.gov.br/htms/sicor/DadosBrutos/SICOR_COMPLEMENTO_OPERACAO_BASICA.gz'],
        }
    
    
    DTYPES = {
        'microdados_operacao': {
            'DT_EMISSAO': str,
            'DT_VENCIMENTO': str,
            'CD_ESTADO': str,
            '#REF_BACEN': str,
            'REFBACEN': str,
            'NU_ORDEM': str,
            'CD_CATEG_EMITENTE': str,
            'CD_EMPREENDIMENTO': str,
            'CD_FASE_CICLO_PRODUCAO': str,
            'CD_FONTE_RECURSO': str,
            'CD_INST_CREDITO': str,
            'CD_PROGRAMA': str,
            'CD_REF_BACEN_INVESTIMENTO': str,
            'CD_SUBPROGRAMA': str,
            'CD_TIPO_AGRICULTURA': str,
            'CD_TIPO_CULTIVO': str,
            'CD_TIPO_ENCARG_FINANC': str,
            'CD_TIPO_GRAO_SEMENTE': str,
            'CD_TIPO_INTGR_CONSOR': str,
            'CD_TIPO_IRRIGACAO': str,
            'CD_TIPO_SEGURO': str,
            'CD_TIPO_SOLO' : str,
            'CD_CICLO_CULTIVAR' :  str,
            'CNPJ_AGENTE_INVEST': str,
            'CNPJ_IF': str,
            'CD_CONTRATO_STN': str,
            'CD_CNPJ_CADASTRANTE': str,
            'VL_AREA_INFORMADA' : float,
            'DT_FIM_COLHEITA': str,
            'DT_FIM_PLANTIO': str,
            'DT_INIC_COLHEITA': str,
            'DT_INIC_PLANTIO': str,
            'VL_AREA_FINANC': float,
            'VL_AREA_INFORMADA':float,
            'VL_ALIQ_PROAGRO': float,
            'VL_PARC_CREDITO': float,
            'VL_PRESTACAO_INVESTIMENTO': float,
            'VL_REC_PROPRIO': float,
            'VL_RECEITA_BRUTA_ESPERADA': float,
            'VL_REC_PROPRIO_SRV': float,
            'VL_QUANTIDADE': float,
            'VL_PRODUTIV_OBTIDA': float,
            'VL_PREV_PROD': float,
            'VL_JUROS': float,
            'VL_JUROS_ENC_FINAN_POSFIX': float,
            'VL_PERC_CUSTO_EFET_TOTAL': float,
            'VL_PERC_RISCO_FUNDO_CONST': float,
            'VL_PERC_RISCO_STN': float,
        },
        
        'recurso_publico_complemento': {
            'NU_ORDEM': str,
            '#REFBACEN': str,
            'REF_BACEN_EFETIVO' : str,
            'AGENCIA_IF' : str,
            'CD_IBGE_MUNICIPIO' : str
        }
    }
    
    COL_NAMES = {
        'microdados_operacao': {
            
            'NU_ORDEM': 'numero_ordem',
            '#REF_BACEN': 'id_referencia_bacen',
            'CD_ESTADO': 'sigla_uf',
            'CD_CATEG_EMITENTE': 'id_categoria_emitente',
            'CD_EMPREENDIMENTO': 'id_empreendimento',
            'CD_FASE_CICLO_PRODUCAO': 'id_fase_ciclo_producao',
            'CD_FONTE_RECURSO': 'id_fonte_recurso',
            'CD_INST_CREDITO': 'id_instrumento_credito',
            'CD_PROGRAMA': 'id_programa',
            'CD_REF_BACEN_INVESTIMENTO': 'id_referencia_bacen_investimento',
            'CD_SUBPROGRAMA': 'id_subprograma',
            'CD_TIPO_AGRICULTURA': 'id_tipo_agricultura',
            'CD_TIPO_CULTIVO': 'id_tipo_cultivo',
            'CD_TIPO_SOLO' : 'id_tipo_solo',
            'CD_CICLO_CULTIVAR' :  'id_ciclo_cultivar',
            'CD_TIPO_ENCARG_FINANC': 'id_tipo_encargo_financeiro',
            'CD_TIPO_GRAO_SEMENTE': 'id_tipo_grao_semente',
            'CD_TIPO_INTGR_CONSOR': 'id_tipo_integracao_consorcio',
            'CD_TIPO_IRRIGACAO': 'id_tipo_irrigacao',
            'CD_TIPO_SEGURO': 'id_tipo_seguro',
            'CNPJ_AGENTE_INVEST': 'cnpj_agente_investimento',
            'CNPJ_IF': 'cnpj_basico_instituicao_financeira',
            'DT_EMISSAO': 'data_emissao',
            'DT_VENCIMENTO': 'data_vencimento',
            'DT_FIM_COLHEITA': 'data_fim_colheita',
            'DT_FIM_PLANTIO': 'data_fim_plantio',
            'DT_INIC_COLHEITA': 'data_inicio_colheita',
            'DT_INIC_PLANTIO': 'data_inicio_plantio',
            'VL_AREA_FINANC': 'area_financiada',
            'VL_AREA_INFORMADA': 'area_informada',
            'VL_ALIQ_PROAGRO': 'valor_aliquota_proagro',
            'VL_PARC_CREDITO': 'valor_parcela_credito',
            'VL_PRESTACAO_INVESTIMENTO': 'valor_prestacao_investimento',
            'VL_REC_PROPRIO': 'valor_recurso_proprio',
            'VL_RECEITA_BRUTA_ESPERADA': 'valor_receita_bruta_esperada',
            'VL_REC_PROPRIO_SRV': 'valor_recurso_proprio_srv',
            'VL_QUANTIDADE': 'valor_quantidade_itens_financiados',
            'VL_PRODUTIV_OBTIDA': 'valor_produtividade_obtida',
            'VL_PREV_PROD': 'valor_previsao_producao',
            'VL_JUROS': 'taxa_juro',
            'VL_JUROS_ENC_FINAN_POSFIX': 'taxa_juro_encargo_financeiro_posfixado',
            'VL_PERC_RISCO_STN': 'valor_percentual_risco_stn',
            'VL_PERC_CUSTO_EFET_TOTAL': 'valor_percentual_custo_efetivo_total',
            'VL_PERC_RISCO_FUNDO_CONST': 'valor_percentual_risco_fundo_constitucional',
            'CD_CONTRATO_STN' : 'id_contrato_sistema_tesouro_nacional',
            'CD_CNPJ_CADASTRANTE' : 'cnpj_cadastrante',
        },
        
        'recurso_publico_complemento' : {
            
            'NU_ORDEM': 'numero_ordem',
            '#REFBACEN': 'id_referencia_bacen',
            'REF_BACEN_EFETIVO' : 'id_referencia_bacen_efetivo',
            'AGENCIA_IF' : 'id_agencia',
            'CD_IBGE_MUNICIPIO' : 'id_municipio',
        }
    }
    
    
   
    
    
    
    