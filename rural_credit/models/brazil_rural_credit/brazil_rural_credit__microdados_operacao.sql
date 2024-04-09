{{
    config(
        alias="microdados_operacao",
        schema="brazil_rural_credit",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2024, "interval": 1},
        },
        cluster_by=["sigla_uf", "plano_safra_emissao"],
    )
}}
-- settar configs e foramtar datas se houver necessidade
with staging_data as (
select
    EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', data_emissao)) AS ano_emissao,
    EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', data_emissao)) AS mes_emissao,
    PARSE_DATE('%m/%d/%Y', data_emissao) AS data_emissao,
    EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', data_vencimento)) AS ano_vencimento,
    EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', data_vencimento)) AS mes_vencimento,
    PARSE_DATE('%m/%d/%Y', data_vencimento) AS data_vencimento,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(id_categoria_emitente as string) id_categoria_emitente,
    safe_cast(id_empreendimento as string) id_empreendimento,
    safe_cast(id_fonte_recurso as string) id_fonte_recurso,
    safe_cast(id_instrumento_credito as string) id_instrumento_credito,
    safe_cast(id_programa as string) id_programa,
    safe_cast(cnpj_agente_investimento as string) cnpj_agente_investimento,
    safe_cast(
        cnpj_basico_instituicao_financeira as string
    ) cnpj_basico_instituicao_financeira,
    safe_cast(area_financiada as float64) area_financiada,
    safe_cast(valor_parcela_credito as float64) valor_parcela_credito,
    safe_cast(valor_recurso_proprio as float64) valor_recurso_proprio,
    safe_cast(taxa_juro as float64) taxa_juro,
    safe_cast(valor_percentual_risco_stn as float64) valor_percentual_risco_stn
from `de-zoomcamp-2k24.brazil_rural_credit_staging.microdados_operacao_test` as t
),

safra_emissao_calculations AS (
    SELECT 
        *,
        CASE 
            WHEN mes_emissao BETWEEN 1 AND 6 THEN CONCAT(CAST(ano_emissao - 1 AS STRING), '/', CAST(ano_emissao AS STRING))
            ELSE CONCAT(CAST(ano_emissao AS STRING), '/', CAST(ano_emissao + 1 AS STRING))
        END AS plano_safra_emissao,
        CASE 
            WHEN mes_vencimento BETWEEN 1 AND 6 THEN CONCAT(CAST(ano_vencimento - 1 AS STRING), '/', CAST(ano_vencimento AS STRING))
            ELSE CONCAT(CAST(ano_vencimento AS STRING), '/', CAST(ano_vencimento + 1 AS STRING))
        END AS plano_safra_vencimento,
    FROM staging_data
)

SELECT
*
from safra_emissao_calculations

