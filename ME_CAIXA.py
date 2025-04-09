import os
import pandas as pd
import pyodbc
import re

# Caminho para o arquivo .accdb
accdb_file = r"C:\Users\pedro.pneto\PycharmProjects\PythonProject1\CAIXA\me1"

# Diretório de saída
output_dir = r"C:\Users\pedro.pneto\OneDrive - Ministério do Desenvolvimento e Assistência Social\Power BI\DIE"

# Verificar e criar o diretório, caso não exista
os.makedirs(output_dir, exist_ok=True)
print(f"Diretório '{output_dir}' pronto para uso.")

# Conectar ao banco de dados Access
connection_string = fr"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={accdb_file}"
conn = pyodbc.connect(connection_string)

# Função para ajustar valores percentuais
def ajustar_percentual(coluna):
    # Corrigir valores superiores a 100% para 100%
    coluna = coluna.apply(lambda x: min(x, 100) if pd.notna(x) else x)
    # Formatar para porcentagem com duas casas decimais
    coluna = coluna.apply(lambda x: f"{x:.2f}%" if pd.notna(x) else x)
    return coluna

try:
    # Carregar as tabelas relevantes
    df_dados_cadastrais = pd.read_sql("SELECT * FROM [DADOS CADASTRAIS DA OPERAÇÃO]", conn)
    df_execucao_fisica = pd.read_sql("SELECT * FROM [EXECUÇÃO FÍSICA]", conn)
    df_situacao_contrato = pd.read_sql("SELECT * FROM [SITUAÇÃO_CONTRATO]", conn)
    df_objetivo = pd.read_sql("SELECT * FROM [OBJETIVO]", conn)
    df_situacao_obra = pd.read_sql("SELECT * FROM [SITUAÇÃO_OBRA]", conn)
    df_programa = pd.read_sql("SELECT * FROM [PROGRAMA]", conn)
    df_dados_orcamentarios = pd.read_sql("SELECT * FROM [DADOS ORÇAMENTÁRIOS DO PT]", conn)
    df_execucao_financeira = pd.read_sql("SELECT * FROM [EXECUÇÃO FINANCEIRA]", conn)

    # Merge inicial para juntar os dados
    df_combined = pd.merge(df_dados_cadastrais, df_execucao_fisica, on='PT', how='outer')

    # Criar dicionários para substituição dos códigos pelos valores correspondentes
    contrato_dict = dict(zip(df_situacao_contrato['COD_SIT_CONTRATO'], df_situacao_contrato['SIT_CONTRATO']))
    objetivo_dict = dict(zip(df_objetivo['COD_OBJETIVO'], df_objetivo['OBJETIVO']))
    obra_dict = dict(zip(df_situacao_obra['COD_SIT_OBRA'], df_situacao_obra['SIT_OBRA']))
    programa_dict = dict(zip(df_programa['COD_PROGRAMA'], df_programa['PROGRAMA']))

    # Substituir os valores nos DataFrames
    df_combined['COD_SIT_CONTRATO'] = df_combined['COD_SIT_CONTRATO'].map(contrato_dict).fillna(
        df_combined['COD_SIT_CONTRATO'])
    df_combined['COD_OBJETIVO'] = df_combined['COD_OBJETIVO'].map(objetivo_dict).fillna(df_combined['COD_OBJETIVO'])
    df_combined['COD_SIT_OBRA'] = df_combined['COD_SIT_OBRA'].map(obra_dict).fillna(df_combined['COD_SIT_OBRA'])
    df_combined['PROGRAMA'] = df_combined['COD_PROGRAMA'].map(programa_dict).fillna(df_combined['COD_PROGRAMA'])

    # Adicionar a coluna PROGRAMA ao lado de COD_PROGRAMA
    df_combined['PROGRAMA'] = df_combined['COD_PROGRAMA'].map(programa_dict).fillna(df_combined['PROGRAMA'])

    # Selecionar colunas específicas da tabela de dados orçamentários
    colunas_orcamentarias = ["DT_NE", "FONTE", "NATUREZA_DESPESA", "NE", "PT", "STATUS", "FUNCIONAL_PROGRAMATICA",
                             "VLR_NE"]
    df_dados_orcamentarios = df_dados_orcamentarios[colunas_orcamentarias]


    # Função para extrair a parte reduzida da 'FUNCIONAL_PROGRAMATICA' (da posição 10 à 13)
    def extrair_reduzida(fp):
        if pd.isna(fp):
            return None
        # A função agora pega os caracteres entre as posições 10 e 13 (base 0)
        fp_str = str(fp)
        return fp_str[9:13]  # Pega os 4 caracteres do 10º ao 13º (base 0)

    # Aplicar a função para extrair os dados
    df_dados_orcamentarios["FUNCIONAL_PROGRAMATICA_REDUZIDA"] = df_dados_orcamentarios["FUNCIONAL_PROGRAMATICA"].apply(
        extrair_reduzida)

    # Merge com df_combined
    df_combined = pd.merge(df_combined, df_dados_orcamentarios, on="PT", how="outer")

    # Selecionar colunas específicas da tabela de execução financeira
    colunas_execucao_financeira = ["PT", "VLR_DESBLOQUEADO", "VLR_LIBERADO", "VLR_REPASSE_DEVOLVIDO", "VLR_SOLICITADO"]
    df_execucao_financeira = df_execucao_financeira[colunas_execucao_financeira]

    # Renomear VLR_REPASSE para VLR_REPASSE_DEVOLVIDO
    df_execucao_financeira.rename(columns={"VLR_REPASSE": "VLR_REPASSE_DEVOLVIDO"}, inplace=True)

    # Merge final
    df_combined = pd.merge(df_combined, df_execucao_financeira, on="PT", how="outer")

    # Ajustar as colunas de percentual
    if 'ULT_%_REALIZADO' in df_combined.columns:
        df_combined['ULT_%_REALIZADO'] = ajustar_percentual(df_combined['ULT_%_REALIZADO'])

    if 'ULT_%_INFORMADO' in df_combined.columns:
        df_combined['ULT_%_INFORMADO'] = ajustar_percentual(df_combined['ULT_%_INFORMADO'])

    # Salvar o resultado
    output_file = os.path.join(output_dir, "resultado_agrupado.xlsx")
    df_combined.to_excel(output_file, index=False)

    print(f"Resultado com formatação corrigida salvo em {output_file}")

finally:
    conn.close()
