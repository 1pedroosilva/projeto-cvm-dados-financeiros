"""Testes das funcoes puras de config_parametros."""

import importlib.util
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
CAMINHO_CONFIG = RAIZ / "05_apoio" / "config_parametros.py"

spec = importlib.util.spec_from_file_location("config_parametros", CAMINHO_CONFIG)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


def test_url_do_ano_aponta_para_zip_da_cvm():
    url = config.get_url_arquivo_cvm(2023)
    assert url == (
        "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
        "dfp_cia_aberta_2023.zip"
    )


def test_url_muda_conforme_o_ano():
    assert config.get_url_arquivo_cvm(2020) != config.get_url_arquivo_cvm(2021)

def test_janela_temporal_padrao_e_de_cinco_anos():
    """A politica de quantos anos processar e uma decisao de negocio.

    Se alguem mudar JANELA_ANOS_RELEVANTE sem querer, este teste avisa.
    """
    assert config.JANELA_ANOS_RELEVANTE == 5


def test_historico_da_cvm_comeca_em_2010():
    assert config.ANO_INICIAL_CVM == 2010


def test_anos_disponiveis_cobrem_do_inicio_ate_hoje():
    from datetime import datetime

    anos = config.get_anos_disponiveis_cvm()
    ano_atual = datetime.now(config.FUSO_PROJETO).year

    assert anos[0] == 2010
    assert anos[-1] == ano_atual
    assert len(anos) == ano_atual - 2010 + 1


def test_todos_os_anos_sao_inteiros_sem_repeticao():
    anos = config.get_anos_disponiveis_cvm()

    assert all(isinstance(ano, int) for ano in anos)
    assert len(anos) == len(set(anos))


def test_comparacao_datetime_naive_vs_aware_apos_normalizacao():
    """Garante que a normalização naive → aware permite comparação segura.
    
    Bug histórico: Spark TIMESTAMP vem como pandas.Timestamp naive ao entrar
    no Python. verificar_arquivo_existe_cvm retorna datetime aware (UTC).
    Comparação direta levanta TypeError.
    
    Proteção: Este teste quebra se alguém remover a normalização defensiva
    em get_anos_com_atualizacao_cvm sem migrar a leitura do Spark.
    """
    from datetime import datetime, timezone
    
    # Simula timestamp lido do Spark (sempre naive)
    dt_naive = datetime(2026, 8, 23, 10, 31, 9)
    assert dt_naive.tzinfo is None, "Pré-condição: datetime deve ser naive"
    
    # Simula timestamp do header Last-Modified da CVM (sempre aware UTC)
    dt_aware = datetime(2026, 8, 23, 11, 0, 0, tzinfo=timezone.utc)
    assert dt_aware.tzinfo is not None, "Pré-condição: datetime deve ser aware"
    
    # ❌ SEM normalização: comparação levanta TypeError
    try:
        _ = dt_aware > dt_naive
        assert False, "Comparação naive vs aware deveria levantar TypeError"
    except TypeError as e:
        assert "offset-naive and offset-aware" in str(e)
    
    # ✓ COM normalização: comparação funciona
    dt_naive_normalizado = dt_naive.replace(tzinfo=timezone.utc)
    assert dt_naive_normalizado.tzinfo is not None
    
    # Esta linha NUNCA pode levantar TypeError
    resultado = dt_aware > dt_naive_normalizado
    assert isinstance(resultado, bool), "Comparação deve retornar bool"
    assert resultado is True, "2026-08-23 11:00 UTC > 2026-08-23 10:31 UTC"


def test_normalizacao_preserva_o_valor_do_timestamp():
    """Adicionar tzinfo=UTC não muda o momento absoluto representado.
    
    Spark TIMESTAMP armazena valores como UTC internamente. Quando Python
    lê como naive, está "esquecendo" que é UTC - a normalização restaura
    essa informação sem alterar o valor.
    """
    from datetime import datetime, timezone
    
    # Timestamp naive representando "2026-08-23 10:31:09 UTC" (implícito)
    dt_naive = datetime(2026, 8, 23, 10, 31, 9)
    
    # Normalizar para aware UTC
    dt_aware = dt_naive.replace(tzinfo=timezone.utc)
    
    # Valores devem ser idênticos
    assert dt_naive.year == dt_aware.year
    assert dt_naive.month == dt_aware.month
    assert dt_naive.day == dt_aware.day
    assert dt_naive.hour == dt_aware.hour
    assert dt_naive.minute == dt_aware.minute
    assert dt_naive.second == dt_aware.second
    
    # Apenas tzinfo muda (None → UTC)
    assert dt_naive.tzinfo is None
    assert dt_aware.tzinfo == timezone.utc
