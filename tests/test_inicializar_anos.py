"""Testes da função inicializar_anos_processar — precedência de override.

Cobre os quatro ramos da lógica de precedência:
1. Override manual (force_anos) — ignora env var e detecção
2. Variável de ambiente (ANOS_OVERRIDE)
3. Detecção automática (get_anos_para_processar_inteligente)
4. Fallback para ano atual quando detecção retorna vazio

Os testes rodam no CI sem Databricks: dbutils não existe, então o
try/except no widget ANOS_OVERRIDE cai para string vazia, e o módulo
usa _CONFIG_DEGRADADO (CARGA=incremental), que não ativa o ramo
CARGA=completa.
"""

import importlib.util
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
CAMINHO_CONFIG = RAIZ / "05_apoio" / "config_parametros.py"

spec = importlib.util.spec_from_file_location("config_parametros", CAMINHO_CONFIG)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


# ---------------------------------------------------------------------------
# Ramo 1: Override manual (force_anos)
# ---------------------------------------------------------------------------

def test_override_manual_retorna_exatamente_o_passado(monkeypatch):
    """force_anos tem precedência máxima: ignora env var e detecção."""
    monkeypatch.setenv("ANOS_OVERRIDE", "2020,2021")
    resultado = config.inicializar_anos_processar(
        force_anos=[2023, 2024], silent=True
    )
    assert resultado == [2023, 2024]


def test_override_manual_ordena_resultado(monkeypatch):
    """Mesmo passado fora de ordem, retorna ordenado."""
    monkeypatch.setenv("ANOS_OVERRIDE", "2019")
    resultado = config.inicializar_anos_processar(
        force_anos=[2025, 2021, 2023], silent=True
    )
    assert resultado == [2021, 2023, 2025]


def test_override_manual_lista_vazia_nao_e_override(monkeypatch):
    """force_anos=[] é falsy — cai para o próximo ramo (env var)."""
    monkeypatch.setenv("ANOS_OVERRIDE", "2022,2023")
    resultado = config.inicializar_anos_processar(force_anos=[], silent=True)
    assert resultado == [2022, 2023]


# ---------------------------------------------------------------------------
# Ramo 2: Variável de ambiente (ANOS_OVERRIDE)
# ---------------------------------------------------------------------------

def test_env_var_definida_retorna_anos_da_env(monkeypatch):
    """Sem force_anos mas com ANOS_OVERRIDE, retorna os anos da env var."""
    monkeypatch.setenv("ANOS_OVERRIDE", "2021,2022,2023")
    resultado = config.inicializar_anos_processar(silent=True)
    assert resultado == [2021, 2022, 2023]


def test_env_var_ano_unico(monkeypatch):
    """ANOS_OVERRIDE com um único ano retorna lista de um elemento."""
    monkeypatch.setenv("ANOS_OVERRIDE", "2025")
    resultado = config.inicializar_anos_processar(silent=True)
    assert resultado == [2025]


def test_env_var_com_espacos(monkeypatch):
    """Espaços ao redor dos anos são tratados via strip()."""
    monkeypatch.setenv("ANOS_OVERRIDE", " 2021 , 2022 , 2023 ")
    resultado = config.inicializar_anos_processar(silent=True)
    assert resultado == [2021, 2022, 2023]


# ---------------------------------------------------------------------------
# Ramo 3: Detecção automática
# ---------------------------------------------------------------------------

def test_deteccao_automatica_retorna_anos_detectados(monkeypatch):
    """Sem override nem env var, chama detecção e retorna anos detectados."""
    monkeypatch.delenv("ANOS_OVERRIDE", raising=False)

    monkeypatch.setattr(
        config,
        "get_anos_para_processar_inteligente",
        lambda **kwargs: [2022, 2024],
    )

    resultado = config.inicializar_anos_processar(silent=True)
    assert resultado == [2022, 2024]


def test_deteccao_automatica_consolida_multiplas_fontes(monkeypatch):
    """Detecção consolida anos de múltiplas fontes sem duplicar."""
    monkeypatch.delenv("ANOS_OVERRIDE", raising=False)

    chamadas = []

    def mock_inteligente(**kwargs):
        fonte = kwargs.get("fonte")
        chamadas.append(fonte)
        return {
            "dre": [2023],
            "bpa": [2023, 2024],
            "bpp": [2025],
            "dre_silver": [2023],
            "bpa_silver": [2024],
            "bpp_silver": [2025],
        }[fonte]

    monkeypatch.setattr(
        config, "get_anos_para_processar_inteligente", mock_inteligente
    )

    resultado = config.inicializar_anos_processar(silent=True)
    assert resultado == [2023, 2024, 2025]
    assert len(chamadas) == 6


# ---------------------------------------------------------------------------
# Ramo 4: Fallback (detecção retorna vazio)
# ---------------------------------------------------------------------------

def test_deteccao_vazia_cai_em_fallback_ano_atual(monkeypatch):
    """Quando detecção retorna lista vazia, fallback usa ano atual."""
    monkeypatch.delenv("ANOS_OVERRIDE", raising=False)

    monkeypatch.setattr(
        config,
        "get_anos_para_processar_inteligente",
        lambda **kwargs: [],
    )

    resultado = config.inicializar_anos_processar(silent=True)
    ano_esperado = datetime.now(config.FUSO_PROJETO).year
    assert resultado == [ano_esperado]


def test_erro_na_deteccao_cai_em_fallback_ano_atual(monkeypatch):
    """Quando a detecção levanta exceção, fallback usa ano atual."""
    monkeypatch.delenv("ANOS_OVERRIDE", raising=False)

    def mock_erro(**kwargs):
        raise RuntimeError("SparkSession não disponível")

    monkeypatch.setattr(
        config, "get_anos_para_processar_inteligente", mock_erro
    )

    resultado = config.inicializar_anos_processar(silent=True)
    ano_esperado = datetime.now(config.FUSO_PROJETO).year
    assert resultado == [ano_esperado]