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