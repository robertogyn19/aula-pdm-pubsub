import io
import sys
from pathlib import Path

import pytest
import requests
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from simple_crawler import IMG_BASE_URL, Anuncio, baixar_imagem


def _anuncio(**alteracoes) -> Anuncio:
    """Um Anuncio completo, para os testes só precisarem declarar o que importa."""
    campos = dict(
        id=1,
        titulo="Casa em Goiânia",
        ativo=True,
        aceita_troca=False,
        pet_friendly=False,
        descricao="",
        area_total=100.0,
        area_util=80.0,
        categoria="casa",
        preco=250000.0,
        preco_fmt="R$ 250.000",
        preco_iptu="",
        imagens=[],
        transacao="venda",
        suites=1,
        quartos=3,
        banheiros=2,
        garagens=1,
        latitude=-16.6,
        longitude=-49.2,
        rua="Rua 1",
        bairro="Setor Central",
        cidade="Goiânia",
        estado="GO",
        cep="74000-000",
        data_atualizacao="2025-09-14",
    )
    campos.update(alteracoes)
    return Anuncio(**campos)


def _bytes_de_imagem(formato: str) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (8, 8), color=(120, 60, 30)).save(buffer, format=formato)
    return buffer.getvalue()


class _RespostaFalsa:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code
        self.headers = {"Content-Type": "image/jpeg"}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


def test_urls_imagens_monta_a_url_completa():
    anuncio = _anuncio(imagens=["ab/cd/1.jpg", "ab/cd/2.jpg"])

    assert anuncio.urls_imagens() == [
        f"{IMG_BASE_URL}/ab/cd/1.jpg",
        f"{IMG_BASE_URL}/ab/cd/2.jpg",
    ]


def test_urls_imagens_sem_imagens_devolve_lista_vazia():
    assert _anuncio(imagens=[]).urls_imagens() == []


def test_urls_imagens_aceita_outra_base():
    anuncio = _anuncio(imagens=["x.jpg"])

    assert anuncio.urls_imagens(base_url="https://exemplo.test/img") == [
        "https://exemplo.test/img/x.jpg"
    ]


def test_baixar_imagem_grava_o_jpeg_recebido(tmp_path, monkeypatch):
    original = _bytes_de_imagem("JPEG")
    monkeypatch.setattr(requests, "get", lambda *a, **k: _RespostaFalsa(original))

    destino = baixar_imagem("https://exemplo.test/1.jpg", tmp_path / "1.jpg")

    assert destino == tmp_path / "1.jpg"
    assert destino.read_bytes() == original


def test_baixar_imagem_converte_webp_para_jpeg(tmp_path, monkeypatch):
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: _RespostaFalsa(_bytes_de_imagem("WEBP"))
    )

    destino = baixar_imagem("https://exemplo.test/1.webp", tmp_path / "1.webp")

    assert destino == tmp_path / "1.jpg"
    with Image.open(destino) as imagem:
        assert imagem.format == "JPEG"


def test_baixar_imagem_cria_o_diretorio_de_destino(tmp_path, monkeypatch):
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: _RespostaFalsa(_bytes_de_imagem("JPEG"))
    )

    destino = baixar_imagem("https://exemplo.test/1.jpg", tmp_path / "novo" / "sub" / "1.jpg")

    assert destino.exists()


def test_baixar_imagem_propaga_erro_http(tmp_path, monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: _RespostaFalsa(b"", status_code=404))

    with pytest.raises(requests.HTTPError):
        baixar_imagem("https://exemplo.test/1.jpg", tmp_path / "1.jpg")
