import json
import logging
from pathlib import Path
from time import sleep
from typing import Any

import requests
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging = logging.getLogger(__name__)


class Anuncio(BaseModel):
    id: int
    titulo: str
    ativo: bool
    aceita_troca: bool
    pet_friendly: bool
    descricao: str
    area_total: float
    area_util: float
    categoria: str
    preco: float
    preco_fmt: str
    preco_iptu: str
    imagens: list[str]
    transacao: str
    suites: int
    quartos: int
    banheiros: int
    garagens: int
    latitude: float
    longitude: float
    rua: str
    bairro: str
    cidade: str
    estado: str
    cep: str
    data_atualizacao: str

    def endereco(self) -> str:
        partes = [self.rua, self.bairro, self.cidade, self.estado, self.cep]
        return ", ".join([parte for parte in partes if parte])


class ChavesNaMaoCrawler:
    _base_url_default = "https://www.chavesnamao.com.br"
    _base_path_default = "/api/realestate/listing/items/"

    def __init__(self, base_url: str = _base_url_default, base_path: str = _base_path_default):
        self.base_url = base_url
        self.base_path = base_path

    def coletar_paginas(
        self,
        level1: str,
        level2: str,
        primeira: int = 0,
        ultima: int = -1,
    ) -> list[Anuncio]:

        todos_anuncios: list[Anuncio] = []

        url = f"{self.base_url}{self.base_path}?level1={level1}&level2={level2}&pg={primeira}"
        logging.info(f"Coletando página {primeira}...")
        # Coleta a primeira página
        payload = self.coletar_pagina(url)

        # Insere os anúncios da primeira página
        todos_anuncios.extend(self.extrair_dados(payload))

        # Define a última página com base no total de páginas retornado pela API
        ultima = self._configura_ultima_pagina(payload, ultima)

        # Obtém a URL da próxima página
        next_url = self._proxima_url(payload)

        for page in range(primeira + 1, ultima + 1):
            try:
                logging.info(f"Coletando página {page}...")
                payload = self.coletar_pagina(next_url)
                anuncios = self.extrair_dados(payload)
                todos_anuncios.extend(anuncios)

                next_url = self._proxima_url(payload)
            except Exception as e:
                logging.error(f"Erro ao coletar página {page}: {e}")
                self.salva_payload_json(payload, f"../dados/erro_{level2}_pagina_{page}.json")
                continue
            finally:
                # Adiciona um delay de 1s entre as requisições para não sobrecarregar a API
                sleep(0.1)

        return todos_anuncios

    @staticmethod
    def _configura_ultima_pagina(payload: dict[str, Any], ultima: int) -> int:
        if ultima <= 0 or ultima > 100:
            ultima = payload.get("metadata", {}).get("totalPages", 1)
            logging.info(f"Número total de páginas: {ultima}")

            if ultima > 100:
                logging.warning("Atenção: É permitido coletar no máximo até a página 100")
                ultima = 100

        return ultima

    def _proxima_url(self, payload: dict[str, Any]) -> str:
        next_url = payload.get("metadata", {}).get("links", {}).get("nextApiParams", "")
        if next_url:
            next_url = f"{self.base_url}{self.base_path}{next_url}"

        return next_url

    @staticmethod
    def coletar_pagina(url: str) -> dict[str, Any]:
        payload = requests.get(url, timeout=15).json()
        return payload

    def extrair_dados(self, payload: dict[str, Any]) -> list[Anuncio]:
        anuncios: list[Anuncio] = []
        items = payload["items"]

        for item in items:
            if "id" not in item:
                continue

            anuncio = Anuncio(
                id=item.get("id", 0),
                titulo=item.get("title", ""),
                ativo=item.get("active", False),
                aceita_troca=item.get("acceptTrade", False),
                pet_friendly=item.get("petFriendly", False),
                descricao=item.get("description", ""),
                area_total=self.parse_float(item.get("area", {}).get("total", 0.0)),
                area_util=self.parse_float(item.get("area", {}).get("useful", 0.0)),
                categoria=item.get("category", ""),
                preco=item.get("prices", {}).get("rawPrice", 0.0),
                preco_fmt=item.get("prices", {}).get("main", ""),
                preco_iptu=item.get("prices", {}).get("iptuValue", ""),
                imagens=item.get("pictures", {}).get("list", []),
                transacao=item.get("transaction", ""),
                quartos=self.extrair_count(item, "bedrooms"),
                suites=item.get("suites", {}).get("count", 0) or 0,
                banheiros=item.get("bathrooms", {}).get("count", 0) or 0,
                garagens=item.get("garages", {}).get("count", 0) or 0,
                latitude=self.extrair_coordenadas(item, "lat"),
                longitude=self.extrair_coordenadas(item, "lon"),
                rua=self.extrair_localizacao(item, "street"),
                bairro=self.extrair_localizacao(item, "neighborhood"),
                cidade=self.extrair_localizacao(item, "city"),
                estado=self.extrair_localizacao(item, "state"),
                cep=self.extrair_localizacao(item, "zipCode"),
                data_atualizacao=item.get("updatedAt", ""),
            )
            anuncios.append(anuncio)

        return anuncios

    @staticmethod
    def parse_float(value: Any) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, str):
            try:
                return float(value.replace(",", "."))
            except ValueError:
                return 0.0
        else:
            return 0.0

    @staticmethod
    def extrair_localizacao(item: dict[str, Any], prop_name: str) -> str:
        location = item.get("location", {})
        if prop_name not in location:
            return ""

        prop = location.get(prop_name)

        if not isinstance(prop, dict):
            return str(prop).strip()

        if "name" not in prop:
            return ""

        nome = prop.get("name", "")
        return str(nome).strip()

    @staticmethod
    def extrair_coordenadas(item: dict[str, Any], prop_name: str) -> float:
        location = item.get("location", {})
        if "geoposition" not in location:
            return 0.0

        geoposition = location.get("geoposition", {})
        if prop_name not in geoposition:
            return 0.0

        coordenada = geoposition.get(prop_name, 0.0)
        if isinstance(coordenada, (int, float)):
            return float(coordenada)
        elif isinstance(coordenada, str):
            try:
                return float(coordenada)
            except ValueError:
                return 0.0
        else:
            return 0.0

    @staticmethod
    def salvar_anuncios_jsonl(anuncios: list[Anuncio], filename: str) -> None:
        with Path(filename).open("w", encoding="utf-8") as f:
            for anuncio in anuncios:
                f.write(anuncio.model_dump_json() + "\n")

    @staticmethod
    def salva_payload_json(payload: dict[str, Any], filename: str) -> None:
        with Path(filename).open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=4)

    @staticmethod
    def extrair_count(item: dict[str, Any], prop_name: str) -> int:
        prop = item.get(prop_name, {})
        count = prop.get("count", 0)
        max = prop.get("max", 0)

        if count == 0 and max == 0:
            return 0

        if isinstance(max, int):
            return max

        if isinstance(count, int):
            return count

        if isinstance(count, str):
            try:
                return int(count)
            except ValueError:
                return 0

        return 0


def realizar_coleta_varias_paginas():
    level1 = "casas-a-venda"

    crawler = ChavesNaMaoCrawler()
    chaves_na_mao_level2 = Path("../dados/chavesnamao_level2.txt").read_text().splitlines()

    for level2 in chaves_na_mao_level2:
        logging.info(f"Coletando anúncios para {level1} / {level2}...")
        arquivo_de_saida = f"../dados/anuncios/anuncios_{level2}.jsonl"

        if Path(arquivo_de_saida).exists():
            logging.info(f"O arquivo {arquivo_de_saida} já existe. Pulando coleta.")
            continue

        anuncios = crawler.coletar_paginas(level1, level2, primeira=1)
        if len(anuncios) > 0:
            logging.info(f"Salvando {len(anuncios)} anúncios em {arquivo_de_saida}...")
            crawler.salvar_anuncios_jsonl(anuncios, arquivo_de_saida)
        else:
            logging.info(f"Nenhum anúncio encontrado para {level1} / {level2}.")


def realizar_coleta_goiania():
    level1 = "imoveis-a-venda"
    level2 = "go-goiania"
    arquivo_de_saida = f"../dados/anuncios/imoveis_{level2}.jsonl"

    crawler = ChavesNaMaoCrawler()

    logging.info(f"Coletando anúncios para {level1} / {level2}...")

    anuncios = crawler.coletar_paginas(level1, level2, primeira=1, ultima=5)
    logging.info(f"Salvando {len(anuncios)} anúncios em {arquivo_de_saida}...")
    crawler.salvar_anuncios_jsonl(anuncios, arquivo_de_saida)


if __name__ == "__main__":
    # realizar_coleta_goiania()
    realizar_coleta_varias_paginas()
