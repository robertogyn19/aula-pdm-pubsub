import base64
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from langchain.chains.base import Chain
from langchain.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if GOOGLE_API_KEY is None:
    raise ValueError(
        "A chave da API do Google (GOOGLE_API_KEY) não está definida no arquivo .env"
    )

MODEL = os.getenv("MODEL", "gemini-2.5-flash")


def encode_image(image_bytes: bytes):
    """Codifica bytes de imagem em base64."""
    return base64.b64encode(image_bytes).decode("utf-8")


def encode_images_from_bytes(image_bytes_list: list[bytes]):
    """Codifica uma lista de bytes de imagem em base64."""
    return [encode_image(image_bytes) for image_bytes in image_bytes_list]


def read_prompt_template(filepath: str):
    """Lê um arquivo de texto e retorna o conteúdo como uma string."""
    with open(filepath, "r", encoding="utf-8") as file:
        return file.read()


def configure_llm(image_data_list: list[bytes], model=MODEL, temperature=0.05) -> Chain:
    # noinspection PyArgumentList
    llm = ChatGoogleGenerativeAI(
        model=model,
        google_api_key=GOOGLE_API_KEY,
        temperature=temperature
    )

    encoded_images = encode_images_from_bytes(image_data_list)

    system_message_prompt = read_prompt_template("prompts/system_message_prompt.txt")
    human_message_prompt_template = read_prompt_template("prompts/human_message_prompt.txt")

    # Adicionar informação sobre o número de imagens e pedir para identificar cada uma
    human_message_text = human_message_prompt_template

    prompt_messages = [
        SystemMessage(content=system_message_prompt)
    ]

    human_message_content_parts = [{"type": "text", "text": human_message_text}]

    for i, encoded_image in enumerate(encoded_images):
        # Idealmente, o prompt instruiria o modelo a referenciar as imagens pela ordem ou um ID.
        human_message_content_parts.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{encoded_image}"},
            }
        )
    prompt_messages.append(HumanMessage(content=human_message_content_parts))
    prompt = ChatPromptTemplate.from_messages(prompt_messages)

    output_parser = StrOutputParser()
    return prompt | llm | output_parser


def process_images(image_data_list: list[bytes], model=MODEL, temperature=0.05, original_filenames=None):
    """
    Processa uma lista de dados de imagem (bytes) com o Gemini e retorna os resultados.
    Esta versão envia TODAS as imagens em uma ÚNICA chamada ao Gemini.
    O resultado é um dicionário onde as chaves são identificadores de imagem
    e os valores são os JSONs de análise.
    """
    chain = configure_llm(image_data_list, model=model, temperature=temperature)

    final_results = {}

    try:
        result_str = chain.invoke({})
        output_text = re.sub(r"```(?:json)?\n?(.*?)\n?```", r"\1", result_str, flags=re.DOTALL)
        output_text = output_text.strip()

        # Verifica se o output_text está vazio ou contém apenas espaços em branco
        if not output_text:
            final_results["erro_processamento"] = {"erro": "Resposta vazia do modelo Gemini."}
            print("Erro: Resposta vazia do modelo Gemini.")
            return final_results

        result_json_data = json.loads(output_text)

        if isinstance(result_json_data, list):
            for i, image_result in enumerate(result_json_data):
                key = ""
                if original_filenames and i < len(original_filenames):
                    key = original_filenames[i]
                # O Gemini pode fornecer um campo 'imagem' em sua resposta por item
                if isinstance(image_result, dict) and "imagem" in image_result:
                    key = image_result["imagem"]
                elif not key:  # Fallback se o nome do arquivo não estiver disponível e a IA não fornecer um
                    key = f"analise_imagem_{i + 1}"
                final_results[key] = image_result
        elif isinstance(result_json_data, dict):
            # Se for um dicionário único, assume-se que é para a primeira imagem ou uma análise combinada
            # ou um erro do modelo.
            if "erro" in result_json_data:  # Verifica se é um objeto de erro do modelo
                final_results["erro_gemini"] = result_json_data
            else:
                key = "analise_unica_combinada"
                if original_filenames:  # Se apenas uma imagem foi enviada
                    key = original_filenames[0] if len(original_filenames) == 1 else key
                # Se o próprio dicionário tiver um campo 'imagem', use-o como chave
                if "imagem" in result_json_data:
                    key = result_json_data["imagem"]
                final_results[key] = result_json_data
        else:
            final_results["erro_formato_inesperado"] = {
                "erro": f"Formato de JSON inesperado: {type(result_json_data)}",
                "raw_output": output_text,
            }

    except json.JSONDecodeError:
        final_results["erro_decodificacao_json"] = {
            "erro": f"Resposta inválida do Gemini (não é JSON decodificável): {output_text[:500]}..."}  # Trunca saídas longas
        print(f"Erro ao decodificar JSON: {output_text}")
    except Exception as e:
        final_results["erro_geral_processamento"] = {"erro": str(e)}
        print(f"Erro ao processar imagens: {e}")

    return final_results


if __name__ == "__main__":
    from PIL import Image
    import io

    test_image_paths = Path("imgs/42").glob("*.jpg")
    image_bytes_list = []

    for path in test_image_paths:
        img = Image.open(path)
        buf = io.BytesIO()
        img.save(buf, format="JPEG")
        image_bytes = buf.getvalue()
        image_bytes_list.append(image_bytes)

    results = process_images(image_bytes_list, original_filenames=[os.path.basename(p) for p in test_image_paths])
    print(json.dumps(results, indent=2, ensure_ascii=False))
