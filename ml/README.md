### Análise de Imagens com Gemini

Este repositório contém um projeto de análise de imagens utilizando o modelo Gemini da Google. O objetivo é demonstrar
como utilizar o Gemini para tarefas de visão computacional e processamento de linguagem natural para extrair informações
de imagens.

Para executar o código, é necessário criar uma chave de API do Google AI Studio e definir a variável de ambiente
`GOOGLE_API_KEY` com essa chave.

Siga os passos abaixo para criar a chave:

- Acesse o [link](https://aistudio.google.com/app/apikey) https://aistudio.google.com/app/apikey
- Clique no botão `Criar chave de API` do canto superior direito
- Selecione o projeto do GCP que está utilizando para a aula
- Depois clique no botão `Criar uma chave de API em um projeto atual`
- Copie a chave gerada e defina a variável de ambiente `GOOGLE_API_KEY` no seu ambiente local ou no notebook.

Para executar o código, certifique-se de ter as bibliotecas necessárias instaladas. Você pode instalar as dependências
utilizando o arquivo `requirements.txt`:

```bash
pip install -r requirements.txt
```