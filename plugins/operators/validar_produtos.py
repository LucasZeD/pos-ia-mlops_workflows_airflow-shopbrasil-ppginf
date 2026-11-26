"""
Operador customizado: ValidarProdutosOperator

Validam-se os "schemas" dos produtos provenientes da API ANTES de
quaisquer cálculos. Caso as APIs alterem os formatos (ex.: price recebido como
texto, category ausente), as inconsistências são detectadas nesta etapa, e não
por meio de painéis que exibam números incorretos.

Comportamentos:
  - Produtos válidos   -> seguem para o processamento.
  - Produtos inválidos -> são descartados e registrados no log (com os motivos).
  - Nenhum válido      -> as tasks falham (painéis vazios não justificam gravações).

Como os operadores recebem as saídas de tasks TaskFlow (XComArgs), os
parâmetros `produtos` precisam constar em `template_fields`: é durante as
renderizações dos templates que o Airflow substitui os XComArgs pelos valores reais.
"""

from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    # Definem-se os campos obrigatórios e os tipos esperados de cada um.
    SCHEMA: dict[str, type | tuple[type, ...]] = {
        "id": int,
        "title": str,
        "price": (int, float),
        "category": str,
    }

    template_fields: Sequence[str] = ("produtos",)
    ui_color = "#ffe8a3"

    def __init__(self, *, produtos: Any, falhar_se_vazio: bool = True, **kwargs) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos
        self.falhar_se_vazio = falhar_se_vazio

    # ------------------------------------------------------------------ auxiliares
    def _erros_do_produto(self, produto: Any) -> list[str]:
        """Retornam-se as listas de problemas encontrados (listas vazias indicam produtos válidos)."""
        if not isinstance(produto, dict):
            return [f"esperado dict, veio {type(produto).__name__}"]

        erros = []
        for campo, tipo in self.SCHEMA.items():
            valor = produto.get(campo)
            if valor is None:
                erros.append(f"campo '{campo}' ausente")
            # Os valores bool constituem subclasses de int em Python; por isso, tratam-se esses valores separadamente.
            elif isinstance(valor, bool) or not isinstance(valor, tipo):
                erros.append(f"campo '{campo}' com tipo inválido ({type(valor).__name__})")

        preco = produto.get("price")
        if isinstance(preco, (int, float)) and not isinstance(preco, bool) and preco < 0:
            erros.append("price negativo")

        categoria = produto.get("category")
        if isinstance(categoria, str) and not categoria.strip():
            erros.append("category vazia")

        return erros

    # ------------------------------------------------------------------ execute
    def execute(self, context) -> list[dict]:
        """Validam-se os produtos recebidos, descartando-se os registros inválidos."""
        produtos = self.produtos or []
        validos: list[dict] = []
        qtd_invalidos = 0

        for produto in produtos:
            erros = self._erros_do_produto(produto)
            if erros:
                qtd_invalidos += 1
                ident = produto.get("id") if isinstance(produto, dict) else "?"
                self.log.warning("Produto id=%s descartado: %s", ident, "; ".join(erros))
            else:
                validos.append(produto)

        self.log.info(
            "Validação concluída: %d recebidos | %d válidos | %d descartados",
            len(produtos), len(validos), qtd_invalidos,
        )

        if self.falhar_se_vazio and not validos:
            raise AirflowException("Nenhum produto válido após a validação de schema.")

        # Os retornos convertem-se automaticamente em XComs (do_xcom_push=True por padrão).
        return validos
