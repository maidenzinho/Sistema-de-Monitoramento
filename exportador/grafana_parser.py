from __future__ import annotations

from typing import Any, Dict, List


def converter_query_inspector_para_paineis(query_inspector: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converte um JSON típico do Query Inspector do Grafana (datasource Elasticsearch)
    para a estrutura de paineis suportada pelo exportador.
    """
    paineis: List[Dict[str, Any]] = []

    queries = (
        query_inspector.get("queries")
        or query_inspector.get("request", {}).get("data", {}).get("queries")
        or []
    )
    if not isinstance(queries, list):
        return paineis

    for idx, q in enumerate(queries):
        bucket_aggs = q.get("bucketAggs") or []
        metrics = q.get("metrics") or []
        query_str = q.get("query") or q.get("queryString") or ""

        metrica = {"tipo": "count"}
        if metrics and isinstance(metrics, list):
            m0 = metrics[0] or {}
            m_tipo = m0.get("type") or "count"
            metrica["tipo"] = m_tipo
            if m_tipo != "count":
                metrica["campo"] = m0.get("field")

        tem_date = any(isinstance(b, dict) and b.get("type") in ("date_histogram", "dateHistogram") for b in bucket_aggs)
        tem_terms = any(isinstance(b, dict) and b.get("type") == "terms" for b in bucket_aggs)

        nome = f"Grafana_{idx+1}"
        if isinstance(q.get("refId"), str):
            nome = f"Grafana_{q['refId']}"

        if tem_date:
            intervalo = "5m"
            for b in bucket_aggs:
                if isinstance(b, dict) and b.get("type") in ("date_histogram", "dateHistogram"):
                    settings = b.get("settings") or {}
                    intervalo = settings.get("interval") or settings.get("fixed_interval") or "5m"
                    break
            paineis.append({"nome": nome, "tipo": "time_series", "intervalo": str(intervalo), "metrica": metrica, "query_grafana": query_str})
            continue

        if tem_terms:
            campo = None
            tamanho = 10
            for b in bucket_aggs:
                if isinstance(b, dict) and b.get("type") == "terms":
                    campo = b.get("field")
                    settings = b.get("settings") or {}
                    tamanho = int(settings.get("size", 10))
                    break
            paineis.append({"nome": nome, "tipo": "top_terms", "campo": campo, "tamanho": tamanho, "metrica": metrica, "query_grafana": query_str})
            continue

    return paineis
