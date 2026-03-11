from __future__ import annotations

import os
import math
import time
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from elasticsearch import Elasticsearch


class ErroExportacao(RuntimeError):
    pass


@dataclass
class ConfigElastic:
    url: str
    usuario: str = ""
    senha: str = ""
    verify_certs: bool = True
    ca_certs: str = ""
    suppress_tls_warnings: bool = False


def _agora_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _callback_vazio(p: int, msg: str) -> None:
    pass


def _normalizar_iso(iso: str) -> str:
    iso = (iso or "").strip()
    if not iso:
        return ""
    if len(iso) == 16:
        return iso + ":00"
    return iso


def _montar_query_time_range(campo_tempo: str, inicio_iso: str, fim_iso: str) -> Dict[str, Any]:
    inicio_iso = _normalizar_iso(inicio_iso)
    fim_iso = _normalizar_iso(fim_iso)

    filtros = []
    if inicio_iso or fim_iso:
        rng: Dict[str, Any] = {}
        if inicio_iso:
            rng["gte"] = inicio_iso
        if fim_iso:
            rng["lte"] = fim_iso
        filtros.append({"range": {campo_tempo: rng}})

    if not filtros:
        return {"match_all": {}}

    return {"bool": {"filter": filtros}}


def _sugerir_freq(inicio_iso: str, fim_iso: str) -> str:
    try:
        ini = pd.to_datetime(_normalizar_iso(inicio_iso), utc=True) if inicio_iso else None
        fim = pd.to_datetime(_normalizar_iso(fim_iso), utc=True) if fim_iso else None
        if ini is None or fim is None:
            return "5min"
        delta = (fim - ini).total_seconds()
        if delta <= 2 * 3600:
            return "1min"
        if delta <= 24 * 3600:
            return "5min"
        if delta <= 7 * 24 * 3600:
            return "30min"
        if delta <= 31 * 24 * 3600:
            return "2h"
        return "1d"
    except Exception:
        return "5min"


def _excel_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        try:
            if pd.api.types.is_datetime64tz_dtype(df[c]):
                df[c] = df[c].dt.tz_convert(None)
        except Exception:
            pass

    for c in df.columns:
        if df[c].dtype == "object":
            try:
                df[c] = df[c].astype("string").str.slice(0, 32700)
            except Exception:
                pass
    return df


def _top_terms_df(df: pd.DataFrame, coluna: str, n: int = 25) -> pd.DataFrame:
    if coluna not in df.columns:
        return pd.DataFrame(columns=[coluna, "count"])
    vc = df[coluna].astype("string").value_counts(dropna=True).head(n)
    out = vc.reset_index()
    out.columns = [coluna, "count"]
    return out


def _serie_tempo_df(df: pd.DataFrame, coluna_tempo: str, freq: str = "5min") -> pd.DataFrame:
    if coluna_tempo not in df.columns:
        return pd.DataFrame(columns=["tempo", "count"])
    ts = pd.to_datetime(df[coluna_tempo], errors="coerce", utc=True)
    s = ts.dropna()
    if s.empty:
        return pd.DataFrame(columns=["tempo", "count"])

    ser = s.dt.floor(freq).value_counts().sort_index()
    out = ser.reset_index()
    out.columns = ["tempo", "count"]

    out["tempo"] = pd.to_datetime(out["tempo"], utc=True).dt.tz_localize(None)
    return out


class ExportadorElasticParaExcel:
    def __init__(self, cfg_elastic: Dict[str, Any] | ConfigElastic, indice: str, campo_tempo: str = "@timestamp"):
        if isinstance(cfg_elastic, dict):
            cfg_elastic = ConfigElastic(**cfg_elastic)
        self.cfg = cfg_elastic
        self.indice = indice
        self.campo_tempo = campo_tempo

        if not self.indice:
            raise ErroExportacao("Índice não informado. Defina DEFAULT_INDEX no .env ou envie 'indice'.")

        self.cliente = self._criar_cliente()

    def _criar_cliente(self) -> Elasticsearch:
        kwargs: Dict[str, Any] = {"hosts": [self.cfg.url]}
        if self.cfg.usuario or self.cfg.senha:
            kwargs["basic_auth"] = (self.cfg.usuario, self.cfg.senha)

        kwargs["verify_certs"] = bool(self.cfg.verify_certs)
        if self.cfg.ca_certs:
            kwargs["ca_certs"] = self.cfg.ca_certs

        kwargs["request_timeout"] = 180

        if not self.cfg.verify_certs and self.cfg.suppress_tls_warnings:
            warnings.filterwarnings("ignore", message="Unverified HTTPS request")
            warnings.filterwarnings("ignore", message="Connecting to .* verify_certs=False")

        return Elasticsearch(**kwargs)

    def testar_conexao(self) -> Dict[str, Any]:
        try:
            info = self.cliente.info()
            return {"cluster_name": info.get("cluster_name"), "version": info.get("version", {})}
        except Exception as e:
            raise ErroExportacao(f"Falha ao conectar no Elasticsearch: {e}")

    def _buscar_todos_documentos(
        self,
        query_string: str,
        inicio_iso: str,
        fim_iso: str,
        tamanho_pagina: int,
        max_docs: Optional[int],
        callback_progresso: Callable[[int, str], None],
    ) -> List[Dict[str, Any]]:
        query_string = (query_string or "*").strip()
        filtro_tempo = _montar_query_time_range(self.campo_tempo, inicio_iso, fim_iso)

        base_query: Dict[str, Any] = {
            "bool": {
                "must": [{"query_string": {"query": query_string}}],
                "filter": [] if "match_all" in filtro_tempo else [filtro_tempo],
            }
        }

        try:
            pit = self.cliente.open_point_in_time(index=self.indice, keep_alive="5m")
            pit_id = pit["id"]
        except Exception as e:
            raise ErroExportacao(f"Não consegui abrir PIT no índice '{self.indice}': {e}")

        documentos: List[Dict[str, Any]] = []
        search_after = None
        total = 0

        try:
            resp0 = self.cliente.search(
                size=0,
                track_total_hits=True,
                query=base_query,
                pit={"id": pit_id, "keep_alive": "5m"},
            )
            total_estimado = resp0.get("hits", {}).get("total", {}).get("value", None)
        except Exception:
            total_estimado = None

        callback_progresso(2, "Lendo documentos (raw)...")

        while True:
            corpo: Dict[str, Any] = {
                "size": int(tamanho_pagina),
                "query": base_query,
                "pit": {"id": pit_id, "keep_alive": "5m"},
                "sort": [
                    {self.campo_tempo: "asc"},
                    {"_shard_doc": "asc"},
                ],
            }
            if search_after is not None:
                corpo["search_after"] = search_after

            try:
                resp = self.cliente.search(**corpo)
                hits = resp.get("hits", {}).get("hits", [])
                if not hits:
                    break

                for h in hits:
                    src = h.get("_source", {}) or {}
                    src["_id"] = h.get("_id")
                    src["_index"] = h.get("_index")
                    src["_score"] = h.get("_score")
                    documentos.append(src)
                    total += 1
                    if max_docs and total >= max_docs:
                        break

                search_after = hits[-1].get("sort")

                if total_estimado and total_estimado > 0:
                    p = min(60, int(2 + (total / total_estimado) * 58))
                    callback_progresso(p, f"Lendo documentos (raw): {total:,}/{total_estimado:,}")
                else:
                    callback_progresso(min(60, 2 + int(math.log10(max(total, 1)) * 10)), f"Lendo documentos (raw): {total:,}")

                if max_docs and total >= max_docs:
                    break

            except Exception as e:
                raise ErroExportacao(f"Erro ao buscar documentos: {e}")

        try:
            self.cliente.close_point_in_time(body={"id": pit_id})
        except Exception:
            pass

        return documentos

    def _executar_agregacao(self, query_string: str, inicio_iso: str, fim_iso: str, painel: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
        tipo = (painel.get("tipo") or "").strip()
        nome = (painel.get("nome") or tipo or "painel").strip()

        filtro_tempo = _montar_query_time_range(self.campo_tempo, inicio_iso, fim_iso)
        filtros = [] if "match_all" in filtro_tempo else [filtro_tempo]

        query: Dict[str, Any] = {
            "bool": {
                "must": [{"query_string": {"query": (query_string or "*")}}],
                "filter": filtros,
            }
        }

        if tipo == "time_series":
            intervalo = str(painel.get("intervalo", "5m"))
            metrica = painel.get("metrica", {"tipo": "count"})
            campo_metrica = metrica.get("campo")
            tipo_metrica = metrica.get("tipo", "count")

            aggs: Dict[str, Any] = {
                "serie": {
                    "date_histogram": {"field": self.campo_tempo, "fixed_interval": intervalo, "min_doc_count": 0},
                    "aggs": {},
                }
            }
            if tipo_metrica != "count":
                if not campo_metrica:
                    raise ErroExportacao(f"Painel '{nome}': metrica '{tipo_metrica}' precisa de 'campo'.")
                aggs["serie"]["aggs"]["valor"] = {tipo_metrica: {"field": campo_metrica}}

            resp = self.cliente.search(index=self.indice, size=0, query=query, aggs=aggs)
            buckets = resp.get("aggregations", {}).get("serie", {}).get("buckets", [])
            linhas = []
            for b in buckets:
                ts = b.get("key_as_string") or b.get("key")
                val = b.get("doc_count", 0) if tipo_metrica == "count" else (b.get("valor", {}) or {}).get("value", None)
                linhas.append({"tempo": ts, "valor": val})
            df = pd.DataFrame(linhas)
            if "tempo" in df.columns:
                df["tempo"] = pd.to_datetime(df["tempo"], errors="coerce", utc=True).dt.tz_localize(None)
            return df, nome

        if tipo == "top_terms":
            campo = painel.get("campo")
            tamanho = int(painel.get("tamanho", 10))
            metrica = painel.get("metrica", {"tipo": "count"})
            campo_metrica = metrica.get("campo")
            tipo_metrica = metrica.get("tipo", "count")

            if not campo:
                raise ErroExportacao(f"Painel '{nome}': 'campo' é obrigatório para top_terms.")

            termo_agg: Dict[str, Any] = {"terms": {"field": campo, "size": tamanho, "order": {"_count": "desc"}}, "aggs": {}}
            if tipo_metrica != "count":
                if not campo_metrica:
                    raise ErroExportacao(f"Painel '{nome}': metrica '{tipo_metrica}' precisa de 'campo'.")
                termo_agg["aggs"]["valor"] = {tipo_metrica: {"field": campo_metrica}}

            resp = self.cliente.search(index=self.indice, size=0, query=query, aggs={"top": termo_agg})
            buckets = resp.get("aggregations", {}).get("top", {}).get("buckets", [])
            linhas = []
            for b in buckets:
                chave = b.get("key")
                val = b.get("doc_count", 0) if tipo_metrica == "count" else (b.get("valor", {}) or {}).get("value", None)
                linhas.append({"chave": chave, "valor": val})
            return pd.DataFrame(linhas), nome

        if tipo == "table":
            campos = painel.get("campos", [])
            tamanho = int(painel.get("tamanho", 100))
            metrica = painel.get("metrica", {"tipo": "count"})
            tipo_metrica = metrica.get("tipo", "count")
            campo_metrica = metrica.get("campo")

            if not campos or not isinstance(campos, list):
                raise ErroExportacao(f"Painel '{nome}': 'campos' (lista) é obrigatório para table.")

            fontes = [{"campo_" + str(i): {"terms": {"field": f}}} for i, f in enumerate(campos)]
            after = None
            linhas = []

            while True:
                comp: Dict[str, Any] = {"size": min(tamanho, 1000), "sources": fontes}
                if after:
                    comp["after"] = after

                aggs = {"comp": {"composite": comp, "aggs": {}}}
                if tipo_metrica != "count":
                    if not campo_metrica:
                        raise ErroExportacao(f"Painel '{nome}': metrica '{tipo_metrica}' precisa de 'campo'.")
                    aggs["comp"]["aggs"]["valor"] = {tipo_metrica: {"field": campo_metrica}}

                resp = self.cliente.search(index=self.indice, size=0, query=query, aggs=aggs)
                comp_agg = resp.get("aggregations", {}).get("comp", {}) or {}
                buckets = comp_agg.get("buckets", []) or []
                for b in buckets:
                    chaves = b.get("key", {}) or {}
                    row = {}
                    for i in range(len(campos)):
                        row[campos[i]] = chaves.get(f"campo_{i}")
                    row["valor"] = b.get("doc_count", 0) if tipo_metrica == "count" else (b.get("valor", {}) or {}).get("value", None)
                    linhas.append(row)
                    if len(linhas) >= tamanho:
                        break

                after = comp_agg.get("after_key")
                if not after or len(linhas) >= tamanho:
                    break

            return pd.DataFrame(linhas), nome

        raise ErroExportacao(f"Tipo de painel não suportado: '{tipo}'. Use: time_series | top_terms | table.")

    def exportar(
        self,
        inicio_iso: str,
        fim_iso: str,
        query_string: str,
        paineis: List[Dict[str, Any]],
        callback_progresso: Callable[[int, str], None] = _callback_vazio,
        max_docs: Optional[int] = None,
        tamanho_pagina: int = 5000,
        gerar_analises_automaticas: bool = True,
    ) -> str:
        callback_progresso(0, "Preparando...")

        os.makedirs("exports", exist_ok=True)
        nome_arquivo = f"export_elastic_{self.indice.replace('*','ALL')}_{_agora_str()}.xlsx"
        caminho = os.path.join("exports", nome_arquivo)

        callback_progresso(1, "Consultando Elasticsearch (raw)...")
        docs = self._buscar_todos_documentos(
            query_string=query_string,
            inicio_iso=inicio_iso,
            fim_iso=fim_iso,
            tamanho_pagina=tamanho_pagina,
            max_docs=max_docs,
            callback_progresso=callback_progresso,
        )
        if not docs:
            raise ErroExportacao("Nenhum documento encontrado para esse filtro.")

        callback_progresso(62, f"Montando DataFrame ({len(docs):,} linhas)...")
        df_raw = pd.json_normalize(docs, sep=".")

        LIMITE_LINHAS = 1_048_576
        LIMITE_COLUNAS = 16_384
        if df_raw.shape[1] > LIMITE_COLUNAS:
            df_raw = df_raw.iloc[:, :LIMITE_COLUNAS]

        callback_progresso(70, "Escrevendo Excel (raw primeiro)...")

        with pd.ExcelWriter(caminho, engine="xlsxwriter") as writer:
            workbook = writer.book
            fmt_header = workbook.add_format({"bold": True, "text_wrap": True})
            fmt_numero = workbook.add_format({"num_format": "#,##0"})

            # ===== Resumo =====
            resumo_ws = workbook.add_worksheet("Resumo")
            resumo_ws.write(0, 0, "Índice", fmt_header); resumo_ws.write(0, 1, self.indice)
            resumo_ws.write(1, 0, "Query", fmt_header); resumo_ws.write(1, 1, query_string)
            resumo_ws.write(2, 0, "Campo de tempo", fmt_header); resumo_ws.write(2, 1, self.campo_tempo)
            resumo_ws.write(3, 0, "Início (ISO)", fmt_header); resumo_ws.write(3, 1, inicio_iso or "(vazio)")
            resumo_ws.write(4, 0, "Fim (ISO)", fmt_header); resumo_ws.write(4, 1, fim_iso or "(vazio)")
            resumo_ws.write(5, 0, "Total de docs (raw)", fmt_header); resumo_ws.write_number(5, 1, int(len(df_raw)), fmt_numero)
            resumo_ws.set_column(0, 0, 18); resumo_ws.set_column(1, 1, 120)

            # ===== raw_events (sempre escreve) =====
            callback_progresso(74, "Escrevendo raw_events...")
            total_linhas = len(df_raw)
            partes = math.ceil(total_linhas / LIMITE_LINHAS)

            for parte in range(partes):
                ini = parte * LIMITE_LINHAS
                fim = min((parte + 1) * LIMITE_LINHAS, total_linhas)
                nome_sheet = "raw_events" if partes == 1 else f"raw_events_{parte+1}"
                df_parte = df_raw.iloc[ini:fim].copy()
                df_parte = _excel_safe_df(df_parte)
                df_parte.to_excel(writer, sheet_name=nome_sheet, index=False)

                ws = writer.sheets[nome_sheet]
                ws.freeze_panes(1, 0)
                ws.autofilter(0, 0, len(df_parte), max(0, df_parte.shape[1] - 1))
                for col, colname in enumerate(df_parte.columns):
                    ws.write(0, col, colname, fmt_header)
                ws.set_column(0, min(50, max(0, df_parte.shape[1]-1)), 22)

            # ===== Análises automáticas (não podem matar o arquivo) =====
            top_sheets: List[str] = []
            df_ts = pd.DataFrame()

            if gerar_analises_automaticas:
                try:
                    callback_progresso(78, "Gerando análises automáticas (Top N / Série / Campos)...")

                    # Campos
                    sample = df_raw.head(5000)
                    linhas_campos = []
                    for c in df_raw.columns:
                        serie = sample[c]
                        total = int(len(sample))
                        nao_nulo = int(serie.notna().sum())
                        perc = (nao_nulo / total * 100.0) if total else 0.0
                        try:
                            distintos = int(serie.dropna().astype("string").nunique())
                        except Exception:
                            distintos = 0
                        try:
                            exemplo = serie.dropna().iloc[0]
                        except Exception:
                            exemplo = ""
                        linhas_campos.append({
                            "campo": c,
                            "% preenchido (amostra)": round(perc, 2),
                            "distintos (amostra)": distintos,
                            "exemplo": str(exemplo)[:200] if exemplo is not None else "",
                        })
                    df_campos = pd.DataFrame(linhas_campos).sort_values(by="% preenchido (amostra)", ascending=False)
                    df_campos = _excel_safe_df(df_campos)
                    df_campos.to_excel(writer, sheet_name="Campos", index=False)
                    ws_campos = writer.sheets["Campos"]
                    ws_campos.freeze_panes(1, 0)
                    ws_campos.autofilter(0, 0, len(df_campos), 3)
                    for col, colname in enumerate(df_campos.columns):
                        ws_campos.write(0, col, colname, fmt_header)
                    ws_campos.set_column(0, 0, 55); ws_campos.set_column(1, 2, 22); ws_campos.set_column(3, 3, 90)

                    # Série temporal total
                    freq = _sugerir_freq(inicio_iso, fim_iso)
                    df_ts = _serie_tempo_df(df_raw, self.campo_tempo, freq=freq)
                    if not df_ts.empty:
                        df_ts = _excel_safe_df(df_ts)
                        df_ts.to_excel(writer, sheet_name="Serie_tempo_total", index=False)
                        ws_ts = writer.sheets["Serie_tempo_total"]
                        ws_ts.freeze_panes(1, 0)
                        for col, colname in enumerate(df_ts.columns):
                            ws_ts.write(0, col, colname, fmt_header)
                            ws_ts.set_column(col, col, 26)

                        chart = workbook.add_chart({"type": "line"})
                        chart.add_series({
                            "categories": ["Serie_tempo_total", 1, 0, len(df_ts), 0],
                            "values": ["Serie_tempo_total", 1, 1, len(df_ts), 1],
                            "name": f"Docs/{freq}",
                        })
                        chart.set_title({"name": "Total de eventos ao longo do tempo"})
                        chart.set_legend({"none": True})
                        ws_ts.insert_chart(1, 3, chart, {"x_scale": 2.0, "y_scale": 1.4})

                    # Top N (ECS comuns)
                    candidatos = [
                        "event.dataset","event.kind","event.module","event.action",
                        "host.name","agent.name","log.level","network.protocol","network.transport","network.direction",
                        "source.ip","destination.ip","source.port","destination.port",
                        "source.as.number","source.as.organization.name","destination.as.number","destination.as.organization.name",
                        "source.geo.country_name","source.geo.region_name","source.geo.city_name",
                        "destination.geo.country_name","destination.geo.region_name","destination.geo.city_name",
                        "suricata.eve.event_type","suricata.eve.in_iface","suricata.eve.proto",
                        "suricata.eve.alert.signature","suricata.eve.alert.category","suricata.eve.alert.severity",
                        "rule.id","alert.signature","http.request.method","url.domain","url.path","user.name",
                    ]

                    for c in candidatos:
                        if c in df_raw.columns:
                            df_top = _top_terms_df(df_raw, c, n=25)
                            if df_top.empty:
                                continue
                            nome_sheet = ("Top_" + c.replace(".", "_"))[:31]
                            df_top = _excel_safe_df(df_top)
                            df_top.to_excel(writer, sheet_name=nome_sheet, index=False)
                            ws = writer.sheets[nome_sheet]
                            ws.freeze_panes(1, 0)
                            for col, colname in enumerate(df_top.columns):
                                ws.write(0, col, colname, fmt_header)
                                ws.set_column(col, col, 45 if col == 0 else 16)

                            chart = workbook.add_chart({"type": "column"})
                            chart.add_series({
                                "categories": [nome_sheet, 1, 0, len(df_top), 0],
                                "values": [nome_sheet, 1, 1, len(df_top), 1],
                                "name": "count",
                            })
                            chart.set_title({"name": f"Top valores: {c}"})
                            chart.set_legend({"none": True})
                            ws.insert_chart(1, 3, chart, {"x_scale": 1.9, "y_scale": 1.3})

                            top_sheets.append(nome_sheet)

                    # Dashboard (atalhos)
                    dash = workbook.add_worksheet("Dashboard")
                    fmt_title = workbook.add_format({"bold": True, "font_size": 16})
                    fmt_kpi_label = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "fg_color": "#1f2937", "font_color": "#ffffff", "border": 1})
                    fmt_kpi_value = workbook.add_format({"bold": True, "font_size": 14, "align": "center", "valign": "vcenter", "border": 1})
                    dash.set_row(0, 28)
                    dash.set_column(0, 0, 26)
                    dash.set_column(1, 1, 40)
                    dash.set_column(2, 8, 22)
                    dash.write(0, 0, "Dashboard – Elastic → Excel", fmt_title)
                    dash.write(1, 0, "Índice", fmt_header); dash.write(1, 1, self.indice)
                    dash.write(2, 0, "Query", fmt_header); dash.write(2, 1, query_string)
                    dash.write(3, 0, "Período", fmt_header); dash.write(3, 1, f"{inicio_iso or '(vazio)'} → {fim_iso or '(vazio)'}")
                    
                    # KPIs
                    total_docs = int(len(df_raw))
                    kpi_src = int(df_raw["source.ip"].nunique()) if "source.ip" in df_raw.columns else 0
                    kpi_dst = int(df_raw["destination.ip"].nunique()) if "destination.ip" in df_raw.columns else 0
                    kpi_sig = int(df_raw["suricata.eve.alert.signature"].nunique()) if "suricata.eve.alert.signature" in df_raw.columns else (int(df_raw["alert.signature"].nunique()) if "alert.signature" in df_raw.columns else 0)
                    dash.write(5, 0, "Total eventos", fmt_kpi_label); dash.write_number(6, 0, total_docs, fmt_kpi_value)
                    dash.write(5, 1, "IPs origem únicos", fmt_kpi_label); dash.write_number(6, 1, kpi_src, fmt_kpi_value)
                    dash.write(5, 2, "IPs destino únicos", fmt_kpi_label); dash.write_number(6, 2, kpi_dst, fmt_kpi_value)
                    dash.write(5, 3, "Assinaturas únicas", fmt_kpi_label); dash.write_number(6, 3, kpi_sig, fmt_kpi_value)
                    
                    # Links rápidos
                    dash.write_url(1, 4, "internal:'Resumo'!A1", string="Resumo")
                    dash.write_url(2, 4, "internal:'raw_events'!A1", string="raw_events")
                    dash.write_url(3, 4, "internal:'Campos'!A1", string="Campos")
                    if not df_ts.empty:
                        dash.write_url(4, 4, "internal:'Serie_tempo_total'!A1", string="Série temporal")
                    
                    # Gráficos principais
                    row_chart = 8
                    if not df_ts.empty:
                        chart_ts = workbook.add_chart({"type": "line"})
                        chart_ts.add_series({
                            "categories": ["Serie_tempo_total", 1, 0, len(df_ts), 0],
                            "values": ["Serie_tempo_total", 1, 1, len(df_ts), 1],
                            "name": "Eventos",
                        })
                        chart_ts.set_title({"name": "Histórico de eventos"})
                        chart_ts.set_legend({"none": True})
                        dash.insert_chart(row_chart, 0, chart_ts, {"x_scale": 2.1, "y_scale": 1.3})
                        row_chart += 16
                    
                    def _achar_sheet(chaves: List[str]) -> str:
                        for s in top_sheets:
                            for k in chaves:
                                if k in s:
                                    return s
                        return ""
                    
                    sheet_attacks = _achar_sheet(["Top_suricata_eve_alert_signature", "Top_alert_signature"])
                    if sheet_attacks:
                        chart_a = workbook.add_chart({"type": "bar"})
                        chart_a.add_series({
                            "categories": [sheet_attacks, 1, 0, 15, 0],
                            "values": [sheet_attacks, 1, 1, 15, 1],
                            "name": "count",
                        })
                        chart_a.set_title({"name": "Top assinaturas"})
                        chart_a.set_legend({"none": True})
                        dash.insert_chart(8, 5, chart_a, {"x_scale": 1.6, "y_scale": 1.2})
                    
                    sheet_ips = _achar_sheet(["Top_source_ip"])
                    if sheet_ips:
                        chart_i = workbook.add_chart({"type": "bar"})
                        chart_i.add_series({
                            "categories": [sheet_ips, 1, 0, 15, 0],
                            "values": [sheet_ips, 1, 1, 15, 1],
                            "name": "count",
                        })
                        chart_i.set_title({"name": "Top IPs origem"})
                        chart_i.set_legend({"none": True})
                        dash.insert_chart(row_chart, 0, chart_i, {"x_scale": 1.8, "y_scale": 1.2})
                    
                    sheet_ports = _achar_sheet(["Top_destination_port"])
                    if sheet_ports:
                        chart_p = workbook.add_chart({"type": "column"})
                        chart_p.add_series({
                            "categories": [sheet_ports, 1, 0, 15, 0],
                            "values": [sheet_ports, 1, 1, 15, 1],
                            "name": "count",
                        })
                        chart_p.set_title({"name": "Top portas destino"})
                        chart_p.set_legend({"none": True})
                        dash.insert_chart(row_chart, 5, chart_p, {"x_scale": 1.6, "y_scale": 1.2})
                    
                    # Lista de Top N gerados (links)
                    r = 24
                    if top_sheets:
                        dash.write(r, 0, "Top N gerados:", fmt_header); r += 1
                        for sname in top_sheets[:60]:
                            dash.write_url(r, 0, f"internal:'{sname}'!A1", string=sname)
                            r += 1
                    dash.set_column(4, 4, 18)

                except Exception as e:
                    df_err = pd.DataFrame([{"erro_analises": str(e)}])
                    df_err.to_excel(writer, sheet_name="Erros", index=False)

            # ===== Painéis via aggs no ES (opcional) =====
            if paineis:
                callback_progresso(90, "Gerando painéis (aggs no ES)...")

            for i, painel in enumerate(paineis or []):
                df_painel, nome = self._executar_agregacao(query_string, inicio_iso, fim_iso, painel)
                nome_sheet = (nome[:31] if nome else f"painel_{i+1}") or f"painel_{i+1}"
                df_painel = _excel_safe_df(df_painel)
                df_painel.to_excel(writer, sheet_name=nome_sheet, index=False)

                ws = writer.sheets[nome_sheet]
                ws.freeze_panes(1, 0)
                ws.autofilter(0, 0, max(1, len(df_painel)), max(0, df_painel.shape[1] - 1))
                for col, colname in enumerate(df_painel.columns):
                    ws.write(0, col, colname, fmt_header)
                    ws.set_column(col, col, 24)

                tipo = (painel.get("tipo") or "").strip()
                if not df_painel.empty and df_painel.shape[1] >= 2 and tipo in ("time_series", "top_terms"):
                    chart = workbook.add_chart({"type": "line" if tipo == "time_series" else "column"})
                    chart.add_series({
                        "categories": [nome_sheet, 1, 0, len(df_painel), 0],
                        "values": [nome_sheet, 1, 1, len(df_painel), 1],
                        "name": nome_sheet,
                    })
                    chart.set_title({"name": nome_sheet})
                    chart.set_legend({"none": True})
                    ws.insert_chart(1, min(4, df_painel.shape[1] + 1), chart, {"x_scale": 1.6, "y_scale": 1.2})

                callback_progresso(90 + int((i + 1) / max(1, len(paineis)) * 8), f"Painéis ES: {i+1}/{len(paineis)}")

        callback_progresso(98, "Finalizando...")
        time.sleep(0.2)
        return caminho