from __future__ import annotations

import os
import json
import uuid
import threading
from datetime import datetime, timezone
from typing import Any, Dict

from flask import Flask, jsonify, render_template, request, send_file
from werkzeug.exceptions import HTTPException
from dotenv import load_dotenv

from exportador import ExportadorElasticParaExcel, ErroExportacao
from grafana_parser import converter_query_inspector_para_paineis

load_dotenv()

app = Flask(__name__)

trabalhos: Dict[str, Dict[str, Any]] = {}

def _caminho_presets() -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, "config", "presets.json")

def _ler_presets() -> Dict[str, Any]:
    caminho = _caminho_presets()
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"index_presets": [], "query_presets": [], "time_presets": []}
    except Exception as e:
        app.logger.exception(e)
        return {"index_presets": [], "query_presets": [], "time_presets": []}


def _bool_env(nome: str, padrao: bool = True) -> bool:
    val = os.getenv(nome, str(padrao)).strip().lower()
    return val in ("1", "true", "t", "yes", "y", "sim")


def _cfg_elastic() -> Dict[str, Any]:
    return {
        "url": os.getenv("ES_URL", "https://45.189.179.214:9200"),
        "usuario": os.getenv("ES_USER", "elastic"),
        "senha": os.getenv("ES_PASSWORD", "MPxslqBRakPNE4u99jgb"),
        "verify_certs": _bool_env("ES_VERIFY_CERTS", False),
        "ca_certs": os.getenv("ES_CA_CERTS", ""),
        "suppress_tls_warnings": _bool_env("ES_SUPPRESS_TLS_WARNINGS", False),
    }


@app.before_request
def _log_request():
    app.logger.info("REQ %s %s", request.method, request.path)


@app.errorhandler(HTTPException)
def handle_http_exception(e: HTTPException):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "erro": f"{e.code} {e.name}: {e.description}"}), e.code
    return e


@app.errorhandler(Exception)
def handle_exception(e: Exception):
    app.logger.exception(e)
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "erro": str(e)}), 500
    raise e


@app.get("/")
def pagina_inicial():
    cfg = {
        "default_index": os.getenv("DEFAULT_INDEX", ""),
        "default_time_field": os.getenv("DEFAULT_TIME_FIELD", "@timestamp"),
    }
    return render_template("index.html", cfg=cfg)

@app.get("/api/presets")
def presets():
    return jsonify({"ok": True, "presets": _ler_presets()})



@app.post("/api/testar")
def testar_conexao():
    dados = request.get_json(force=True) or {}
    indice = (dados.get("indice") or os.getenv("DEFAULT_INDEX") or "").strip()
    cfg = _cfg_elastic()

    exp = ExportadorElasticParaExcel(
        cfg_elastic=cfg,
        indice=indice,
        campo_tempo=dados.get("campo_tempo") or os.getenv("DEFAULT_TIME_FIELD", "@timestamp"),
    )
    info = exp.testar_conexao()
    return jsonify({"ok": True, "info": info})


@app.post("/api/importar_grafana")
def importar_grafana():
    dados = request.get_json(force=True) or {}
    texto = (dados.get("query_inspector_json") or "").strip()
    if not texto:
        return jsonify({"ok": False, "erro": "Cole o JSON do Query Inspector do painel do Grafana."}), 400

    obj = json.loads(texto)
    paineis = converter_query_inspector_para_paineis(obj)
    return jsonify({"ok": True, "paineis": paineis})


def _executar_trabalho(job_id: str, parametros: Dict[str, Any]) -> None:
    trabalhos[job_id]["status"] = "rodando"
    trabalhos[job_id]["mensagem"] = "Iniciando..."

    def progresso(p: int, msg: str):
        trabalhos[job_id]["progresso"] = int(p)
        trabalhos[job_id]["mensagem"] = msg

    cfg = _cfg_elastic()
    indice = (parametros.get("indice") or os.getenv("DEFAULT_INDEX") or "").strip()
    campo_tempo = (parametros.get("campo_tempo") or os.getenv("DEFAULT_TIME_FIELD", "@timestamp")).strip()

    inicio = parametros.get("inicio") or ""
    fim = parametros.get("fim") or ""
    query_string = parametros.get("query_string") or "*"

    paineis = parametros.get("paineis") or []
    gerar_auto = bool(parametros.get("gerar_analises_automaticas", True))

    try:
        exp = ExportadorElasticParaExcel(cfg_elastic=cfg, indice=indice, campo_tempo=campo_tempo)
        caminho = exp.exportar(
            inicio_iso=inicio,
            fim_iso=fim,
            query_string=query_string,
            paineis=paineis,
            callback_progresso=progresso,
            max_docs=parametros.get("max_docs"),
            tamanho_pagina=parametros.get("tamanho_pagina", 5000),
            gerar_analises_automaticas=gerar_auto,
        )
        trabalhos[job_id]["status"] = "concluido"
        trabalhos[job_id]["progresso"] = 100
        trabalhos[job_id]["mensagem"] = "Exportação concluída."
        trabalhos[job_id]["arquivo"] = caminho
    except Exception as e:
        trabalhos[job_id]["status"] = "erro"
        trabalhos[job_id]["mensagem"] = str(e)


@app.post("/api/exportar")
def iniciar_exportacao():
    parametros = request.get_json(force=True) or {}
    job_id = uuid.uuid4().hex

    trabalhos[job_id] = {
        "id": job_id,
        "status": "fila",
        "progresso": 0,
        "mensagem": "Na fila...",
        "arquivo": None,
        "criado_em": datetime.now(timezone.utc).isoformat(),
    }

    t = threading.Thread(target=_executar_trabalho, args=(job_id, parametros), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": job_id})


@app.get("/api/status/<job_id>")
def status(job_id: str):
    job = trabalhos.get(job_id)
    if not job:
        return jsonify({"ok": False, "erro": "Job não encontrado."}), 404
    return jsonify({"ok": True, "job": job})


@app.get("/api/download/<job_id>")
def download(job_id: str):
    job = trabalhos.get(job_id)
    if not job or job.get("status") != "concluido" or not job.get("arquivo"):
        return jsonify({"ok": False, "erro": "Arquivo ainda não está pronto."}), 400
    caminho = job["arquivo"]
    nome = os.path.basename(caminho)
    return send_file(caminho, as_attachment=True, download_name=nome)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)