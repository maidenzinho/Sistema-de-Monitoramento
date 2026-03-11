let jobId = null;
let paineis = [];

const $ = (id) => document.getElementById(id);

async function carregarPresets(){
  try{
    const resp = await fetch("/api/presets");
    const data = await resp.json();
    if(!data.ok) throw new Error(data.erro || "Falha ao carregar presets");
    const p = data.presets || {};

    // Índices
    const selIdx = $("indice_preset");
    if(selIdx){
      selIdx.innerHTML = "";
      (p.index_presets || []).forEach(item => {
        const opt = document.createElement("option");
        opt.value = item.pattern ?? "";
        opt.textContent = item.label ?? item.pattern ?? "";
        selIdx.appendChild(opt);
      });
      selIdx.addEventListener("change", () => {
        const v = selIdx.value;
        if(v) $("indice").value = v;
      });
    }

    // Query presets
    const selQ = $("query_preset");
    if(selQ){
      selQ.innerHTML = "";
      (p.query_presets || []).forEach(item => {
        const opt = document.createElement("option");
        opt.value = item.query ?? "*";
        opt.textContent = item.label ?? item.query ?? "*";
        selQ.appendChild(opt);
      });
      selQ.addEventListener("change", () => {
        const v = selQ.value;
        if(v) $("query_string").value = v;
      });
    }

    // Time presets
    const selT = $("time_preset");
    if(selT){
      selT.innerHTML = "";
      (p.time_presets || []).forEach(item => {
        const opt = document.createElement("option");
        opt.value = JSON.stringify(item);
        opt.textContent = item.label ?? "preset";
        selT.appendChild(opt);
      });

      const toInput = (d) => {
        const pad = (n) => String(n).padStart(2,"0");
        const yyyy = d.getFullYear();
        const mm = pad(d.getMonth()+1);
        const dd = pad(d.getDate());
        const hh = pad(d.getHours());
        const mi = pad(d.getMinutes());
        return `${yyyy}-${mm}-${dd}T${hh}:${mi}`;
      };

      selT.addEventListener("change", () => {
        let item = null;
        try{ item = JSON.parse(selT.value); }catch(_e){}
        if(!item || item.custom) return;

        const now = new Date();
        const start = new Date(now);
        if(item.hours) start.setHours(start.getHours() - item.hours);
        if(item.days) start.setDate(start.getDate() - item.days);

        $("inicio").value = toInput(start);
        $("fim").value = toInput(now);
      });
    }
  }catch(e){
    console.warn("Presets:", e);
  }
}


function setStatus(msg, ok = true){
  const el = $("status");
  el.textContent = msg;
  el.style.color = ok ? "var(--muted)" : "var(--bad)";
}

function setProgresso(p){
  $("progresso").style.width = `${Math.max(0, Math.min(100, p))}%`;
}

function lerParametros(){
  const indice = $("indice").value.trim();
  const campo_tempo = $("campo_tempo").value.trim() || "@timestamp";
  const inicio = $("inicio").value ? `${$("inicio").value}:00` : "";
  const fim = $("fim").value ? `${$("fim").value}:00` : "";
  const query_string = $("query_string").value.trim() || "*";
  const max_docs = $("max_docs").value ? parseInt($("max_docs").value, 10) : null;
  const tamanho_pagina = $("tamanho_pagina").value ? parseInt($("tamanho_pagina").value, 10) : 5000;
  const gerar_analises_automaticas = ($("gerar_auto").value === "true");

  return { indice, campo_tempo, inicio, fim, query_string, max_docs, tamanho_pagina, paineis, gerar_analises_automaticas };
}

async function postJson(url, payload){
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const ct = (r.headers.get("content-type") || "").toLowerCase();
  const text = await r.text();

  let j = null;
  try { j = JSON.parse(text); } catch(e) {}

  if (!j) {
    throw new Error(`Servidor retornou ${r.status} (${ct || "sem content-type"}). Início: ${text.slice(0, 160)}`);
  }
  if (!r.ok || !j.ok) {
    throw new Error(j.erro || `Erro HTTP ${r.status}`);
  }
  return j;
}

async function testar(){
  $("download").innerHTML = "";
  setStatus("Testando conexão...");
  setProgresso(5);

  try{
    const p = lerParametros();
    const j = await postJson("/api/testar", {indice: p.indice, campo_tempo: p.campo_tempo});
    setStatus(`Conectou! Cluster: ${j.info.cluster_name} | Versão: ${j.info.version.number}`);
    setProgresso(10);
  }catch(e){
    setStatus(`Falhou: ${e.message}`, false);
    setProgresso(0);
  }
}

async function converter(){
  const texto = $("qi_json").value.trim();
  if(!texto){
    setStatus("Cole o JSON do Query Inspector primeiro.", false);
    return;
  }
  try{
    setStatus("Convertendo Query Inspector...");
    const j = await postJson("/api/importar_grafana", {query_inspector_json: texto});
    paineis = j.paineis || [];
    $("paineis_preview").textContent = JSON.stringify(paineis, null, 2);
    setStatus(`Convertido: ${paineis.length} painel(is).`);
  }catch(e){
    setStatus(`Erro ao converter: ${e.message}`, false);
  }
}

async function exportar(){
  $("download").innerHTML = "";
  setStatus("Iniciando exportação...");
  setProgresso(1);

  try{
    const p = lerParametros();
    const j = await postJson("/api/exportar", p);
    jobId = j.job_id;
    setStatus(`Job criado: ${jobId}. Exportando...`);
    acompanhar();
  }catch(e){
    setStatus(`Falhou: ${e.message}`, false);
    setProgresso(0);
  }
}

async function acompanhar(){
  if(!jobId) return;

  try{
    const r = await fetch(`/api/status/${jobId}`);
    const j = await r.json();
    if(!j.ok) throw new Error(j.erro || "Erro ao ler status");

    const job = j.job;
    setProgresso(job.progresso || 0);
    setStatus(job.mensagem || job.status);

    if(job.status === "concluido"){
      $("download").innerHTML = `<a class="link" href="/api/download/${jobId}">Baixar Excel</a>`;
      return;
    }
    if(job.status === "erro"){
      setStatus(job.mensagem || "Erro", false);
      return;
    }

    setTimeout(acompanhar, 900);
  }catch(e){
    setStatus(`Erro ao acompanhar: ${e.message}`, false);
  }
}

$("btn_testar").addEventListener("click", testar);
$("btn_converter").addEventListener("click", converter);
$("btn_limpar").addEventListener("click", () => {
  $("qi_json").value = "";
  paineis = [];
  $("paineis_preview").textContent = "[]";
});
$("btn_exportar").addEventListener("click", exportar);


window.addEventListener('DOMContentLoaded', () => { carregarPresets(); });
